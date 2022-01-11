package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/zajca/go-executor/internal/pkg/cleaner"
	"github.com/zajca/go-executor/internal/pkg/client"
	"github.com/zajca/go-executor/internal/pkg/job"
)

// with more thant 1K limit process are throwing "invalid memory address or nil pointer dereference"
// this might be system limit for processes
// when limit is reached system will not proccess new messages
const JOB_COUNT_LIMIT = 1000

type applicationStatus int

const (
	Starting applicationStatus = 0
	Running  applicationStatus = 1
	Draining applicationStatus = 2
)

func (s applicationStatus) String() string {
	switch s {
	case Starting:
		return "STARTING"
	case Running:
		return "RUNNING"
	case Draining:
		return "DRAINING"
	}
	panic("Unknow status")
}

type healthResponse struct {
	Status           applicationStatus
	Jobs             []job.JobResponse
	CountClearing    uint32
	CountJobsRunning uint32
	CountJobsTotal   uint32
}

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	jobDone     = make(chan *job.Job)
	jobCleared  = make(chan *job.Job)
	broadcaster = client.NewBroadcaster()
	cleanerInst = cleaner.NewJobCleaner(broadcaster)
	jobRunner   = job.Runner{}
	cmdPath     = strings.Split(os.Getenv("CMD_PATH"), ",")
	msgPath     = os.Getenv("MSG_PATH")
	status      = Starting
)

func ws(c echo.Context) error {
	ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		return err
	}
	broadcaster.AddClient(ws)
	defer ws.Close()

	for {
		// top limit is not exact as there could be more messages by time this is evaluated
		if jobRunner.RunningJobsCount() >= JOB_COUNT_LIMIT {
			continue
		}
		if status == Draining {
			continue
		}
		// Read
		_, msg, err := ws.ReadMessage()
		if err != nil {
			continue
		}
		c.Logger().Debug(msg)
		currentJob, err := job.MakeJob(cmdPath, msgPath, string(msg))
		if err != nil {
			c.Logger().Error(err)
			continue
		}
		c.Logger().Debug(currentJob.JobId)
		//init job dumper
		currentDumper := job.NewJobDumper(&currentJob)
		currentDumper.Init()
		// init job channels
		messages := make(chan *job.Message)
		PID := make(chan int, 1)
		cmdRunDone := make(chan bool, 1)
		// run job
		go jobRunner.Run(&currentJob, messages, PID, cmdRunDone, c.Logger())

		go func() {
			for m := range messages {
				isSend := broadcaster.SendMessage(m, c.Logger())
				if isSend {
					// set message as send if broadcast to any client was successfull
					m.Status = job.Send
				}
				// dump message to csv
				err = currentDumper.DumpMessage(m)
				if err != nil {
					c.Logger().Error(err)
				}
			}
		}()

		go func() {
			<-cmdRunDone
			// close file descriptor if job ended
			currentDumper.Close()
			jobDone <- &currentJob
		}()

		go func() {
			// set PID to file
			for p := range PID {
				err = currentDumper.DumpPID(p)
				if err != nil {
					c.Logger().Error(err)
				}
			}
		}()
	}
}

func showHealth(c echo.Context) error {

	jobs, _ := job.ListJobs(msgPath, c.Logger())

	return c.JSON(http.StatusOK, healthResponse{
		Status:           status,
		Jobs:             jobs,
		CountClearing:    cleanerInst.Count(),
		CountJobsRunning: jobRunner.RunningJobsCount(),
		CountJobsTotal:   jobRunner.TotalJobsCount(),
	})
}

func exitIfDone(l echo.Logger) {
	if cleanerInst.Count() == 0 && jobRunner.RunningJobsCount() == 0 {
		// exit when all jobs are done and cleared
		l.Info("No jobs are running and all cleared, exiting,...")
		l.Info(fmt.Sprintf("Total '%d' jobs were run.", jobRunner.TotalJobsCount()))
		os.Exit(0)
	}
}

func main() {
	status = Starting
	e := echo.New()
	e.Debug = true

	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	e.Logger.Info(fmt.Sprintf("Using cmd: %v.", cmdPath))
	e.Logger.Info(fmt.Sprintf("Using msg dir: %s.", cmdPath))

	e.GET("/ws", ws)
	e.GET("/health", showHealth)

	go func() {
		for j := range jobDone {
			cleanerInst.HandleJob(j, e.Logger)
		}
	}()
	go cleanerInst.Run(jobCleared, e.Logger)

	go func() {
		signalDrain := make(chan os.Signal, 1)

		signal.Notify(signalDrain, syscall.SIGINT)

		go func() {
			for range jobCleared {
				if status == Draining {
					exitIfDone(e.Logger)
				}
			}
		}()

		<-signalDrain
		e.Logger.Info("App status changed into draining status")
		exitIfDone(e.Logger)
		status = Draining
	}()

	status = Running
	e.Logger.Fatal(e.Start(":1323"))
}
