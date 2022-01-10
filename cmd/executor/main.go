package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/zajca/go-executor/internal/pkg/cleaner"
	"github.com/zajca/go-executor/internal/pkg/client"
	"github.com/zajca/go-executor/internal/pkg/job"
)

const JOB_COUNT_LIMIT = 1000

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	jobDone       = make(chan *job.Job)
	broadcaster   = client.NewBroadcaster()
	cmdPath       = strings.Split(os.Getenv("CMD_PATH"), ",")
	msgPath       = os.Getenv("MSG_PATH")
	jobCountLimit = 0
)

func ws(c echo.Context) error {
	ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		return err
	}
	broadcaster.AddClient(ws)
	defer ws.Close()

	for {
		if jobCountLimit == JOB_COUNT_LIMIT {
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
		jobCountLimit++
		go currentJob.Run(messages, PID, cmdRunDone, c.Logger())

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
			jobCountLimit--
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

type JobResponse struct {
	JobId string `json:"jobId"`
	PID   string `json:"pid"`
}

func showJobs(c echo.Context) error {

	jobs, _ := job.ListJobs(msgPath, c.Logger())
	return c.JSON(http.StatusOK, jobs)
}

func main() {
	e := echo.New()
	e.Debug = true

	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	e.Logger.Info(fmt.Sprintf("Using cmd: %v.", cmdPath))
	e.Logger.Info(fmt.Sprintf("Using msg dir: %s.", cmdPath))

	e.GET("/ws", ws)
	e.GET("/jobs", showJobs)

	cleaner := cleaner.NewJobCleaner(broadcaster)
	go func() {
		for j := range jobDone {
			cleaner.HandleJob(j, e.Logger)
		}
	}()
	go cleaner.Run(e.Logger)

	e.Logger.Fatal(e.Start(":1323"))
}
