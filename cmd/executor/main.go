package main

import (
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/zajca/go-executor/internal/pkg/cleaner"
	"github.com/zajca/go-executor/internal/pkg/client"
	"github.com/zajca/go-executor/internal/pkg/job"
)

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	jobDone     = make(chan *job.Job)
	broadcaster = client.NewBroadcaster()
)

func ws(c echo.Context) error {
	ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		return err
	}
	broadcaster.AddClient(ws)
	defer ws.Close()

	for {
		// Read
		_, msg, err := ws.ReadMessage()
		if err != nil {
			continue
		}
		c.Logger().Debug(msg)
		currentJob, err := job.MakeJob(string(msg))
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
		// run job
		go currentJob.Run(messages, PID, c.Logger())

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
				if currentJob.Status == job.Success || currentJob.Status == job.Fail {
					// close file descriptor if job ended
					currentDumper.Close()
					jobDone <- &currentJob
				}
			}
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

func main() {
	e := echo.New()
	e.Debug = true

	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.GET("/ws", ws)

	cleaner := cleaner.NewJobCleaner(broadcaster)
	go func() {
		for j := range jobDone {
			cleaner.HandleJob(j, e.Logger)
		}
	}()
	go cleaner.Run(e.Logger)

	e.Logger.Fatal(e.Start(":1323"))
}
