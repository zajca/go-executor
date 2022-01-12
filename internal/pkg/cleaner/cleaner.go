package cleaner

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/zajca/go-executor/internal/pkg/client"
	"github.com/zajca/go-executor/internal/pkg/job"
	"golang.org/x/sync/syncmap"
)

type JobCleaner struct {
	broadcaster *client.Broadcaster
	jobsToClean syncmap.Map
	jobsCounter uint32
}

func NewJobCleaner(b *client.Broadcaster) *JobCleaner {
	return &JobCleaner{
		broadcaster: b,
		jobsToClean: syncmap.Map{},
	}
}

func (c *JobCleaner) HandleJob(j *job.Job, l echo.Logger) {
	l.Info(fmt.Sprintf("Job '%s' will be cleared.", j.JobId))
	if err := os.Remove(j.Path.PID); err != nil {
		l.Error(err)
	}
	c.jobsToClean.Store(j, false)
	atomic.AddUint32(&c.jobsCounter, 1)
}

func (c *JobCleaner) Count() uint32 {
	return c.jobsCounter
}

func (c *JobCleaner) Run(jc chan<- *job.Job, l echo.Logger) {
	for {
		if !c.broadcaster.HasAnyClient() {
			if c.jobsCounter != 0 {
				l.Info(fmt.Sprintf("'%d' jobs to clean, but no client is connected.", c.jobsCounter))
			}
			time.Sleep(1 * time.Second)
			continue
		}

		if c.jobsCounter == 0 {
			l.Info("No jobs to clean.")
			time.Sleep(1 * time.Second)
			continue
		}

		c.jobsToClean.Range(func(j, p interface{}) bool {
			if p.(bool) == false {
				// if clean is not processing run it
				val, _ := j.(*job.Job)
				c.jobsToClean.Store(j, true)
				l.Debug(fmt.Sprintf("[Running] Clean for job: '%s'.", val.JobId))
				err := c.cleanJob(val, l)
				if err != nil {
					l.Error(err)
					l.Debug(fmt.Sprintf("[Failed] Clean for job: '%s'.", val.JobId))
					c.jobsToClean.Store(j, false)
				} else {
					l.Debug(fmt.Sprintf("[Success] Clean for job: '%s'.", val.JobId))
					c.jobsToClean.Delete(j)
					atomic.AddUint32(&c.jobsCounter, ^uint32(0))
					jc <- val
				}
				return false
			}

			return true
		})
	}
}

func (c *JobCleaner) cleanJob(j *job.Job, l echo.Logger) error {
	fr, err := os.Open(j.Path.Messages)
	if err != nil {
		return err
	}
	defer fr.Close()
	rr := csv.NewReader(fr)

	tmpCsv := j.Path.Messages + "tmp"
	if _, err = os.Create(tmpCsv); err != nil {
		return err
	}
	fw, err := os.OpenFile(tmpCsv, os.O_APPEND|os.O_WRONLY, 0777)
	if err != nil {
		return err
	}
	defer fw.Close()
	rw := csv.NewWriter(fw)

	isAllMessages := true
	for {
		record, err := rr.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}
		m := job.NewMessageFromCsv(record)

		if m.Status == job.NotSend {
			isSend := c.broadcaster.SendMessage(m, l)
			if isSend {
				m.Status = job.Send
			} else {
				isAllMessages = false
			}
		}

		rw.Write(m.ToCsv())
	}
	rw.Flush()

	if isAllMessages {
		if err := os.RemoveAll(j.Path.Dir); err != nil {
			return err
		}
		return nil
	}

	if err := os.Remove(tmpCsv); err != nil {
		l.Error(err)
	}
	if err := os.Rename(tmpCsv, j.Path.Messages); err != nil {
		l.Error(err)
	}

	return errors.New("cleaner: Not all messages were send")
}
