package cleaner

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/zajca/go-executor/internal/pkg/client"
	"github.com/zajca/go-executor/internal/pkg/job"
)

type JobCleaner struct {
	broadcaster *client.Broadcaster
	jobsToClean map[*job.Job]bool
}

func NewJobCleaner(b *client.Broadcaster) *JobCleaner {
	return &JobCleaner{
		broadcaster: b,
		jobsToClean: make(map[*job.Job]bool),
	}
}

func (c *JobCleaner) HandleJob(j *job.Job, l echo.Logger) {
	l.Info(fmt.Sprintf("Job '%s' will be cleared.", j.JobId))
	if err := os.Remove(j.Path.PID); err != nil {
		l.Error(err)
	}
	c.jobsToClean[j] = false
}

func (c *JobCleaner) Run(l echo.Logger) {
	for {
		if !c.broadcaster.HasAnyClient() {
			if len(c.jobsToClean) != 0 {
				l.Info(fmt.Sprintf("'%d' jobs to clean, but no client is connected.", len(c.jobsToClean)))
			}
			time.Sleep(1 * time.Second)
			continue
		}
		if len(c.jobsToClean) != 0 {
			for j, p := range c.jobsToClean {
				if !p {
					// if not processing run clean
					c.jobsToClean[j] = true
					l.Debug(fmt.Sprintf("[Running] Clean for job: '%s'.", j.JobId))
					err := c.cleanJob(j, l)
					if err != nil {
						l.Error(err)
						l.Debug(fmt.Sprintf("[Failed] Clean for job: '%s'.", j.JobId))
						c.jobsToClean[j] = false
					} else {
						l.Debug(fmt.Sprintf("[Success] Clean for job: '%s'.", j.JobId))
						delete(c.jobsToClean, j)
					}
				}
			}
		}
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
