package job

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/labstack/echo/v4"
)

const (
	// MessagesPath     = "/var/cache/executor/messages"
	// MessagesPath     = "/home/zajca/Code/go/src/github.com/zajca/go-executor/tmp"
	MessagesPath     = "/home/zajca/Code/me/go-executor/tmp"
	massagesFileName = "messages.csv"
	PIDFileName      = "PID"
)

func NewJobError(job *Job, text string, previous error) error {
	return &jobError{job, text, previous}
}

type jobError struct {
	j *Job
	s string
	p error
}

func (e *jobError) Error() string {
	return fmt.Sprintf("Error occured during processing job jobId: %s. %s", e.j.JobId, e.s)
}

type Job struct {
	JobId      string `json:"jobId"`
	Command    string `json:"command"`
	Parameters string `json:"parameters"`
	Status     JobStatus
	Path       path
	Metrics    jobMetrics
}

type jobMetrics struct {
	InitTime  time.Time
	StartTime time.Time
	EndTime   time.Time
}

type path struct {
	Dir      string
	Messages string
	PID      string
}

type JobStatus int

const (
	Waiting JobStatus = iota
	Running
	Success
	Fail
)

func (s JobStatus) Int() int {
	switch s {
	case Waiting:
		return 2
	case Running:
		return 3
	case Success:
		return 0
	case Fail:
		return 1
	}
	panic("Unknow status")
}

func MakeJob(msg string) (Job, error) {
	job := Job{}
	err := json.Unmarshal([]byte(msg), &job)
	job.Status = Waiting
	dir := filepath.Join(MessagesPath, job.JobId)
	job.Path = path{
		Dir:      dir,
		Messages: filepath.Join(dir, massagesFileName),
		PID:      filepath.Join(dir, PIDFileName),
	}
	job.Metrics = jobMetrics{
		InitTime: time.Now().UTC(),
	}
	return job, err
}

func (job *Job) Run(m chan<- *Message, p chan<- int, l echo.Logger) error {
	if job.Status != Waiting {
		return NewJobError(job, "Job is not in waiting state.", nil)
	}
	err := job.runCmd(m, p, l)
	if err != nil {
		l.Debug(err)
		return NewJobError(job, err.Error(), err)
	}

	return nil
}

func (job *Job) runCmd(m chan<- *Message, p chan<- int, l echo.Logger) error {
	// cmd := exec.Command("php", "/home/zajca/Code/go/src/github.com/zajca/go-executor/cmd.php", job.Command, "--parameters", job.Parameters, "--jobId", job.JobId)
	cmd := exec.Command("php", "/home/zajca/Code/me/go-executor/cmd.php", job.Command, "--parameters", job.Parameters, "--jobId", job.JobId)
	job.Metrics.StartTime = time.Now().UTC()

	var wg sync.WaitGroup
	wg.Add(2)

	cmdReader, _ := cmd.StdoutPipe()
	scanner := bufio.NewScanner(cmdReader)
	go func() {
		for scanner.Scan() {
			text := scanner.Text()
			l.Debug(text)
			m <- NewMessage(text, ProcessRunning)
		}
		wg.Done()
	}()

	cmdReaderErr, _ := cmd.StderrPipe()
	scannerErr := bufio.NewScanner(cmdReaderErr)
	go func() {
		for scannerErr.Scan() {
			text := scannerErr.Text()
			l.Debug(text)
			m <- NewMessage(text, ProcessRunning)
		}
		wg.Done()
	}()
	cmd.Start()

	l.Debug(cmd.Process.Pid)
	job.Status = Running
	p <- cmd.Process.Pid
	close(p)

	err := cmd.Wait()
	job.Metrics.EndTime = time.Now().UTC()

	metrics, _ := json.Marshal(job.Metrics)
	m <- NewMessage(string(metrics), ProcessSuccess)

	wg.Wait()
	if err != nil {
		job.Status = Fail
		m <- NewMessage(err.Error(), ProcessFail)
	} else {
		job.Status = Success
		m <- NewMessage("Cmd done", ProcessSuccess)
	}
	close(m)

	if err != nil {
		return NewJobError(job, err.Error(), err)
	}

	return nil
}
