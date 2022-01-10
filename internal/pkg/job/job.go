package job

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/labstack/echo/v4"
)

var validate *validator.Validate

const (
	massagesFileName = "messages.csv"
	PIDFileName      = "PID"
)

func NewJobError(job *Job, text string, previous error) error {
	return &jobError{job, text, previous}
}

type jobValidationError struct {
	s string
	p error
}

func (e *jobValidationError) Error() string {
	return fmt.Sprintf("Job structure is invalid: %s.", e.s)
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
	JobId       string `json:"jobId",validate:"required"`
	CommandPath []string
	Command     string `json:"command",validate:"required"`
	Parameters  string `json:"parameters",validate:"required"`
	Status      JobStatus
	Path        path
	Metrics     jobMetrics
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

func MakeJob(cmdPath []string, msgPath string, msg string) (Job, error) {
	job := Job{}
	if err := json.Unmarshal([]byte(msg), &job); err != nil {
		return job, err
	}

	validate = validator.New()
	if err := validate.Struct(job); err != nil {
		return job, err
	}

	job.Status = Waiting
	dir := filepath.Join(msgPath, job.JobId)
	job.Path = path{
		Dir:      dir,
		Messages: filepath.Join(dir, massagesFileName),
		PID:      filepath.Join(dir, PIDFileName),
	}
	job.Metrics = jobMetrics{
		InitTime: time.Now().UTC(),
	}
	job.CommandPath = cmdPath
	return job, nil
}

func (job *Job) Run(m chan<- *Message, p chan<- int, d chan<- bool, l echo.Logger) error {
	if job.Status != Waiting {
		return NewJobError(job, "Job is not in waiting state.", nil)
	}
	err := job.runCmd(m, p, d, l)
	if err != nil {
		l.Debug(err)
		return NewJobError(job, err.Error(), err)
	}

	return nil
}

func (job *Job) runCmd(m chan<- *Message, p chan<- int, d chan<- bool, l echo.Logger) error {
	args := []string{job.Command, "--parameters", job.Parameters, "--jobId", job.JobId}
	cmdArgs := append(job.CommandPath, args...)
	cmd := exec.Command(cmdArgs[0], cmdArgs[1:]...)
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
	m <- NewMessage(string(metrics), ProcessRunning)

	wg.Wait()
	if err != nil {
		job.Status = Fail
		m <- NewMessage(err.Error(), ProcessFail)
	} else {
		job.Status = Success
		m <- NewMessage("Cmd done", ProcessSuccess)
	}
	close(m)
	d <- true
	close(d)

	if err != nil {
		return NewJobError(job, err.Error(), err)
	}

	return nil
}
