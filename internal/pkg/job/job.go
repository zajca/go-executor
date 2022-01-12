package job

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/go-playground/validator/v10"
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
	JobId       string `json:"jobId",validate:"required,notblank"`
	CommandPath []string
	Command     string `json:"command",validate:"required,notblank"`
	Parameters  string `json:"parameters",validate:"required,notblank"`
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
