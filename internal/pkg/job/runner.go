package job

import (
	"bufio"
	"encoding/json"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/labstack/echo/v4"
	"golang.org/x/sync/syncmap"
)

type Runner struct {
	jobs               syncmap.Map
	runningJobsCounter uint32
	jobsCounter        uint32
}

func (r *Runner) Run(job *Job, m chan<- *Message, p chan<- int, d chan<- bool, l echo.Logger) error {
	if job.Status != Waiting {
		return NewJobError(job, "Job is not in waiting state.", nil)
	}
	r.jobs.Store(job, nil)
	atomic.AddUint32(&r.jobsCounter, 1)
	atomic.AddUint32(&r.runningJobsCounter, 1)
	err := r.runCmd(job, m, p, d, l)
	if err != nil {
		l.Debug(err)
		return NewJobError(job, err.Error(), err)
	}

	return nil
}

func (r *Runner) TotalJobsCount() uint32 {
	return r.jobsCounter
}

func (r *Runner) RunningJobsCount() uint32 {
	return r.runningJobsCounter
}

func (r *Runner) runCmd(job *Job, m chan<- *Message, p chan<- int, d chan<- bool, l echo.Logger) error {
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

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}
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

	r.jobs.Delete(job)
	atomic.AddUint32(&r.runningJobsCounter, ^uint32(0))

	if err != nil {
		return NewJobError(job, err.Error(), err)
	}

	return nil
}
