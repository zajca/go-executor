package job

import (
	"encoding/csv"
	"io/ioutil"
	"os"
	"strconv"
)

type JobDumper struct {
	Job                    *Job
	messagesWriter         *csv.Writer
	messagesFileDescriptor *os.File
}

func NewJobDumper(job *Job) *JobDumper {
	return &JobDumper{
		Job: job,
	}
}

func (d *JobDumper) Init() error {
	err := os.MkdirAll(d.Job.Path.Dir, os.ModeDir|os.ModePerm)
	if err != nil {
		return err
	}

	_, err = os.Create(d.Job.Path.Messages)
	if err != nil {
		return err
	}

	f, err := os.OpenFile(d.Job.Path.Messages, os.O_APPEND|os.O_WRONLY, 0777)
	if err != nil {
		return err
	}

	d.messagesFileDescriptor = f
	d.messagesWriter = csv.NewWriter(f)

	return nil
}

func (d *JobDumper) Close() {
	d.messagesFileDescriptor.Close()
}

func (d *JobDumper) DumpPID(pid int) error {
	return ioutil.WriteFile(d.Job.Path.PID, []byte(strconv.Itoa(pid)), 0777)
}

func (d *JobDumper) DumpMessage(m *Message) error {
	err := d.messagesWriter.Write(m.ToCsv())

	d.messagesWriter.Flush()

	return err
}
