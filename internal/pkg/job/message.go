package job

import (
	"strconv"
	"time"

	"github.com/google/uuid"
)

type MessageStatus int

const (
	NotSend MessageStatus = 0
	Send    MessageStatus = 1
)

func NewMessageStatusFromString(s string) MessageStatus {
	switch s {
	case "0":
		return NotSend
	case "1":
		return Send
	}

	panic("Unknow message status")
}

func (s MessageStatus) Int() int {
	switch s {
	case NotSend:
		return 0
	case Send:
		return 1
	}
	panic("Unknow status")
}

type ProcessStatus int

const (
	ProcessRunning ProcessStatus = 1
	ProcessSuccess ProcessStatus = 0
	ProcessFail    ProcessStatus = 2
)

func NewProcessStatusFromString(s string) ProcessStatus {
	switch s {
	case "0":
		return ProcessSuccess
	case "1":
		return ProcessRunning
	case "2":
		return ProcessFail
	}

	panic("Unknow process status")
}

func (s ProcessStatus) Int() int {
	switch s {
	case ProcessRunning:
		return 1
	case ProcessSuccess:
		return 0
	case ProcessFail:
		return 2
	}
	panic("Unknow process status")
}

type Message struct {
	Id            uuid.UUID
	Text          string
	Status        MessageStatus
	Timestamp     time.Time
	ProcessStatus ProcessStatus
}

func NewMessage(t string, s ProcessStatus) *Message {
	return &Message{
		Id:            uuid.New(),
		Text:          t,
		Status:        NotSend,
		ProcessStatus: s,
		Timestamp:     time.Now().UTC(),
	}
}

func NewMessageFromCsv(csv []string) *Message {

	id, _ := uuid.Parse(csv[0])
	t, _ := time.Parse(time.RFC3339, csv[1])

	return &Message{
		Id:            id,
		Timestamp:     t,
		Status:        NewMessageStatusFromString(csv[2]),
		ProcessStatus: NewProcessStatusFromString(csv[3]),
		Text:          csv[4],
	}
}

func (m *Message) ToCsv() []string {
	return []string{
		m.Id.String(),
		m.Timestamp.Format(time.RFC3339),
		strconv.Itoa(m.Status.Int()),
		strconv.Itoa(m.ProcessStatus.Int()),
		m.Text,
	}
}
