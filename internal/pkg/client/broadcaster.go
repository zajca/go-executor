package client

import (
	"encoding/json"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/zajca/go-executor/internal/pkg/job"
)

type Broadcaster struct {
	Clients map[*websocket.Conn]bool
}

func NewBroadcaster() *Broadcaster {
	return &Broadcaster{
		make(map[*websocket.Conn]bool),
	}
}

func (b *Broadcaster) AddClient(ws *websocket.Conn) {
	b.Clients[ws] = true
}

func (b *Broadcaster) HasAnyClient() bool {
	return len(b.Clients) != 0
}

func (b *Broadcaster) SendMessage(m *job.Message, l echo.Logger) bool {
	isSend := false
	// broadcast message to all clients
	for soc := range b.Clients {
		json, _ := json.Marshal(m)
		err := soc.WriteMessage(websocket.TextMessage, json)
		if err != nil {
			l.Error(err)
			delete(b.Clients, soc)
		} else {
			// message was send to at least one client
			isSend = true
		}
	}
	return isSend
}
