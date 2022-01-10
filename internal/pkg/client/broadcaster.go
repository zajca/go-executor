package client

import (
	"encoding/json"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/zajca/go-executor/internal/pkg/job"
)

type Broadcaster struct {
	Clients []*websocket.Conn
	mu      sync.Mutex
}

func NewBroadcaster() *Broadcaster {
	return &Broadcaster{
		Clients: []*websocket.Conn{},
		mu:      sync.Mutex{},
	}
}

func (b *Broadcaster) AddClient(ws *websocket.Conn) {
	b.Clients = append(b.Clients, ws)
}

func (b *Broadcaster) removeClient(i int) {
	b.Clients[i] = b.Clients[len(b.Clients)-1]
	b.Clients = b.Clients[:len(b.Clients)-1]
}

func (b *Broadcaster) HasAnyClient() bool {
	return len(b.Clients) != 0
}

func (b *Broadcaster) SendMessage(m *job.Message, l echo.Logger) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	isSend := false
	// broadcast message to all clients
	for i, soc := range b.Clients {
		json, _ := json.Marshal(m)
		err := soc.WriteMessage(websocket.TextMessage, json)
		if err != nil {
			l.Error(err)
			b.removeClient(i)
		} else {
			// message was send to at least one client
			isSend = true
		}
	}
	return isSend
}
