package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

var addr = flag.String("addr", "localhost:1323", "http service address")

type message struct {
	JobId      uuid.UUID `json:"jobId"`
	Command    string    `json:"command"`
	Parameters string    `json:"parameters"`
}

func main() {
	flag.Parse()
	log.SetFlags(0)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	u := url.URL{Scheme: "ws", Host: *addr, Path: "/ws"}
	log.Printf("connecting to %s", u.String())

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer c.Close()

	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Println("read:", err)
				return
			}
			log.Printf("recv: %s", message)
		}
	}()

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	counter := 0

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			counter++
			log.Println("Counter:", counter)
			json, _ := json.Marshal(message{
				JobId:      uuid.New(),
				Command:    "driver:workspace:create",
				Parameters: "{\"host\":\"123\"," + "\"counter\":\"" + strconv.Itoa(counter) + "\"}",
			})
			err := c.WriteMessage(websocket.TextMessage, json)
			if err != nil {
				log.Println("write:", err)
				return
			}
			log.Println("send:", string(json))
		case <-interrupt:
			log.Println("interrupt")

			// Cleanly close the connection by sending a close message and then
			// waiting (with timeout) for the server to close the connection.
			err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			if err != nil {
				log.Println("write close:", err)
				return
			}
			select {
			case <-done:
			case <-time.After(time.Second):
			}
			return
		}
	}
}
