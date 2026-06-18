package client

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/Galdoba/remser/api/models"
	"github.com/gorilla/websocket"
)

type Client struct {
	ServerAddr  string
	ClientID    string
	Interactive bool
	OnMessage   func(msg models.ClientMessage)
	Stdout      io.Writer
	Stderr      io.Writer
}

// Execute запускает задачу через WebSocket.
func (c *Client) Execute(args []string) error {
	if c.ServerAddr == "" {
		c.ServerAddr = "http://localhost:8080"
	}
	// преобразуем http:// в ws://
	wsURL := "ws" + c.ServerAddr[4:] + "/ws"

	if c.Stdout == nil {
		c.Stdout = os.Stdout
	}
	if c.Stderr == nil {
		c.Stderr = os.Stderr
	}

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial ws: %w", err)
	}
	defer conn.Close()

	req := models.WSCommand{
		Type:        "execute",
		Args:        args,
		ClientID:    c.ClientID,
		Interactive: c.Interactive,
	}
	if err := conn.WriteJSON(req); err != nil {
		return fmt.Errorf("send execute: %w", err)
	}

	// Интерактивный ввод (отправка нажатий).
	if c.Interactive {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			var buf [1]byte
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				n, err := os.Stdin.Read(buf[:])
				if err != nil {
					return
				}
				if n > 0 {
					inputMsg := models.WSCommand{
						Type:  models.WSCmdInput,
						Input: string(buf[:n]),
					}
					if err := conn.WriteJSON(inputMsg); err != nil {
						return
					}
				}
			}
		}()
	}

	// Чтение ответных сообщений.
	for {
		var msg models.ClientMessage
		if err := conn.ReadJSON(&msg); err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				fmt.Fprintf(c.Stderr, "ws read error: %v\n", err)
			}
			return nil
		}
		// НОВОЕ: проверка завершения задачи
		if msg.Type == "system" && msg.Event == "task_finished" {
			return nil
		}
		if c.OnMessage != nil {
			c.OnMessage(msg)
		} else {
			c.defaultHandler(msg)
		}
	}
}

func (c *Client) defaultHandler(msg models.ClientMessage) {
	switch msg.Type {
	case "output":
		fmt.Fprint(c.Stdout, msg.Data)
		if msg.Delim != "" {
			fmt.Fprint(c.Stdout, msg.Delim)
		} else {
			if len(msg.Data) > 0 && msg.Data[len(msg.Data)-1] != '\n' {
				fmt.Fprintln(c.Stdout)
			}
		}
	case "system":
		switch msg.Event {
		case "queue_position":
			fmt.Fprintf(c.Stderr, "[queue] position: %d\r", msg.Pos)
		case "task_started":
			fmt.Fprintln(c.Stderr, "[queue] a task started executing")
		default:
			fmt.Fprintf(c.Stderr, "[queue] event: %s\n", msg.Event)
		}
	default:
		fmt.Fprintf(c.Stderr, "[client] unknown message type: %s\n", msg.Type)
	}
}
