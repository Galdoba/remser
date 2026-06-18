package models

import "time"

type Config struct {
	ListenAddr string
	TaskDelay  time.Duration
}

type ServerTask struct {
	Args        []string `json:"args"`
	ClientID    string   `json:"client_id,omitempty"`
	Interactive bool     `json:"interactive,omitempty"`
	Input       string   `json:"input,omitempty"`
}

type ClientMessage struct {
	Type   string `json:"type"`
	Stream string `json:"stream,omitempty"`
	Data   string `json:"data,omitempty"`
	Event  string `json:"event,omitempty"`
	Pos    int    `json:"position,omitempty"`
	Delim  string `json:"delim,omitempty"`
}

// WSCommand – сообщение от клиента к серверу по WebSocket.
type WSCommand struct {
	Type        string   `json:"type"` // execute / input / detach
	Args        []string `json:"args,omitempty"`
	ClientID    string   `json:"client_id,omitempty"`
	Interactive bool     `json:"interactive,omitempty"`
	Input       string   `json:"input,omitempty"`
}

// Типы команд WebSocket
const (
	WSCmdExecute = "execute"
	WSCmdInput   = "input"
)
