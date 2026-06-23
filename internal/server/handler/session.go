package handler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/Galdoba/remser/api/models"
	"github.com/gorilla/websocket"
)

// Session wraps a websocket connection and its associated context.
type Session struct {
	conn       *websocket.Conn
	ctx        context.Context
	cancel     context.CancelFunc
	readerDone chan struct{}
	log        *slog.Logger
	once       sync.Once // ensures close is called once
}

func newSession(conn *websocket.Conn, log *slog.Logger) *Session {
	ctx, cancel := context.WithCancel(context.Background()) // will be replaced by request context if needed
	return &Session{
		conn:       conn,
		ctx:        ctx,
		cancel:     cancel,
		readerDone: make(chan struct{}),
		log:        log,
	}
}

// Close cancels the context and closes the connection. Safe to call multiple times.
func (s *Session) Close() {
	s.once.Do(func() {
		s.cancel()
		s.conn.Close()
		s.log.Info("session closed")
	})
}

// ReadJSON reads a JSON message from the websocket, respecting context cancellation
// by setting an immediate read deadline when the context is done.
func (s *Session) ReadJSON(v interface{}) error {
	// If context already cancelled, immediately set a deadline to unblock any pending read.
	if s.ctx.Err() != nil {
		s.conn.SetReadDeadline(time.Now())
	}
	return s.conn.ReadJSON(v)
}

// WriteJSON sends a JSON message to the websocket.
func (s *Session) WriteJSON(v interface{}) error {
	return s.conn.WriteJSON(v)
}

// Context returns the session's context.
func (s *Session) Context() context.Context {
	return s.ctx
}

// ReaderDone returns a channel that is closed when the reader goroutine has finished.
func (s *Session) ReaderDone() <-chan struct{} {
	return s.readerDone
}

// StartReader spawns a goroutine that reads from the websocket and forwards commands to the given channel.
// It sets readerDone when finished.
func (s *Session) StartReader(cmds chan<- models.WSCommand) {
	go func() {
		defer close(s.readerDone)
		for {
			var cmd models.WSCommand
			err := s.ReadJSON(&cmd)
			if err != nil {
				if s.ctx.Err() != nil {
					s.log.Debug("reader: context canceled, exiting")
				} else {
					s.log.Error("reader: connection error", "error", err)
				}
				return
			}
			s.log.Info("reader: received command", "type", cmd.Type, "input", cmd.Input)
			select {
			case cmds <- cmd:
			case <-s.ctx.Done():
				s.log.Warn("reader: context done while sending command")
				return
			}
		}
	}()
}
