package handler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/Galdoba/remser/api/models"
	"github.com/gorilla/websocket"
)

const (
	writeTimeout = 10 * time.Second
	readTimeout  = 90 * time.Second
)

// Session wraps a WebSocket connection with its context and lifecycle.
type Session struct {
	id           string
	conn         *websocket.Conn
	ctx          context.Context
	cancel       context.CancelFunc
	readerDone   chan struct{}
	log          *slog.Logger
	once         sync.Once
	healthCtx    context.Context
	healthCancel context.CancelFunc
	healthWg     sync.WaitGroup
	outbox       chan<- models.ClientMessage
}

func newSession(ctx context.Context, conn *websocket.Conn, log *slog.Logger) *Session {
	ctx, cancel := context.WithCancel(ctx)
	s := &Session{
		conn:       conn,
		ctx:        ctx,
		cancel:     cancel,
		readerDone: make(chan struct{}),
		log:        log,
	}

	conn.SetPingHandler(func(appData string) error {
		s.log.Debug("received ping from client")
		deadline := time.Now().Add(readTimeout)
		conn.SetReadDeadline(deadline)
		return conn.WriteControl(websocket.PongMessage, nil, time.Now().Add(writeTimeout))
	})

	conn.SetPongHandler(func(appData string) error {
		s.log.Debug("received pong from client")
		conn.SetReadDeadline(time.Now().Add(readTimeout))
		return nil
	})

	return s
}

func (s *Session) setID(id string) {
	s.id = id
}

func (s *Session) ID() string {
	return s.id
}

func (s *Session) setOutbox(ch chan<- models.ClientMessage) {
	s.outbox = ch
}

func (s *Session) Outbox() chan<- models.ClientMessage {
	return s.outbox
}

func (s *Session) StartHealthCheck(clientID string, interval, timeout time.Duration) {
	s.healthCtx, s.healthCancel = context.WithCancel(s.ctx)
	s.healthWg.Add(1)
	go func() {
		defer s.healthWg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.healthCtx.Done():
				return
			case <-ticker.C:
				s.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
				if err := s.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					s.log.Warn("health check: failed to write ping, closing session", "error", err)
					s.Close()
					return
				}
			}
		}
	}()
}

func (s *Session) StopHealthCheck(clientID string) {
	if s.healthCancel != nil {
		s.healthCancel()
		s.healthWg.Wait()
		s.healthCancel = nil
	}
}

func (s *Session) Close() {
	s.once.Do(func() {
		s.StopHealthCheck("")
		s.cancel()
		s.conn.Close()
		s.log.Info("session closed")
	})
}

func (s *Session) ReadJSON(v any) error {
	if s.ctx.Err() != nil {
		s.conn.SetReadDeadline(time.Now())
	}
	return s.conn.ReadJSON(v)
}

func (s *Session) WriteJSON(v any) error {
	return s.conn.WriteJSON(v)
}

func (s *Session) Context() context.Context {
	return s.ctx
}

func (s *Session) ReaderDone() <-chan struct{} {
	return s.readerDone
}

func (s *Session) StartReader(cmds chan<- models.WSCommand) {
	go func() {
		defer close(s.readerDone)
		for {
			s.conn.SetReadDeadline(time.Now().Add(readTimeout))
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
				return
			}
		}
	}()
}
