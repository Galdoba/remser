package handler

import (
	"context"
	"log/slog"

	"github.com/Galdoba/remser/api/models"
)

// Router multiplexes messages to/from a WebSocket session.
type Router struct {
	outbox         chan models.ClientMessage // to client
	inbox          <-chan models.WSCommand   // from client
	ctx            context.Context
	log            *slog.Logger
	onOutbox       func(models.ClientMessage) // called when a message should be written to client
	onCommand      func(models.WSCommand)     // called when a command is received from client
	onOutboxClosed func()                     // called once when outbox is closed
}

// NewRouter creates a Router. outbox capacity is configurable.
func NewRouter(outboxSize int, inbox <-chan models.WSCommand, ctx context.Context,
	log *slog.Logger) *Router {
	return &Router{
		outbox: make(chan models.ClientMessage, outboxSize),
		inbox:  inbox,
		ctx:    ctx,
		log:    log,
	}
}

// SetOutboxHandler sets the function that writes a message to the websocket.
func (r *Router) SetOutboxHandler(f func(models.ClientMessage)) {
	r.onOutbox = f
}

// SetCommandHandler sets the function that processes incoming commands.
func (r *Router) SetCommandHandler(f func(models.WSCommand)) {
	r.onCommand = f
}

// Outbox returns the channel to which the queue should send messages for the client.
func (r *Router) Outbox() chan<- models.ClientMessage {
	return r.outbox
}

func (r *Router) SetOutboxClosedHandler(f func()) {
	r.onOutboxClosed = f
}

// Run starts the router's main loop. It blocks until the context is done.
func (r *Router) Run() {
	for {
		select {
		case msg, ok := <-r.outbox:
			if !ok {
			}
			if !ok {
				if r.onOutboxClosed != nil {
					r.onOutboxClosed()
				}
				r.log.Debug("router: outbox closed")
				return
			}
			if r.onOutbox != nil {
				r.onOutbox(msg)
			}

		case cmd, ok := <-r.inbox:
			if !ok {
				r.log.Debug("router: inbox closed")
				return
			}
			if r.onCommand != nil {
				r.onCommand(cmd)
			}

		case <-r.ctx.Done():
			r.log.Debug("router: context done")
			return
		}
	}
}
