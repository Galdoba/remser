// router.go
package handler

import (
	"context"
	"log/slog"

	"github.com/Galdoba/remser/api/models"
)

// RouterConfig holds the configuration for creating a Router.
type RouterConfig struct {
	OutboxSize int
	Inbox      <-chan models.WSCommand
	Ctx        context.Context
	Logger     *slog.Logger
}

// Router multiplexes messages between the queue (outbox) and the WebSocket client (inbox).
type Router struct {
	outbox         chan models.ClientMessage
	inbox          <-chan models.WSCommand
	ctx            context.Context
	log            *slog.Logger
	onOutbox       func(models.ClientMessage)
	onCommand      func(models.WSCommand)
	onInboxClosed  func()
	onOutboxClosed func()
}

// NewRouter creates a Router with the provided configuration.
func NewRouter(cfg RouterConfig) *Router {
	return &Router{
		outbox: make(chan models.ClientMessage, cfg.OutboxSize),
		inbox:  cfg.Inbox,
		ctx:    cfg.Ctx,
		log:    cfg.Logger,
	}
}

// SetOutboxHandler sets the function that writes a message to the WebSocket connection.
func (r *Router) SetOutboxHandler(f func(models.ClientMessage)) {
	r.onOutbox = f
}

// SetCommandHandler sets the function that processes incoming commands from the client.
func (r *Router) SetCommandHandler(f func(models.WSCommand)) {
	r.onCommand = f
}

// SetOutboxClosedHandler sets the function called when the outbox channel is closed.
func (r *Router) SetOutboxClosedHandler(f func()) {
	r.onOutboxClosed = f
}

// SetInboxClosedHandler sets the function called when the inbox channel is closed.
func (r *Router) SetInboxClosedHandler(f func()) {
	r.onInboxClosed = f
}

// Outbox returns the send-only channel to which the queue writes messages for the client.
func (r *Router) Outbox() chan<- models.ClientMessage {
	return r.outbox
}

// Run starts the main event loop. It blocks until the context is cancelled,
// the outbox is closed, or the inbox is closed.
func (r *Router) Run() {
	for {
		select {
		case msg, ok := <-r.outbox:
			if !r.handleOutbox(msg, ok) {
				return
			}
		case cmd, ok := <-r.inbox:
			if !r.handleInbox(cmd, ok) {
				return
			}
		case <-r.ctx.Done():
			r.log.Debug("router: context done")
			return
		}
	}
}

// handleOutbox processes a single message from the outbox channel.
// hasValue is false when the outbox channel has been closed and no more messages will arrive.
// It returns false if the loop should exit (channel closed).
func (r *Router) handleOutbox(msg models.ClientMessage, hasValue bool) bool {
	if !hasValue {
		r.ensureOutboxClosed()
		r.log.Debug("router: outbox closed")
		return false
	}
	if r.onOutbox != nil {
		r.onOutbox(msg)
	}
	return true
}

// ensureOutboxClosed invokes the outbox-closed callback if it was set.
func (r *Router) ensureOutboxClosed() {
	if r.onOutboxClosed != nil {
		r.onOutboxClosed()
	}
}

// handleInbox processes a single command from the inbox channel.
// hasValue is false when the inbox channel has been closed (client disconnected).
// It returns false if the loop should exit.
func (r *Router) handleInbox(cmd models.WSCommand, hasValue bool) bool {
	if !hasValue {
		r.ensureInboxClosed()
		r.log.Debug("router: inbox closed")
		return false
	}
	if r.onCommand != nil {
		r.onCommand(cmd)
	}
	return true
}

// ensureInboxClosed invokes the inbox-closed callback if it was set.
func (r *Router) ensureInboxClosed() {
	if r.onInboxClosed != nil {
		r.onInboxClosed()
	}
}
