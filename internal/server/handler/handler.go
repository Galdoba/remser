package handler

import (
	"context"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/Galdoba/remser/api/models"
	"github.com/Galdoba/remser/internal/infrastructure/config"
	"github.com/Galdoba/remser/internal/server/queue"
	"github.com/gorilla/websocket"
)

const (
	defaultClientCMDsBufferSize = 8
	defaultMsgChanBufferSize    = 64
	initialConnectionDeadline   = 10 * time.Minute

	msgTypeSystem        = "system"
	msgEventError        = "error"
	msgEventTaskFinished = "task_finished"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// QueueManager defines the contract for managing task queues and client sessions.
// The health parameter in RegisterSession is an object that provides health-check
// capabilities for the session being registered.
type QueueManager interface {
	RegisterSession(sess queue.ClientSession)
	UnregisterSession(clientID string)
	FeedInput(clientID string, input []byte) error
	Enqueue(task *queue.Task)
}

// TaskHandler handles incoming WebSocket connections, upgrades them,
// performs protocol handshake, and wires them into the job queue.
type TaskHandler struct {
	queueMgr   QueueManager
	log        *slog.Logger
	activeConn atomic.Int64
	maxConn    int64
}

// NewTaskHandler creates a TaskHandler with the provided server configuration,
// queue manager and logger.
func NewTaskHandler(cfg config.ServerCFG, queueMgr QueueManager, logger *slog.Logger) *TaskHandler {
	return &TaskHandler{
		queueMgr: queueMgr,
		log:      logger,
		maxConn:  cfg.MaxConnections,
	}
}

// WebSocketHandler is the http.HandlerFunc that accepts WebSocket connections.
func (h *TaskHandler) WebSocketHandler(w http.ResponseWriter, r *http.Request) {
	if h.activeConn.Load() >= h.maxConn {
		h.log.Warn("maximum connections reached", "ip", r.RemoteAddr)
		http.Error(w, "too many connections", http.StatusServiceUnavailable)
		return
	}
	h.activeConn.Add(1)
	defer h.activeConn.Add(-1)

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.log.Error("connection upgrade error", "error", err)
		return
	}

	h.processConnection(conn, r)
}

// processConnection orchestrates the full lifecycle of a WebSocket connection.
func (h *TaskHandler) processConnection(conn *websocket.Conn, r *http.Request) {
	sess := newSession(r.Context(), conn, h.log)
	defer sess.Close()

	sessID, first, err := h.doHandshake(sess, r)
	if err != nil {
		return
	}

	sess.setID(sessID)

	router, t := h.setupSessionRouter(first, sess)
	sess.setOutbox(router.Outbox())

	defer h.queueMgr.UnregisterSession(sess.ID())

	h.queueMgr.RegisterSession(sess)
	h.queueMgr.Enqueue(t)

	router.Run()
	<-sess.ReaderDone()
	h.log.Debug("reader stopped, handler returning")
}

// doHandshake performs the initial protocol handshake and returns the assigned clientID
// together with the parsed first command.
func (h *TaskHandler) doHandshake(sess *Session, r *http.Request) (string, *models.WSCommand, error) {
	handshaker := NewInteractiveHandshake(initialConnectionDeadline, h.log)
	clientID, extra, err := handshaker.Handshake(sess, r)
	if err != nil {
		return "", nil, err
	}
	first := extra.(*models.WSCommand)
	return clientID, first, nil
}

// setupSessionRouter creates the Router, the initial task, configures all handlers,
// and starts the client reader. It returns the router and the task to be enqueued.
func (h *TaskHandler) setupSessionRouter(first *models.WSCommand, sess *Session) (*Router, *queue.Task) {
	clientCmds := make(chan models.WSCommand, defaultClientCMDsBufferSize)
	router := h.createRouter(clientCmds, sess.Context())
	task := h.createTask(taskConfig{
		First:   first,
		Outbox:  router.Outbox(),
		Session: sess,
	})
	h.configureRouterHandlers(routerHandlersConfig{
		Router:     router,
		Session:    sess,
		ClientCmds: clientCmds,
	})
	return router, task
}

// createRouter builds a Router with the default buffer sizes and the given inbox.
func (h *TaskHandler) createRouter(clientCmds chan models.WSCommand, ctx context.Context) *Router {
	cfg := RouterConfig{
		OutboxSize: defaultMsgChanBufferSize,
		Inbox:      clientCmds,
		Ctx:        ctx,
		Logger:     h.log,
	}
	return NewRouter(cfg)
}

// taskConfig holds all parameters needed to create a queue.Task.
type taskConfig struct {
	First   *models.WSCommand
	Outbox  chan<- models.ClientMessage
	Session *Session
}

// createTask builds a queue.Task from the initial command and session context.
func (h *TaskHandler) createTask(cfg taskConfig) *queue.Task {
	return &queue.Task{
		Args:        cfg.First.Args,
		SessionID:   cfg.Session.ID(),
		Interactive: cfg.First.Interactive,
		MsgChan:     cfg.Outbox,
		Ctx:         cfg.Session.Context(),
		Cancel:      cfg.Session.cancel,
	}
}

// routerHandlersConfig bundles the dependencies needed to attach handlers
// to the Router.
type routerHandlersConfig struct {
	Router     *Router
	Session    *Session
	ClientCmds chan models.WSCommand
}

// configureRouterHandlers attaches the four essential handlers to the router and
// starts the reader goroutine that pumps incoming commands into the inbox channel.
func (h *TaskHandler) configureRouterHandlers(cfg routerHandlersConfig) {
	cfg.Router.SetOutboxHandler(h.makeOutboxHandler(cfg.Session))
	cfg.Router.SetCommandHandler(h.makeCommandHandler(cfg.Session.ID()))
	cfg.Router.SetOutboxClosedHandler(h.makeOutboxClosedHandler(cfg.Session))
	cfg.Router.SetInboxClosedHandler(h.makeInboxClosedHandler(cfg.Session))
	cfg.Session.StartReader(cfg.ClientCmds)
}

// makeOutboxHandler returns a function that forwards a message from the queue to the
// WebSocket client. If the write fails, the session is closed because the connection
// can no longer be used reliably.
func (h *TaskHandler) makeOutboxHandler(sess *Session) func(models.ClientMessage) {
	return func(msg models.ClientMessage) {
		h.log.Debug("sending message", "type", msg.Type, "stream", msg.Stream, "data", msg.Data)
		if err := sess.WriteJSON(msg); err != nil {
			h.log.Error("failed to write message", "error", err)
			sess.Close() // write error means the connection is dead, stop everything
		}
	}
}

// makeCommandHandler returns a function that processes commands received from the client.
// Currently, only WSCmdInput is forwarded to the queue manager to feed interactive tasks.
// Other command types are ignored and may be added later.
func (h *TaskHandler) makeCommandHandler(clientID string) func(models.WSCommand) {
	return func(cmd models.WSCommand) {
		h.log.Debug("processing client command", "type", cmd.Type)
		if cmd.Type == models.WSCmdInput {
			if err := h.queueMgr.FeedInput(clientID, []byte(cmd.Input)); err != nil {
				h.log.Error("failed to feed input", "error", err)
			}
		}
	}
}

// makeOutboxClosedHandler returns a function that is called when the queue finishes
// processing the task and closes the outbox. It sends a final "task_finished" system
// message to the client to guarantee an explicit completion signal before closing the
// session.
func (h *TaskHandler) makeOutboxClosedHandler(sess *Session) func() {
	return func() {
		finMsg := models.ClientMessage{
			Type:  msgTypeSystem,
			Event: msgEventTaskFinished,
		}
		if err := sess.WriteJSON(finMsg); err != nil {
			h.log.Warn("failed to send task_finished", "error", err)
		}
		h.log.Info("task finished, canceling context")
		sess.Close()
	}
}

// makeInboxClosedHandler returns a function that is called when the client disconnects
// (the reader goroutine closes the inbox channel). It closes the session, which cancels
// the task's context if it was still running.
func (h *TaskHandler) makeInboxClosedHandler(sess *Session) func() {
	return func() {
		h.log.Info("client disconnected, closing session")
		sess.Close()
	}
}
