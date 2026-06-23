package handler

import (
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
	initialConnectionDeadline   = time.Duration(10 * time.Minute)
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type TaskHandler struct {
	queueMgr   *queue.QueueManager
	log        *slog.Logger
	activeConn atomic.Int64
	maxConn    int64
}

func NewTaskHandler(cfg config.ServerCFG, qm *queue.QueueManager, logger *slog.Logger) *TaskHandler {
	return &TaskHandler{
		queueMgr: qm,
		log:      logger,
		maxConn:  cfg.MaxConnections,
	}
}

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

	// 1. Create session
	sess := newSession(conn, h.log)
	defer sess.Close()

	// 2. Handshake
	handshaker := NewInteractiveHandshake(initialConnectionDeadline, h.log)
	clientID, extra, err := handshaker.Handshake(sess, r)
	if err != nil {
		return
	}
	first := extra.(*models.WSCommand)

	// 3. Build router (the router owns the outbox channel)
	clientCmds := make(chan models.WSCommand, defaultClientCMDsBufferSize)
	router := NewRouter(defaultMsgChanBufferSize, clientCmds, sess.Context(), h.log)

	// 4. Register with the queue using the router’s outbox channel
	h.queueMgr.RegisterSession(clientID, router.Outbox())
	defer h.queueMgr.UnregisterSession(clientID)

	// 5. Create task – use router’s outbox (with type workaround if needed)
	t := &queue.Task{
		Args:        first.Args,
		ClientID:    clientID,
		Interactive: first.Interactive,
		MsgChan:     router.Outbox(), // see type note below
		Ctx:         sess.Context(),
		Cancel:      sess.cancel,
	}

	// 6. Set router handlers
	router.SetOutboxHandler(func(msg models.ClientMessage) {
		h.log.Debug("sending message", "type", msg.Type, "stream", msg.Stream, "data", msg.Data)
		if err := sess.WriteJSON(msg); err != nil {
			h.log.Error("failed to write message", "error", err)
			sess.Close() // stop everything
		}
	})

	router.SetCommandHandler(func(cmd models.WSCommand) {
		h.log.Debug("processing client command", "type", cmd.Type)
		switch cmd.Type {
		case models.WSCmdInput:
			h.queueMgr.FeedInput(clientID, []byte(cmd.Input))
		}
	})

	// 7. When the outbox is closed (task finished), send final message and cancel context
	router.SetOutboxClosedHandler(func() {
		finMsg := models.ClientMessage{
			Type:  "system",
			Event: "task_finished",
		}
		if err := sess.WriteJSON(finMsg); err != nil {
			h.log.Warn("failed to send task_finished", "error", err)
		}
		h.log.Info("task finished, canceling context")
		sess.cancel() // trigger reader shutdown
	})

	// 8. Start reader
	sess.StartReader(clientCmds)

	// 9. Enqueue task
	h.queueMgr.Enqueue(t)

	// 10. Run router (blocks until outbox closes, inbox closes, or context done)
	router.Run()
	h.log.Debug("router returned, waiting for reader to stop")
	<-sess.ReaderDone()
	h.log.Debug("reader stopped, handler returning")
}
