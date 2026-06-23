package handler

/*
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
		h.log.Warn("maximum connections reached, refused connection", "ip", r.RemoteAddr)
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
	defer func() {
		h.log.Info("closing connection")
		conn.Close()
	}()

	conn.SetReadDeadline(time.Now().Add(initialConnectionDeadline))
	var first models.WSCommand
	if err := conn.ReadJSON(&first); err != nil {
		h.log.Warn("first message deadline reached", "ip", conn.RemoteAddr().String())
		return
	}
	conn.SetReadDeadline(time.Time{}) // remove deadline for the rest of the session

	h.log.Info("received first message", "type", first.Type, "id", first.ClientID, "interactive", first.Interactive, "args", first.Args)
	if first.Type != models.WSCmdExecute {
		conn.WriteJSON(models.ClientMessage{
			Type:  "system",
			Event: "error",
			Data:  "first message must be type=execute",
		})
		return
	}

	clientID := first.ClientID
	if clientID == "" {
		clientID = queue.ExtractClientIP(r)
		h.log.Warn("no clientID provided, using IP", "ip", clientID)
	}

	msgChan := make(chan models.ClientMessage, defaultMsgChanBufferSize)

	h.queueMgr.RegisterSession(clientID, msgChan)
	defer func() {
		h.log.Info("unregistering session", "clientID", clientID)
		h.queueMgr.UnregisterSession(clientID)
	}()

	// Контекст задачи
	ctx, cancel := context.WithCancel(r.Context())
	defer func() {
		h.log.Info("canceling context", "client_id", clientID)
		cancel()
	}()

	// garantees ReadJSON to unblock if ctx is canceled
	go func() {
		<-ctx.Done()
		conn.SetReadDeadline(time.Now()) // immediate deadline -> ReadJSON returns error
	}()

	clientCmds := make(chan models.WSCommand, defaultClientCMDsBufferSize)

	t := &queue.Task{
		Args:        first.Args,
		ClientID:    clientID,
		Interactive: first.Interactive,
		MsgChan:     msgChan,
		Ctx:         ctx,
		Cancel:      cancel,
	}

	// WebSocket reader routine
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			// read with assumption of ctx cancelation (deadline is set by other routine)
			var cmd models.WSCommand
			err := conn.ReadJSON(&cmd)
			if err != nil {
				// if ctx is canceled - return as expected
				if ctx.Err() != nil {
					h.log.Info("reader: context canceled, exiting")
					return
				}
				// otherwise is connection error
				h.log.Error("reader: connection error", "error", err)
				return
			}
			h.log.Info("reader: received command", "type", cmd.Type, "input", cmd.Input)
			select {
			case clientCmds <- cmd:
			case <-ctx.Done():
				h.log.Warn("reader: context done while sending command")
				return
			}
		}
	}()

	h.queueMgr.Enqueue(t)

	h.log.Info("entering main loop", "taskID", t.ID)
	for {
		select {
		case msg, ok := <-msgChan:
			if !ok {
				h.log.Info("[WS] msgChan closed – task finished", "taskID", t.ID)
				finMsg := models.ClientMessage{
					Type:  "system",
					Event: "task_finished",
				}
				if err := conn.WriteJSON(finMsg); err != nil {
					h.log.Warn("failed to send task_finished", "error", err)
				} else {
					h.log.Info("sent task_finished to client")
				}
				// cancel ctx and wait routine to stop
				cancel()
				h.log.Info("waiting for reader goroutine to stop")
				<-readerDone // is bound to stop now
				h.log.Info("reader goroutine stopped, returning")
				return
			}
			h.log.Info("sending message",
				"type", msg.Type, "stream", msg.Stream, "data", msg.Data)
			if err := conn.WriteJSON(msg); err != nil {
				h.log.Error("failed to write message", "error", err)
				return
			}

		case cmd, ok := <-clientCmds:
			if !ok {
				//never fire, but keep 'ok', just in case
			}
			h.log.Info("processing client command", "type", cmd.Type)
			switch cmd.Type {
			case models.WSCmdInput:
				if err := h.queueMgr.FeedInput(clientID, []byte(cmd.Input)); err != nil {
					h.log.Error("feed input", "error", err)
				}
			}

		case <-ctx.Done():
			h.log.Info("ctx.Done in main loop", "taskID", t.ID)
			return
		}
	}
}
*/
