package handler

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/Galdoba/remser/api/models"
	"github.com/Galdoba/remser/internal/infrastructure/config"
	"github.com/Galdoba/remser/internal/server/queue"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type TaskHandler struct {
	queueMgr *queue.QueueManager
	nextID   int
	nextIDMu sync.Mutex
	log      *slog.Logger
}

func NewTaskHandler(cfg config.ServerCFG, qm *queue.QueueManager, logger *slog.Logger) *TaskHandler {
	return &TaskHandler{
		queueMgr: qm,
		log:      logger,
	}
}

func (h *TaskHandler) WebSocketHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.log.Error("connection upgrade error", "error", err)
		return
	}
	defer func() {
		h.log.Info("closing connection")
		conn.Close()
	}()

	// Первое сообщение всегда execute
	var first models.WSCommand
	if err := conn.ReadJSON(&first); err != nil {
		h.log.Error("read first message", "error", err)
		return
	}
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

	msgChan := make(chan models.ClientMessage, 64)

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

	clientCmds := make(chan models.WSCommand, 1)

	t := &queue.Task{
		Args:        first.Args,
		ClientID:    clientID,
		Interactive: first.Interactive,
		MsgChan:     msgChan,
		Ctx:         ctx,
		Cancel:      cancel,
	}

	// Горутина чтения WebSocket
	readerDone := make(chan struct{})
	go func() {
		defer func() {
			close(readerDone)
		}()
		for {
			if ctx.Err() != nil {
				// Задаём короткий дедлайн, чтобы разблокировать ReadJSON
				conn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
				h.log.Warn("context canceled, set read deadline", "taskID", t.ID)
			}
			var cmd models.WSCommand
			err := conn.ReadJSON(&cmd)
			if err != nil {
				if ctx.Err() != nil {
					h.log.Error("reader: context canceled, exiting", "error", err)
					return
				}
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
				// Отменяем контекст и ждём остановки читающей горутины
				cancel()
				h.log.Info("waiting for reader goroutine to stop")
				<-readerDone
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
				h.log.Info("clientCmds closed – client disconnected")
				return
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
