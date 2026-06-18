package handler

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os/exec"
	"sync"
	"time"

	"github.com/Galdoba/remser/api/models"
	"github.com/Galdoba/remser/internal/server/queue"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type TaskHandler struct {
	cfg      models.Config
	queueMgr *queue.QueueManager
	ctx      context.Context
	nextID   int
	nextIDMu sync.Mutex
}

func NewTaskHandler(cfg models.Config, serverCtx context.Context) *TaskHandler {
	return &TaskHandler{
		cfg:      cfg,
		queueMgr: queue.NewQueueManager(),
		ctx:      serverCtx,
	}
}

func (h *TaskHandler) WebSocketHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[WS] upgrade error: %v", err)
		return
	}
	defer func() {
		log.Printf("[WS] closing connection")
		conn.Close()
	}()

	// Первое сообщение всегда execute
	var first models.WSCommand
	if err := conn.ReadJSON(&first); err != nil {
		log.Printf("[WS] read first message error: %v", err)
		return
	}
	log.Printf("[WS] received first message: type=%s clientID=%s interactive=%v args=%v",
		first.Type, first.ClientID, first.Interactive, first.Args)
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
		log.Printf("[WS] no clientID provided, using IP: %s", clientID)
	}

	taskID := h.generateTaskID()
	log.Printf("[WS] generated taskID: %s", taskID)

	msgChan := make(chan models.ClientMessage, 64)

	h.queueMgr.RegisterSession(clientID, msgChan)
	defer func() {
		log.Printf("[WS] unregistering session %s", clientID)
		h.queueMgr.UnregisterSession(clientID)
	}()

	// Контекст задачи
	ctx, cancel := context.WithCancel(h.ctx)
	defer func() {
		log.Printf("[WS] canceling context for task %s", taskID)
		cancel()
	}()

	clientCmds := make(chan models.WSCommand, 1)

	// Горутина чтения WebSocket
	readerDone := make(chan struct{})
	go func() {
		defer func() {
			log.Printf("[WS] reader goroutine exiting for task %s", taskID)
			close(readerDone)
		}()
		for {
			if ctx.Err() != nil {
				// Задаём короткий дедлайн, чтобы разблокировать ReadJSON
				conn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
				log.Printf("[WS] context canceled, set read deadline for task %s", taskID)
			}
			var cmd models.WSCommand
			err := conn.ReadJSON(&cmd)
			if err != nil {
				if ctx.Err() != nil {
					log.Printf("[WS] reader: context canceled, exiting (err=%v)", err)
					return
				}
				log.Printf("[WS] reader: connection error: %v", err)
				return
			}
			log.Printf("[WS] reader: received command type=%s input='%s'", cmd.Type, cmd.Input)
			select {
			case clientCmds <- cmd:
			case <-ctx.Done():
				log.Printf("[WS] reader: context done while sending command")
				return
			}
		}
	}()

	t := &queue.Task{
		ID:          taskID,
		Args:        first.Args,
		ClientID:    clientID,
		Interactive: first.Interactive,
		MsgChan:     msgChan,
		Ctx:         ctx,
		Cancel:      cancel,
	}

	log.Printf("[WS] enqueuing task %s", taskID)
	h.queueMgr.Enqueue(t)

	log.Printf("[WS] entering main loop for task %s", taskID)
	for {
		select {
		case msg, ok := <-msgChan:
			if !ok {
				log.Printf("[WS] msgChan closed for task %s – task finished", taskID)
				finMsg := models.ClientMessage{
					Type:  "system",
					Event: "task_finished",
				}
				if err := conn.WriteJSON(finMsg); err != nil {
					log.Printf("[WS] failed to send task_finished: %v", err)
				} else {
					log.Printf("[WS] sent task_finished to client")
				}
				// Отменяем контекст и ждём остановки читающей горутины
				cancel()
				log.Printf("[WS] waiting for reader goroutine to stop")
				<-readerDone
				log.Printf("[WS] reader goroutine stopped, returning")
				return
			}
			log.Printf("[WS] sending message type=%s stream=%s data='%s'",
				msg.Type, msg.Stream, msg.Data)
			if err := conn.WriteJSON(msg); err != nil {
				log.Printf("[WS] write error: %v", err)
				return
			}

		case cmd, ok := <-clientCmds:
			if !ok {
				log.Printf("[WS] clientCmds closed – client disconnected")
				return
			}
			log.Printf("[WS] processing client command: type=%s", cmd.Type)
			switch cmd.Type {
			case models.WSCmdInput:
				if err := h.queueMgr.FeedInput(clientID, []byte(cmd.Input)); err != nil {
					log.Printf("[WS] feed input error: %v", err)
				}
			}

		case <-ctx.Done():
			log.Printf("[WS] ctx.Done in main loop for task %s", taskID)
			return
		}
	}
}

// Worker – фоновый цикл обработки очереди.
func (h *TaskHandler) Worker(ctx context.Context) {
	log.Printf("[Worker] started")
	for {
		t, err := h.queueMgr.Dequeue(ctx)
		if err != nil {
			log.Printf("[Worker] dequeue error (server shutting down?): %v", err)
			return
		}
		log.Printf("[Worker] dequeued task %s (client=%s, interactive=%v, args=%v)",
			t.ID, t.ClientID, t.Interactive, t.Args)
		h.executeTask(t)
		close(t.MsgChan)
		log.Printf("[Worker] finished task %s", t.ID)
		h.queueMgr.CompleteActive() // сбрасываем активную задачу

		select {
		case <-time.After(h.cfg.TaskDelay):
		case <-ctx.Done():
			return
		}
	}
}

func (h *TaskHandler) executeTask(t *queue.Task) {
	if len(t.Args) == 0 {
		log.Printf("[executeTask] task %s has no args, skipping", t.ID)
		return
	}

	log.Printf("[executeTask] starting task %s", t.ID)
	cmdCtx, cmdCancel := context.WithCancel(t.Ctx)
	defer func() {
		log.Printf("[executeTask] cancel context for task %s", t.ID)
		cmdCancel()
	}()

	cmd := exec.CommandContext(cmdCtx, t.Args[0], t.Args[1:]...)

	var stdinWriter io.WriteCloser
	if t.Interactive {
		stdin, sw := io.Pipe()
		cmd.Stdin = stdin
		stdinWriter = sw
		h.queueMgr.SetActiveStdin(stdinWriter)
		log.Printf("[executeTask] interactive mode: stdin pipe set for task %s", t.ID)
		defer func() {
			log.Printf("[executeTask] closing stdin pipe for task %s", t.ID)
			stdinWriter.Close()
			h.queueMgr.SetActiveStdin(nil)
		}()
	}

	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		log.Printf("[executeTask] failed to start command: %v", err)
		msg := models.ClientMessage{
			Type:   "output",
			Stream: "stderr",
			Data:   err.Error(),
		}
		select {
		case t.MsgChan <- msg:
		default:
		}
		return
	}
	log.Printf("[executeTask] process started for task %s (PID=%d)", t.ID, cmd.Process.Pid)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		reader := bufio.NewReader(stdout)
		for {
			token, delim, err := readToken(reader)
			if err != nil {
				if err == io.EOF {
					log.Printf("[executeTask] stdout EOF for task %s", t.ID)
				} else {
					log.Printf("[executeTask] stdout read error for task %s: %v", t.ID, err)
				}
				break
			}
			msg := models.ClientMessage{
				Type:   "output",
				Stream: "stdout",
				Data:   string(token),
				Delim:  string(delim),
			}
			select {
			case t.MsgChan <- msg:
			case <-cmdCtx.Done():
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		reader := bufio.NewReader(stderr)
		for {
			token, delim, err := readToken(reader)
			if err != nil {
				if err == io.EOF {
					log.Printf("[executeTask] stderr EOF for task %s", t.ID)
				} else {
					log.Printf("[executeTask] stderr read error for task %s: %v", t.ID, err)
				}
				break
			}
			msg := models.ClientMessage{
				Type:   "output",
				Stream: "stderr",
				Data:   string(token),
				Delim:  string(delim),
			}
			select {
			case t.MsgChan <- msg:
			case <-cmdCtx.Done():
				return
			}
		}
	}()

	// Дожидаемся завершения чтения stdout/stderr
	wg.Wait()

	// Если интерактивный режим – закрываем stdin, чтобы разблокировать процесс
	if t.Interactive && stdinWriter != nil {
		log.Printf("[executeTask] closing stdin to unblock process for task %s", t.ID)
		stdinWriter.Close()
	}

	// Теперь процесс должен завершиться
	err := cmd.Wait()
	log.Printf("[executeTask] process finished for task %s, err=%v", t.ID, err)
}

func readToken(r *bufio.Reader) (token []byte, delim []byte, err error) {
	var buf bytes.Buffer
	for {
		b, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				if buf.Len() > 0 {
					return buf.Bytes(), nil, nil
				}
				return nil, nil, err
			}
			return nil, nil, err
		}
		if b == '\n' {
			return buf.Bytes(), []byte{'\n'}, nil
		}
		if b == '\r' {
			nextB, err := r.ReadByte()
			if err == nil {
				if nextB == '\n' {
					return buf.Bytes(), []byte{'\r', '\n'}, nil
				}
				r.UnreadByte()
			}
			return buf.Bytes(), []byte{'\r'}, nil
		}
		buf.WriteByte(b)
	}
}

func (h *TaskHandler) generateTaskID() string {
	h.nextIDMu.Lock()
	defer h.nextIDMu.Unlock()
	h.nextID++
	return fmt.Sprintf("%d", h.nextID)
}
