package queue

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/Galdoba/remser/api/models"
	"github.com/Galdoba/remser/internal/infrastructure/config"
	"github.com/google/uuid"
)

// Task – задача, поставленная клиентом.
type Task struct {
	ID          string
	Args        []string
	ClientID    string
	Interactive bool
	MsgChan     chan models.ClientMessage
	Ctx         context.Context
	Cancel      context.CancelFunc
	StdInWriter io.WriteCloser
}

type TaskRunner interface {
	Run(*Task) error
}

// clientSession – активное подключение клиента.
type clientSession struct {
	id      string
	msgChan chan models.ClientMessage
}

// QueueManager управляет очередью задач и сессиями.
type QueueManager struct {
	mu         sync.Mutex
	queue      []*Task
	sessions   map[string]*clientSession
	activeTask *Task
	notify     chan struct{}
	log        *slog.Logger
	runner     TaskRunner
	delay      time.Duration
}

func NewQueueManager(cfg config.ServerCFG, logger *slog.Logger, runner TaskRunner) *QueueManager {
	qm := &QueueManager{
		sessions: make(map[string]*clientSession),
		notify:   make(chan struct{}, 1),
		log:      logger,
		runner:   runner,
		delay:    cfg.TaskDelay,
	}
	go qm.keepAlivePinger()
	return qm
}

// Enqueue добавляет задачу, уведомляет воркер.
func (qm *QueueManager) Enqueue(t *Task) {
	qm.mu.Lock()
	qm.queue = append(qm.queue, t)
	qm.notifyPositionLocked()
	qm.mu.Unlock()

	select {
	case qm.notify <- struct{}{}:
	default:
	}
	t.ID = uuid.Must(uuid.NewV7()).String()
}

// func (h *TaskHandler) generateTaskID() string {
// 	h.nextIDMu.Lock()
// 	defer h.nextIDMu.Unlock()
// 	h.nextID++
// 	return fmt.Sprintf("%d", h.nextID)
// }

// Dequeue извлекает первую задачу, ожидая при необходимости.
func (qm *QueueManager) Dequeue(ctx context.Context) (*Task, error) {
	for {
		qm.mu.Lock()
		if len(qm.queue) > 0 {
			t := qm.queue[0]
			qm.queue = qm.queue[1:]
			qm.activeTask = t
			qm.notifyTaskStartedLocked(t.ID)
			qm.notifyPositionLocked()
			qm.mu.Unlock()
			return t, nil
		}
		qm.mu.Unlock()

		select {
		case <-qm.notify:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Remove удаляет задачу клиента (отменяет активную или убирает из очереди).
func (qm *QueueManager) Remove(clientID, taskID string) {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	if qm.activeTask != nil && qm.activeTask.ClientID == clientID && qm.activeTask.ID == taskID {
		qm.activeTask.Cancel()
		qm.activeTask = nil
		return
	}

	for i, t := range qm.queue {
		if t.ClientID == clientID && t.ID == taskID {
			qm.queue = append(qm.queue[:i], qm.queue[i+1:]...)
			qm.notifyPositionLocked()
			break
		}
	}
	delete(qm.sessions, clientID)
}

// RegisterSession сохраняет сессию клиента (без вывода в лог).
func (qm *QueueManager) RegisterSession(clientID string, msgChan chan models.ClientMessage) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	qm.sessions[clientID] = &clientSession{
		id:      clientID,
		msgChan: msgChan,
	}
}

// UnregisterSession удаляет сессию клиента.
func (qm *QueueManager) UnregisterSession(clientID string) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	delete(qm.sessions, clientID)
}

// notifyPositionLocked отправляет ожидающим клиентам их позицию. Вызывается под локом.
func (qm *QueueManager) notifyPositionLocked() {
	for i, t := range qm.queue {
		pos := i + 1
		msg := models.ClientMessage{
			Type:  "system",
			Event: "queue_position",
			Pos:   pos,
		}
		select {
		case t.MsgChan <- msg:
		default:
		}
	}
}

// notifyTaskStartedLocked оповещает о запуске задачи всех, кроме её владельца.
func (qm *QueueManager) notifyTaskStartedLocked(startedTaskID string) {
	msg := models.ClientMessage{
		Type:  "system",
		Event: "task_started",
	}
	for _, t := range qm.queue {
		if t.ID != startedTaskID {
			select {
			case t.MsgChan <- msg:
			default:
			}
		}
	}
}

// keepAlivePinger периодически обновляет позиции, чтобы соединения не рвались.
func (qm *QueueManager) keepAlivePinger() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		qm.mu.Lock()
		if len(qm.queue) > 0 {
			qm.notifyPositionLocked()
		}
		qm.mu.Unlock()
	}
}

// ExtractClientIP получает IP-адрес клиента из запроса.
func ExtractClientIP(r *http.Request) string {
	// Проверяем заголовок X-Forwarded-For (если за прокси)
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return xff
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (qm *QueueManager) SetActiveStdin(w io.WriteCloser) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	qm.activeTask.StdInWriter = w
}

func (qm *QueueManager) FeedInput(clientID string, data []byte) error {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	if qm.activeTask != nil && qm.activeTask.ClientID == clientID && qm.activeTask.StdInWriter != nil {
		_, err := qm.activeTask.StdInWriter.Write(data)
		return err
	}
	return fmt.Errorf("no active task for client %s", clientID)
}

// CompleteActive сбрасывает активную задачу (вызывается Worker'ом после выполнения).
func (qm *QueueManager) CompleteActive() {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	if qm.activeTask != nil {
		qm.activeTask.StdInWriter = nil
	}
	qm.activeTask = nil
}

// Worker – фоновый цикл обработки очереди.
func (qm *QueueManager) Worker(ctx context.Context) {
	qm.log.Info("worker started")
	for {
		tsk, err := qm.Dequeue(ctx)
		if err != nil {
			qm.log.Error("worker dequeue error (server shutting down?)", "error", err)
			return
		}
		qm.log.Info("Worker dequeued task",
			"taskID", tsk.ID, "clientID", tsk.ClientID, "isInteractive", tsk.Interactive, "args", tsk.Args)
		if err := qm.runner.Run(tsk); err != nil {
			qm.log.Error("task failed", "taskID", tsk.ID, "error", err)
		}
		close(tsk.MsgChan)
		qm.log.Info("worker finished", "taskID", tsk.ID)
		qm.CompleteActive() // сбрасываем активную задачу

		select {
		case <-time.After(qm.delay):
		case <-ctx.Done():
			return
		}
	}
}
