package queue

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/Galdoba/remser/api/models"
)

// Task – задача, поставленная клиентом.
type Task struct {
	ID          string
	Args        []string
	ClientID    string
	Interactive bool // новое
	MsgChan     chan models.ClientMessage
	Ctx         context.Context
	Cancel      context.CancelFunc
}

// clientSession – активное подключение клиента.
type clientSession struct {
	id      string
	msgChan chan models.ClientMessage
}

// QueueManager управляет очередью задач и сессиями.
type QueueManager struct {
	mu          sync.Mutex
	queue       []*Task
	sessions    map[string]*clientSession
	activeTask  *Task
	notify      chan struct{}
	activeStdin io.Writer // stdin pipe активной задачи
}

func NewQueueManager() *QueueManager {
	qm := &QueueManager{
		sessions: make(map[string]*clientSession),
		notify:   make(chan struct{}, 1),
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
}

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

func (qm *QueueManager) SetActiveStdin(w io.Writer) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	qm.activeStdin = w
}

func (qm *QueueManager) FeedInput(clientID string, data []byte) error {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	if qm.activeTask != nil && qm.activeTask.ClientID == clientID && qm.activeStdin != nil {
		_, err := qm.activeStdin.Write(data)
		return err
	}
	return fmt.Errorf("no active task for client %s", clientID)
}

// CompleteActive сбрасывает активную задачу (вызывается Worker'ом после выполнения).
func (qm *QueueManager) CompleteActive() {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	qm.activeTask = nil
	qm.activeStdin = nil
}
