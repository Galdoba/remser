// Package queue provides a task queue manager with client session support.
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

// Event types used in client messages.
const (
	msgTypeSystem      = "system"
	eventQueuePosition = "queue_position"
	eventTaskStarted   = "task_started"
)

// pingInterval defines how often the keep‑alive pinger sends queue positions.
const pingInterval = 2 * time.Second

// Task represents a task submitted by a client.
type Task struct {
	ID          string
	Args        []string
	SessionID   string
	Interactive bool
	MsgChan     chan<- models.ClientMessage
	Ctx         context.Context
	Cancel      context.CancelFunc
	StdInWriter io.WriteCloser
	closeOnce   sync.Once
}

// TaskRunner defines the interface for executing a task.
type TaskRunner interface {
	Run(*Task) error
}

// HealthChecker defines the interface for client health checks.
type HealthChecker interface {
	StartHealthCheck(clientID string, interval, timeout time.Duration)
	StopHealthCheck(clientID string)
}

// ClientSession represents an active client session from the perspective of the queue manager.
// It provides the session ID, a send-only channel for system messages,
// and the ability to start/stop health checks.
type ClientSession interface {
	ID() string
	Outbox() chan<- models.ClientMessage
	HealthChecker
}

// CloseMsgChan safely closes the MsgChan once.
func (t *Task) CloseMsgChan() {
	t.closeOnce.Do(func() {
		close(t.MsgChan)
	})
}

// QueueManager manages the task queue and client sessions.
type QueueManager struct {
	mu         sync.Mutex
	queue      []*Task
	sessions   map[string]ClientSession
	activeTask *Task
	notify     chan struct{}
	log        *slog.Logger
	runner     TaskRunner
	delay      time.Duration
}

// NewQueueManager creates a new QueueManager with the given configuration,
// logger and task runner. It also starts the background keep‑alive pinger.
func NewQueueManager(cfg config.ServerCFG, logger *slog.Logger, runner TaskRunner) *QueueManager {
	qm := &QueueManager{
		sessions: make(map[string]ClientSession),
		notify:   make(chan struct{}, 1),
		log:      logger,
		runner:   runner,
		delay:    cfg.TaskDelay,
	}
	go qm.keepAlivePinger()
	return qm
}

// Enqueue adds a task to the queue, assigns it a unique ID, and notifies
// the worker that a new task is available.
func (qm *QueueManager) Enqueue(task *Task) {
	qm.mu.Lock()
	task.ID = uuid.Must(uuid.NewV7()).String()
	qm.queue = append(qm.queue, task)
	qm.notifyPositionLocked()

	// Non‑blocking send to wake up the worker if it is waiting.
	select {
	case qm.notify <- struct{}{}:
	default:
	}
	qm.mu.Unlock()
}

// Dequeue retrieves the next task from the queue, blocking until a task
// is available or the context is cancelled. It also marks the dequeued
// task as active and notifies waiting clients about queue changes.
func (qm *QueueManager) Dequeue(ctx context.Context) (*Task, error) {
	for {
		qm.mu.Lock()
		if len(qm.queue) > 0 {
			task := qm.queue[0]
			qm.queue = qm.queue[1:]
			qm.activeTask = task
			qm.notifyTaskStartedLocked(task.ID)
			qm.notifyPositionLocked()
			qm.mu.Unlock()
			return task, nil
		}
		qm.mu.Unlock()

		select {
		case <-qm.notify:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Remove cancels and removes a task belonging to the given client.
// If the task is currently active, its context is cancelled and it is
// removed from the active slot. If it is still queued, it is taken out
// of the queue and its message channel is closed.
func (qm *QueueManager) Remove(sessionID, taskID string) {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	if qm.activeTask != nil && qm.activeTask.SessionID == sessionID && qm.activeTask.ID == taskID {
		qm.activeTask.Cancel()
		qm.activeTask = nil
		return
	}

	for i, task := range qm.queue {
		if task.SessionID == sessionID && task.ID == taskID {
			qm.queue = append(qm.queue[:i], qm.queue[i+1:]...)
			qm.cancelAndCloseTask(task)
			qm.notifyPositionLocked()
			break
		}
	}
}

// RegisterSession stores a client session for message delivery and health checking.
func (qm *QueueManager) RegisterSession(sess ClientSession) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	qm.sessions[sess.ID()] = sess
}

// UnregisterSession removes a client session, stops its health checks,
// cancels all of its queued and active tasks, and notifies remaining
// clients about updated queue positions.
func (qm *QueueManager) UnregisterSession(sessionID string) {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	qm.removeSession(sessionID)
	qm.removeQueuedTasksForSession(sessionID)
	qm.cancelActiveTaskIfOwner(sessionID)

	if len(qm.queue) > 0 {
		qm.notifyPositionLocked()
	}
}

// removeSession deletes the client session and stops its health checks.
// Must be called with qm.mu held.
func (qm *QueueManager) removeSession(clientID string) {
	if sess, ok := qm.sessions[clientID]; ok {
		sess.StopHealthCheck(clientID)
		delete(qm.sessions, clientID)
	}
}

// removeQueuedTasksForSession cancels and closes all queued tasks belonging
// to the given client. Must be called with qm.mu held.
func (qm *QueueManager) removeQueuedTasksForSession(sessionID string) {
	for i := 0; i < len(qm.queue); {
		if qm.queue[i].SessionID == sessionID {
			task := qm.queue[i]
			qm.queue = append(qm.queue[:i], qm.queue[i+1:]...)
			qm.cancelAndCloseTask(task)
			// Do not increment i because the slice shifted.
			continue
		}
		i++
	}
}

// cancelActiveTaskIfOwner cancels the currently active task if it belongs
// to the specified client. The task’s MsgChan will be closed by the Worker
// after the Run method returns. Must be called with qm.mu held.
func (qm *QueueManager) cancelActiveTaskIfOwner(sessionID string) {
	if qm.activeTask != nil && qm.activeTask.SessionID == sessionID {
		qm.activeTask.Cancel()
	}
}

// cancelAndCloseTask cancels the task’s context and closes its message channel.
// This is used when a task is permanently removed from the system (e.g. client
// disconnect) and no further processing is expected.
func (qm *QueueManager) cancelAndCloseTask(task *Task) {
	task.Cancel()
	task.CloseMsgChan()
}

// notifyPositionLocked sends each waiting task its current position in the queue.
// Must be called with qm.mu held.
func (qm *QueueManager) notifyPositionLocked() {
	for i, task := range qm.queue {
		pos := i + 1
		msg := models.ClientMessage{
			Type:  msgTypeSystem,
			Event: eventQueuePosition,
			Pos:   pos,
		}
		safeSend(task.MsgChan, msg)
	}
}

// notifyTaskStartedLocked informs all queued tasks (except the one that just started)
// that a task execution has begun. Must be called with qm.mu held.
func (qm *QueueManager) notifyTaskStartedLocked(startedTaskID string) {
	msg := models.ClientMessage{
		Type:  msgTypeSystem,
		Event: eventTaskStarted,
	}
	for _, task := range qm.queue {
		if task.ID != startedTaskID {
			select {
			case task.MsgChan <- msg:
			default:
			}
		}
	}
}

// keepAlivePinger periodically sends queue position updates to prevent
// client connections from timing out.
func (qm *QueueManager) keepAlivePinger() {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for range ticker.C {
		qm.mu.Lock()
		if len(qm.queue) > 0 {
			qm.notifyPositionLocked()
		}
		qm.mu.Unlock()
	}
}

// ExtractClientIP extracts the client IP address from an HTTP request,
// taking into account the X-Forwarded-For header if present.
func ExtractClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return xff
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// SetActiveStdin sets the stdin writer for the currently active task.
// It is typically used when a task requires interactive input.
func (qm *QueueManager) SetActiveStdin(w io.WriteCloser) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	qm.activeTask.StdInWriter = w
}

// FeedInput writes data to the stdin of the active task owned by the given client.
// It returns an error if there is no active task for that client or if writing fails.
func (qm *QueueManager) FeedInput(sessionID string, data []byte) error {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	if qm.activeTask != nil && qm.activeTask.SessionID == sessionID && qm.activeTask.StdInWriter != nil {
		if _, err := qm.activeTask.StdInWriter.Write(data); err != nil {
			return fmt.Errorf("failed to write data to stdin: %w", err)
		}
		return nil
	}
	return fmt.Errorf("no active task for client %s", sessionID)
}

// CompleteActive clears the active task after it has finished executing.
// Called by the Worker.
func (qm *QueueManager) CompleteActive() {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	if qm.activeTask != nil {
		qm.activeTask.StdInWriter = nil
	}
	qm.activeTask = nil
}

// Worker runs the main task processing loop. It dequeues and executes tasks
// one by one, respecting the configured delay between tasks, until the context
// is cancelled.
func (qm *QueueManager) Worker(ctx context.Context) {
	qm.log.Info("worker started")
	for {
		task, err := qm.Dequeue(ctx)
		if err != nil {
			qm.log.Error("worker dequeue error (server shutting down?)", "error", err)
			return
		}
		qm.log.Info("Worker dequeued task", "taskID", task.ID, "clientID", task.SessionID)

		err = qm.runTaskSafe(task)
		if err != nil {
			qm.log.Error("task failed", "taskID", task.ID, "error", err)
		}

		task.CloseMsgChan()
		qm.log.Info("worker finished", "taskID", task.ID)
		qm.CompleteActive()

		select {
		case <-time.After(qm.delay):
		case <-ctx.Done():
			return
		}
	}
}

// runTaskSafe executes the task through the runner, recovering from any panic
// to keep the Worker alive.
func (qm *QueueManager) runTaskSafe(task *Task) (err error) {
	defer func() {
		if r := recover(); r != nil {
			qm.log.Error("task panicked", "taskID", task.ID, "panic", r)
			err = fmt.Errorf("task panicked: %v", r)
		}
	}()
	return qm.runner.Run(task)
}

// Shutdown gracefully stops the queue manager: it cancels all active and
// queued tasks, closes their message channels, and clears the queue.
func (qm *QueueManager) Shutdown() {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	if qm.activeTask != nil {
		qm.activeTask.Cancel()
	}
	for _, task := range qm.queue {
		task.Cancel()
		task.CloseMsgChan()
	}
	qm.queue = nil
}

// safeSend attempts a non‑blocking send on the channel.
// It recovers from a panic caused by sending to a closed channel.
func safeSend(ch chan<- models.ClientMessage, msg models.ClientMessage) {
	defer func() {
		if r := recover(); r != nil {
			// Channel closed; ignore.
		}
	}()
	select {
	case ch <- msg:
	default:
	}
}

// StartHealthCheck delegates the health check start to the client’s session,
// if one exists.
func (qm *QueueManager) StartHealthCheck(clientID string, interval, timeout time.Duration) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	if sess, ok := qm.sessions[clientID]; ok {
		sess.StartHealthCheck(clientID, interval, timeout)
	}
}

// StopHealthCheck delegates the health check stop to the client’s session,
// if one exists.
func (qm *QueueManager) StopHealthCheck(clientID string) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	if sess, ok := qm.sessions[clientID]; ok {
		sess.StopHealthCheck(clientID)
	}
}

// SetRunner replaces the current task runner. This can be used for testing
// or dynamic reconfiguration.
func (qm *QueueManager) SetRunner(r TaskRunner) {
	qm.runner = r
}
