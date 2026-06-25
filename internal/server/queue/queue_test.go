package queue

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Galdoba/remser/api/models"
	"github.com/Galdoba/remser/internal/infrastructure/config"
)

// --- Mock TaskRunner ---

type mockRunner struct {
	mu       sync.Mutex
	runCalls []*Task
	runErr   error
	blockCh  chan struct{} // если задан, Run будет ждать его закрытия
}

func (m *mockRunner) Run(t *Task) error {
	m.mu.Lock()
	m.runCalls = append(m.runCalls, t)
	m.mu.Unlock()
	if m.blockCh != nil {
		<-m.blockCh
	}
	return m.runErr
}

func (m *mockRunner) lastTask() *Task {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.runCalls) == 0 {
		return nil
	}
	return m.runCalls[len(m.runCalls)-1]
}

func (m *mockRunner) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.runCalls)
}

type mockSession struct {
	id     string
	outbox chan<- models.ClientMessage
}

func (m *mockSession) ID() string                                            { return m.id }
func (m *mockSession) Outbox() chan<- models.ClientMessage                   { return m.outbox }
func (m *mockSession) StartHealthCheck(string, time.Duration, time.Duration) {}
func (m *mockSession) StopHealthCheck(string)                                {}

// --- Helpers ---

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, nil))
}

func testConfig() config.ServerCFG {
	return config.ServerCFG{
		TaskDelay: 10 * time.Millisecond,
	}
}

// newTestTask создаёт задачу с уникальным ID и буферизованным каналом.
func newTestTask(sessionID string, args []string) (*Task, chan models.ClientMessage) {
	msgChan := make(chan models.ClientMessage, 10)
	ctx, cancel := context.WithCancel(context.Background())
	task := &Task{
		Args:      args,
		SessionID: sessionID,
		MsgChan:   msgChan,
		Ctx:       ctx,
		Cancel:    cancel,
	}
	return task, msgChan
}

// readWithTimeout пытается прочитать из канала с таймаутом.
func readWithTimeout(ch <-chan models.ClientMessage, d time.Duration) (models.ClientMessage, error) {
	select {
	case msg, ok := <-ch:
		if !ok {
			return models.ClientMessage{}, errors.New("channel closed")
		}
		return msg, nil
	case <-time.After(d):
		return models.ClientMessage{}, errors.New("timeout")
	}
}

// drainAndCheckClosed вычитывает все сообщения из канала и возвращает true,
// если канал был закрыт. Использует таймаут для определения открытости.
func drainAndCheckClosed(ch <-chan models.ClientMessage, timeout time.Duration) bool {
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return true // канал закрыт
			}
			// читаем дальше
		case <-time.After(timeout):
			return false // канал всё ещё открыт
		}
	}
}

// --- Tests ---

func TestNewQueueManager(t *testing.T) {
	cfg := testConfig()
	logger := testLogger()
	runner := &mockRunner{}
	qm := NewQueueManager(cfg, logger, runner)
	if qm == nil {
		t.Fatal("expected non-nil QueueManager")
	}
	if qm.log != logger {
		t.Error("logger not set")
	}
	if qm.runner != runner {
		t.Error("runner not set")
	}
	if qm.delay != cfg.TaskDelay {
		t.Error("delay not set")
	}
	// keepAlivePinger должна запуститься
	time.Sleep(100 * time.Millisecond)
}

func TestEnqueueDequeue(t *testing.T) {
	qm := NewQueueManager(testConfig(), testLogger(), &mockRunner{})
	task, _ := newTestTask("client-1", []string{"cmd"})
	qm.Enqueue(task)
	if task.ID == "" {
		t.Error("task ID should be set by Enqueue")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	dequeued, err := qm.Dequeue(ctx)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	if dequeued.ID != task.ID {
		t.Errorf("expected task ID %s, got %s", task.ID, dequeued.ID)
	}
	if qm.activeTask != dequeued {
		t.Error("activeTask not set")
	}
}

func TestEnqueueNotify(t *testing.T) {
	qm := NewQueueManager(testConfig(), testLogger(), &mockRunner{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dequeuedCh := make(chan *Task, 1)
	go func() {
		t, _ := qm.Dequeue(ctx)
		dequeuedCh <- t
	}()

	time.Sleep(50 * time.Millisecond)

	task, _ := newTestTask("client-2", []string{"cmd"})
	qm.Enqueue(task)

	select {
	case d := <-dequeuedCh:
		if d.ID != task.ID {
			t.Errorf("unexpected task %s", d.ID)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Dequeue was not woken up")
	}
}

func TestDequeueContextCancel(t *testing.T) {
	qm := NewQueueManager(testConfig(), testLogger(), &mockRunner{})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := qm.Dequeue(ctx)
		errCh <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Dequeue didn't return after cancel")
	}
}

func TestRemoveActiveTask(t *testing.T) {
	qm := NewQueueManager(testConfig(), testLogger(), &mockRunner{})
	task, msgChan := newTestTask("client-3", []string{"cmd"})
	qm.Enqueue(task)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	dequeued, err := qm.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}

	qm.Remove(dequeued.SessionID, dequeued.ID)

	// Проверяем, что контекст задачи отменён
	select {
	case <-dequeued.Ctx.Done():
	default:
		t.Error("task context should be cancelled")
	}

	// activeTask должен быть nil
	qm.mu.Lock()
	at := qm.activeTask
	qm.mu.Unlock()
	if at != nil {
		t.Error("activeTask should be nil after Remove")
	}

	// MsgChan не должен быть закрыт менеджером (только Worker'ом, но Worker не запущен)
	select {
	case _, ok := <-msgChan:
		if !ok {
			t.Error("MsgChan was closed unexpectedly")
		}
	default:
	}
}

func TestRemoveQueuedTask(t *testing.T) {
	qm := NewQueueManager(testConfig(), testLogger(), &mockRunner{})
	task, msgChan := newTestTask("client-4", []string{"cmd"})
	qm.Enqueue(task) // добавляет сообщение в канал

	qm.Remove(task.SessionID, task.ID)

	// Вычитываем все сообщения и ждём закрытия
	if !drainAndCheckClosed(msgChan, 100*time.Millisecond) {
		t.Error("MsgChan should be closed after removal from queue")
	}

	// Очередь пуста
	qm.mu.Lock()
	qLen := len(qm.queue)
	qm.mu.Unlock()
	if qLen != 0 {
		t.Errorf("queue length expected 0, got %d", qLen)
	}
}

func TestRegisterUnregisterSession_RemovesTasks(t *testing.T) {
	qm := NewQueueManager(testConfig(), testLogger(), &mockRunner{})
	clientID := "client-5"

	task1, ch1 := newTestTask(clientID, []string{"cmd1"})
	task2, ch2 := newTestTask(clientID, []string{"cmd2"})
	qm.Enqueue(task1)
	qm.Enqueue(task2)

	otherTask, otherCh := newTestTask("other", []string{"cmd"})
	qm.Enqueue(otherTask)

	// Создаём мок-сессию и регистрируем её (вместо старого RegisterSession(nil))
	outbox := make(chan models.ClientMessage, 1)
	ms := &mockSession{id: clientID, outbox: outbox}
	qm.RegisterSession(ms)

	qm.UnregisterSession(clientID)

	qm.mu.Lock()
	qLen := len(qm.queue)
	qm.mu.Unlock()
	if qLen != 1 {
		t.Fatalf("expected 1 task left (other), got %d", qLen)
	}
	if qm.queue[0].SessionID != "other" {
		t.Errorf("remaining task clientID = %s, want 'other'", qm.queue[0].SessionID)
	}

	// Контексты задач клиента должны быть отменены
	select {
	case <-task1.Ctx.Done():
	default:
		t.Error("task1 context not cancelled")
	}
	select {
	case <-task2.Ctx.Done():
	default:
		t.Error("task2 context not cancelled")
	}

	// Каналы задач клиента должны быть закрыты
	if !drainAndCheckClosed(ch1, 100*time.Millisecond) {
		t.Error("ch1 should be closed")
	}
	if !drainAndCheckClosed(ch2, 100*time.Millisecond) {
		t.Error("ch2 should be closed")
	}

	// Канал other не должен быть закрыт
	select {
	case _, ok := <-otherCh:
		if !ok {
			t.Error("otherCh should not be closed")
		}
	default:
	}
}

func TestUnregisterSession_CancelsActiveTask(t *testing.T) {
	qm := NewQueueManager(testConfig(), testLogger(), &mockRunner{})
	clientID := "client-active"

	task, _ := newTestTask(clientID, []string{"cmd"})
	qm.Enqueue(task)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	d, err := qm.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if d.ID != task.ID {
		t.Fatal("dequeued wrong task")
	}

	qm.UnregisterSession(clientID)

	select {
	case <-task.Ctx.Done():
	default:
		t.Error("active task context should be cancelled")
	}

	qm.mu.Lock()
	at := qm.activeTask
	qm.mu.Unlock()
	if at == nil {
		t.Error("activeTask should still be set (cancelled but not cleared)")
	}
}

func TestFeedInputSuccess(t *testing.T) {
	qm := NewQueueManager(testConfig(), testLogger(), &mockRunner{})
	clientID := "feed-client"

	pr, pw := io.Pipe()
	defer pr.Close()
	task, _ := newTestTask(clientID, []string{"cmd"})
	task.StdInWriter = pw
	qm.mu.Lock()
	qm.activeTask = task
	qm.mu.Unlock()

	input := []byte("hello\n")
	go func() {
		err := qm.FeedInput(clientID, input)
		if err != nil {
			t.Errorf("FeedInput returned error: %v", err)
		}
	}()

	buf := make([]byte, len(input))
	_, err := pr.Read(buf)
	if err != nil {
		t.Fatalf("read from pipe failed: %v", err)
	}
	if string(buf) != string(input) {
		t.Errorf("expected %q, got %q", input, buf)
	}
}

func TestFeedInputNoActiveTask(t *testing.T) {
	qm := NewQueueManager(testConfig(), testLogger(), &mockRunner{})
	err := qm.FeedInput("nobody", []byte("data"))
	if err == nil {
		t.Error("expected error when no active task")
	}
}

func TestFeedInputWrongClient(t *testing.T) {
	qm := NewQueueManager(testConfig(), testLogger(), &mockRunner{})
	pr, pw := io.Pipe()
	defer pr.Close()
	task, _ := newTestTask("correct", []string{"cmd"})
	task.StdInWriter = pw
	qm.mu.Lock()
	qm.activeTask = task
	qm.mu.Unlock()

	err := qm.FeedInput("wrong", []byte("data"))
	if err == nil {
		t.Error("expected error for wrong client")
	}
}

func TestCompleteActive(t *testing.T) {
	qm := NewQueueManager(testConfig(), testLogger(), &mockRunner{})
	pr, pw := io.Pipe()
	task, _ := newTestTask("c", []string{"cmd"})
	task.StdInWriter = pw
	qm.mu.Lock()
	qm.activeTask = task
	qm.mu.Unlock()

	qm.CompleteActive()
	qm.mu.Lock()
	if qm.activeTask != nil {
		t.Error("activeTask should be nil after CompleteActive")
	}
	qm.mu.Unlock()

	_ = pr.Close()
}

func TestWorkerProcessesTask(t *testing.T) {
	runner := &mockRunner{}
	qm := NewQueueManager(testConfig(), testLogger(), runner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go qm.Worker(ctx)
	time.Sleep(20 * time.Millisecond)

	task, msgChan := newTestTask("worker-client", []string{"run"})
	qm.Enqueue(task)

	var last *Task
	for i := 0; i < 50; i++ {
		last = runner.lastTask()
		if last != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if last == nil {
		t.Fatal("Runner.Run was not called")
	}
	if last.ID != task.ID {
		t.Errorf("wrong task passed to Run: %s", last.ID)
	}

	// После возврата Run Worker должен закрыть MsgChan и сбросить активную задачу
	if !drainAndCheckClosed(msgChan, 200*time.Millisecond) {
		t.Error("MsgChan should be closed after task completion")
	}

	time.Sleep(50 * time.Millisecond) // задержка на обработку
	qm.mu.Lock()
	at := qm.activeTask
	qm.mu.Unlock()
	if at != nil {
		t.Error("activeTask should be nil after worker completed")
	}

	cancel()
}

func TestWorkerStopsOnContextCancel(t *testing.T) {
	runner := &mockRunner{}
	qm := NewQueueManager(testConfig(), testLogger(), runner)

	ctx, cancel := context.WithCancel(context.Background())
	workerExited := make(chan struct{})
	go func() {
		qm.Worker(ctx)
		close(workerExited)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-workerExited:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Worker did not exit after context cancel")
	}
}

func TestWorkerRespectsDelay(t *testing.T) {
	runner := &mockRunner{}
	cfg := testConfig()
	cfg.TaskDelay = 100 * time.Millisecond
	qm := NewQueueManager(cfg, testLogger(), runner)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go qm.Worker(ctx)
	time.Sleep(30 * time.Millisecond)

	t1, _ := newTestTask("d1", []string{"a"})
	t2, _ := newTestTask("d2", []string{"b"})
	qm.Enqueue(t1)
	qm.Enqueue(t2)

	for runner.callCount() < 1 {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // половина задержки
	if runner.callCount() > 1 {
		t.Error("second task started too early, delay not respected?")
	}

	for runner.callCount() < 2 {
		select {
		case <-ctx.Done():
			t.Fatal("timeout waiting for second task")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Каналы закрывать не нужно — Worker закроет их сам
}

func TestKeepAlivePinger(t *testing.T) {
	qm := NewQueueManager(testConfig(), testLogger(), &mockRunner{})
	time.Sleep(100 * time.Millisecond)

	task, _ := newTestTask("pinger", []string{"cmd"})
	qm.Enqueue(task)
	time.Sleep(250 * time.Millisecond)
	// отсутствие паники — успех
}

func TestExtractClientIP(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		xff        string
		want       string
	}{
		{"IPv4 with port", "192.168.1.1:1234", "", "192.168.1.1"},
		{"IPv6 with port", "[::1]:8080", "", "::1"},
		{"X-Forwarded-For", "10.0.0.1:123", "203.0.113.1", "203.0.113.1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/", nil)
			req.RemoteAddr = tt.remoteAddr
			if tt.xff != "" {
				req.Header.Set("X-Forwarded-For", tt.xff)
			}
			ip := ExtractClientIP(req)
			if ip != tt.want {
				t.Errorf("got %q, want %q", ip, tt.want)
			}
		})
	}
}

func TestSetActiveStdin(t *testing.T) {
	qm := NewQueueManager(testConfig(), testLogger(), &mockRunner{})
	pr, pw := io.Pipe()
	defer pr.Close()

	task, _ := newTestTask("stdin", []string{"cmd"})
	qm.mu.Lock()
	qm.activeTask = task
	qm.mu.Unlock()

	qm.SetActiveStdin(pw)
	if task.StdInWriter != pw {
		t.Error("StdInWriter not set")
	}
}

func TestNotifyPositionLocked(t *testing.T) {
	qm := NewQueueManager(testConfig(), testLogger(), &mockRunner{})

	task1, ch1 := newTestTask("np1", []string{"a"})
	task2, ch2 := newTestTask("np2", []string{"b"})
	qm.mu.Lock()
	qm.queue = append(qm.queue, task1, task2)
	qm.notifyPositionLocked()
	qm.mu.Unlock()

	msg1, err := readWithTimeout(ch1, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("task1 didn't receive position: %v", err)
	}
	if msg1.Event != "queue_position" || msg1.Pos != 1 {
		t.Errorf("task1 expected pos 1, got %d event %s", msg1.Pos, msg1.Event)
	}

	msg2, err := readWithTimeout(ch2, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("task2 didn't receive position: %v", err)
	}
	if msg2.Pos != 2 {
		t.Errorf("task2 expected pos 2, got %d", msg2.Pos)
	}
}

func TestNotifyTaskStartedLocked(t *testing.T) {
	qm := NewQueueManager(testConfig(), testLogger(), &mockRunner{})

	task1, ch1 := newTestTask("nt1", []string{"a"})
	task2, ch2 := newTestTask("nt2", []string{"b"})
	task3, ch3 := newTestTask("nt3", []string{"c"})

	task1.ID = "task1"
	task2.ID = "task2"
	task3.ID = "task3"

	qm.mu.Lock()
	qm.queue = append(qm.queue, task1, task2, task3)
	qm.notifyTaskStartedLocked(task2.ID) // task2 стартует
	qm.mu.Unlock()

	msg1, err := readWithTimeout(ch1, 50*time.Millisecond)
	if err != nil || msg1.Event != "task_started" {
		t.Errorf("task1 expected task_started, got %v err=%v", msg1, err)
	}

	select {
	case msg := <-ch2:
		t.Errorf("task2 should NOT get task_started, got %v", msg)
	default:
	}

	msg3, err := readWithTimeout(ch3, 50*time.Millisecond)
	if err != nil || msg3.Event != "task_started" {
		t.Errorf("task3 expected task_started, got %v err=%v", msg3, err)
	}
}
