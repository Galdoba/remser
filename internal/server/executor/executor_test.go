package executor

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Galdoba/remser/api/models"
	"github.com/Galdoba/remser/internal/server/queue"
)

// hasSh проверяет доступность sh в системе.
func hasSh() bool {
	_, err := exec.LookPath("sh")
	return err == nil
}

// hasSleep проверяет наличие команды sleep.
func hasSleep() bool {
	_, err := exec.LookPath("sleep")
	return err == nil
}

// newTestTask создает задачу с заданными аргументами и интерактивностью.
// Возвращает задачу и канал сообщений.
func newTestTask(ctx context.Context, args []string, interactive bool) (*queue.Task, chan models.ClientMessage) {
	msgChan := make(chan models.ClientMessage, 10)
	task := &queue.Task{
		ID:          "test-task",
		SessionID:   "test-session",
		Args:        args,
		Interactive: interactive,
		MsgChan:     msgChan,
		Ctx:         ctx,
	}
	return task, msgChan
}

// TestReadToken проверяет функцию readToken на различных входных данных.
func TestReadToken(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []byte
		delim    []byte
	}{
		{
			name:     "simple line with newline",
			input:    "hello\nworld",
			expected: []byte("hello"),
			delim:    []byte{'\n'},
		},
		{
			name:     "line with CRLF",
			input:    "hello\r\nworld",
			expected: []byte("hello"),
			delim:    []byte{'\r', '\n'},
		},
		{
			name:     "line with CR only",
			input:    "hello\rworld",
			expected: []byte("hello"),
			delim:    []byte{'\r'},
		},
		{
			name:     "no delimiter at end",
			input:    "hello",
			expected: []byte("hello"),
			delim:    nil,
		},
		{
			name:     "empty input",
			input:    "",
			expected: nil,
			delim:    nil,
		},
		{
			name:     "multiple lines, first read",
			input:    "line1\nline2\n",
			expected: []byte("line1"),
			delim:    []byte{'\n'},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tt.input))
			token, delim, err := readToken(reader)

			if err != nil && err != io.EOF {
				t.Fatalf("unexpected error: %v", err)
			}
			if !bytes.Equal(token, tt.expected) {
				t.Errorf("expected token %q, got %q", tt.expected, token)
			}
			if !bytes.Equal(delim, tt.delim) {
				t.Errorf("expected delim %q, got %q", tt.delim, delim)
			}
		})
	}
}

// TestExecuteTaskSimple проверяет выполнение простой команды без интерактива.
func TestExecuteTaskSimple(t *testing.T) {
	if !hasSh() {
		t.Skip("sh not found, skipping")
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	executor := New(logger, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	task, msgChan := newTestTask(ctx, []string{"sh", "-c", "echo hello world"}, false)

	err := executor.executeTask(task)
	if err != nil {
		t.Fatalf("executeTask failed: %v", err)
	}

	select {
	case msg := <-msgChan:
		if msg.Type != "output" || msg.Stream != "stdout" {
			t.Errorf("unexpected message: %+v", msg)
		}
		if msg.Data != "hello world" {
			t.Errorf("expected 'hello world', got '%s'", msg.Data)
		}
		if string(msg.Delim) != "\n" {
			t.Errorf("expected delim '\\n', got '%q'", msg.Delim)
		}
	case <-time.After(1 * time.Second):
		t.Error("timeout waiting for message")
	}

	select {
	case msg, ok := <-msgChan:
		if ok {
			t.Errorf("unexpected extra message: %+v", msg)
		}
	default:
	}
}

// TestExecuteTaskStderr проверяет, что сообщения из stderr корректно отправляются.
func TestExecuteTaskStderr(t *testing.T) {
	if !hasSh() {
		t.Skip("sh not found, skipping")
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	executor := New(logger, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	args := []string{"sh", "-c", "echo error message >&2"}
	task, msgChan := newTestTask(ctx, args, false)

	err := executor.executeTask(task)
	if err != nil {
		t.Fatalf("executeTask failed: %v", err)
	}

	select {
	case msg := <-msgChan:
		if msg.Stream != "stderr" {
			t.Errorf("expected stderr stream, got %s", msg.Stream)
		}
		if msg.Data != "error message" {
			t.Errorf("expected 'error message', got '%s'", msg.Data)
		}
	case <-time.After(1 * time.Second):
		t.Error("timeout waiting for stderr message")
	}
}

// TestExecuteTaskWithError проверяет выполнение несуществующей команды.
func TestExecuteTaskWithError(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	executor := New(logger, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	task, msgChan := newTestTask(ctx, []string{"nonexistentcommand"}, false)

	err := executor.executeTask(task)
	if err == nil {
		t.Error("expected error but got nil")
	}

	select {
	case msg := <-msgChan:
		if msg.Stream != "stderr" {
			t.Errorf("expected stderr stream, got %s", msg.Stream)
		}
		if msg.Data == "" {
			t.Error("expected non-empty error message")
		}
	case <-time.After(1 * time.Second):
		t.Error("timeout waiting for error message")
	}
}

// TestExecuteTaskCancel проверяет отмену контекста во время выполнения.
func TestExecuteTaskCancel(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skipping on Windows: context cancellation may not work with sleep")
	}
	if !hasSleep() {
		t.Skip("sleep not found, skipping")
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	executor := New(logger, nil)

	ctx, cancel := context.WithCancel(context.Background())
	task, _ := newTestTask(ctx, []string{"sleep", "2"}, false)

	errCh := make(chan error, 1)
	go func() {
		errCh <- executor.executeTask(task)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("expected nil error after cancellation, got: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("timeout waiting for command to finish after cancellation")
	}
}

// TestExecuteTaskMultipleLines проверяет, что каждая строка отправляется отдельным сообщением.
func TestExecuteTaskMultipleLines(t *testing.T) {
	if !hasSh() {
		t.Skip("sh not found, skipping")
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	executor := New(logger, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	args := []string{"sh", "-c", "printf 'line1\nline2\nline3\n'"}
	task, msgChan := newTestTask(ctx, args, false)

	err := executor.executeTask(task)
	if err != nil {
		t.Fatalf("executeTask failed: %v", err)
	}

	var lines []string
	for i := 0; i < 3; i++ {
		select {
		case msg := <-msgChan:
			if msg.Stream != "stdout" {
				t.Errorf("expected stdout, got %s", msg.Stream)
			}
			lines = append(lines, msg.Data)
		case <-time.After(1 * time.Second):
			t.Fatalf("timeout waiting for message %d", i+1)
		}
	}

	expected := []string{"line1", "line2", "line3"}
	if !bytes.Equal([]byte(strings.Join(lines, ",")), []byte(strings.Join(expected, ","))) {
		t.Errorf("expected lines %v, got %v", expected, lines)
	}
}

// TestExecuteTaskNoArgs проверяет обработку задачи без аргументов.
func TestExecuteTaskNoArgs(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	executor := New(logger, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	task, msgChan := newTestTask(ctx, []string{}, false)

	err := executor.executeTask(task)
	if err == nil {
		t.Error("expected error for no args, got nil")
	}

	select {
	case msg := <-msgChan:
		if msg.Stream != "stderr" {
			t.Errorf("expected stderr stream, got %s", msg.Stream)
		}
		if !strings.Contains(msg.Data, "no args") {
			t.Errorf("expected error message about no args, got %s", msg.Data)
		}
	case <-time.After(1 * time.Second):
		t.Error("timeout waiting for error message")
	}
}

// TestExecuteTaskWithDelimiters проверяет разные разделители (CR, LF, CRLF).
func TestExecuteTaskWithDelimiters(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skipping on Windows: line endings may be normalized")
	}
	if !hasSh() {
		t.Skip("sh not found, skipping")
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	executor := New(logger, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	args := []string{"sh", "-c", "printf 'hello\r\nworld\rtest\n'"}
	task, msgChan := newTestTask(ctx, args, false)

	err := executor.executeTask(task)
	if err != nil {
		t.Fatalf("executeTask failed: %v", err)
	}

	expected := []struct {
		data  string
		delim string
	}{
		{"hello", "\r\n"},
		{"world", "\r"},
		{"test", "\n"},
	}

	for i, exp := range expected {
		select {
		case msg := <-msgChan:
			if msg.Data != exp.data {
				t.Errorf("message %d: expected data %q, got %q", i, exp.data, msg.Data)
			}
			if string(msg.Delim) != exp.delim {
				t.Errorf("message %d: expected delim %q, got %q", i, exp.delim, msg.Delim)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("timeout waiting for message %d", i)
		}
	}
}

// TestExecuteTaskWithLargeOutput проверяет, что большой вывод обрабатывается без проблем.
func TestExecuteTaskWithLargeOutput(t *testing.T) {
	if !hasSh() {
		t.Skip("sh not found, skipping")
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	executor := New(logger, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	args := []string{"sh", "-c", "yes | head -n 1000"}
	task, msgChan := newTestTask(ctx, args, false)

	errCh := make(chan error, 1)
	go func() {
		errCh <- executor.executeTask(task)
	}()

	count := 0
	timeout := time.After(5 * time.Second)
loop:
	for {
		select {
		case msg, ok := <-msgChan:
			if !ok {
				break loop
			}
			if msg.Stream != "stdout" {
				t.Errorf("unexpected stream: %s", msg.Stream)
			}
			count++
			if count >= 1000 {
				break loop
			}
		case <-timeout:
			t.Fatalf("timeout, received %d messages", count)
		}
	}

	if count != 1000 {
		t.Errorf("expected 1000 messages, got %d", count)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("executeTask returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("executeTask did not finish")
	}
}

// TestConcurrentExecutions проверяет, что несколько задач могут выполняться параллельно.
func TestConcurrentExecutions(t *testing.T) {
	if !hasSh() {
		t.Skip("sh not found, skipping")
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	executor := New(logger, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			task, _ := newTestTask(ctx, []string{"sh", "-c", "echo hello"}, false)
			err := executor.executeTask(task)
			if err != nil {
				t.Errorf("task %d failed: %v", id, err)
			}
		}(i)
	}
	wg.Wait()
}

// вспомогательная функция должна быть определена в файле теста
func waitForStdinWriter(task *queue.Task, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if task.StdInWriter != nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for StdInWriter")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestExecuteTaskInteractive проверяет интерактивный режим: запись в stdin и получение вывода.
func TestExecuteTaskInteractive(t *testing.T) {
	if !hasSh() {
		t.Skip("sh not found, skipping")
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	executor := New(logger, nil)

	ready := make(chan struct{})
	ctx := context.WithValue(context.Background(), "ready", ready)
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	task, msgChan := newTestTask(ctx, []string{"sh", "-c", "cat"}, true)

	errCh := make(chan error, 1)
	go func() {
		errCh <- executor.executeTask(task)
	}()

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for StdInWriter")
	}

	input := "hello interactive\n"
	if _, err := task.StdInWriter.Write([]byte(input)); err != nil {
		t.Fatalf("failed to write to stdin: %v", err)
	}
	task.StdInWriter.Close()

	select {
	case msg := <-msgChan:
		if msg.Stream != "stdout" {
			t.Errorf("expected stdout stream, got %s", msg.Stream)
		}
		if msg.Data != "hello interactive" {
			t.Errorf("expected 'hello interactive', got '%s'", msg.Data)
		}
	case <-time.After(2 * time.Second):
		t.Error("timeout waiting for output")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("executeTask returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("executeTask did not finish")
	}
}

// TestExecuteTaskWithStdinClose проверяет, что при интерактивном режиме закрытие stdin завершает процесс.
func TestExecuteTaskWithStdinClose(t *testing.T) {
	if !hasSh() {
		t.Skip("sh not found, skipping")
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	executor := New(logger, nil)

	ready := make(chan struct{})
	ctx := context.WithValue(context.Background(), "ready", ready)
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	task, _ := newTestTask(ctx, []string{"sh", "-c", "cat"}, true)

	errCh := make(chan error, 1)
	go func() {
		errCh <- executor.executeTask(task)
	}()

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for StdInWriter")
	}

	task.StdInWriter.Close()

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("executeTask returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("executeTask did not finish after stdin close")
	}
}
