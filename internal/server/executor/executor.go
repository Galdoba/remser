// Package executor provides task execution with optional health checks.
package executor

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"sync"
	"time"

	"github.com/Galdoba/remser/api/models"
	"github.com/Galdoba/remser/internal/server/queue"
)

// HealthStarter defines the interface for health check management.
type HealthStarter interface {
	StartHealthCheck(clientID string, interval, timeout time.Duration)
	StopHealthCheck(clientID string)
}

// Executor manages execution of tasks (commands) with optional health checks.
type Executor struct {
	log    *slog.Logger
	health HealthStarter
}

// New creates a new Executor with the provided logger and health check starter.
func New(logger *slog.Logger, health HealthStarter) *Executor {
	return &Executor{
		log:    logger,
		health: health,
	}
}

// Run executes the given task. It calls executeTask to run the command and
// stream output to the task's message channel. Run returns any error encountered.
func (e *Executor) Run(task *queue.Task) error {
	return e.executeTask(task)
}

// Constants for readability and maintainability.
const (
	healthCheckInterval = 5 * time.Second
	healthCheckTimeout  = 10 * time.Second
	msgTypeOutput       = "output"
	streamStdout        = "stdout"
	streamStderr        = "stderr"
	errNoArgs           = "task has no args"
	contextKeyReady     = "ready"
)

// executeTask runs a single task: validates, prepares, starts the command,
// streams output, and performs cleanup.
func (e *Executor) executeTask(task *queue.Task) error {
	e.log.Info("executeTask starting", "taskID", task.ID)

	cmdCtx, cmdCancel := context.WithCancel(task.Ctx)
	defer cmdCancel()

	if err := e.validateArgs(task, cmdCtx); err != nil {
		return err
	}

	cmd, stdout, stderr, err := e.prepareCommand(task, cmdCtx)
	if err != nil {
		return e.handleStartError(task, cmdCtx, err)
	}

	if err := cmd.Start(); err != nil {
		return e.handleStartError(task, cmdCtx, err)
	}
	e.log.Info("process started", "taskID", task.ID, "pid", cmd.Process.Pid)

	e.startHealthCheck(task)
	defer e.stopHealthCheck(task)

	e.readOutput(stdout, stderr, task, cmdCtx)

	// In interactive mode, closing stdin unblocks the process.
	if task.Interactive && task.StdInWriter != nil {
		e.log.Info("closing stdin", "taskID", task.ID)
		task.StdInWriter.Close()
	}

	err = cmd.Wait()
	e.log.Info("process finished", "taskID", task.ID, "error", err)
	return nil
}

// validateArgs ensures the task has at least one argument.
// If not, it sends an error message and returns an error.
func (e *Executor) validateArgs(task *queue.Task, ctx context.Context) error {
	if len(task.Args) > 0 {
		return nil
	}
	e.log.Error("task has no args, skipping", "taskID", task.ID)
	msg := models.ClientMessage{
		Type:   msgTypeOutput,
		Stream: streamStderr,
		Data:   errNoArgs,
	}
	select {
	case task.MsgChan <- msg:
	case <-ctx.Done():
		return ctx.Err()
	}
	return fmt.Errorf(errNoArgs)
}

// prepareCommand creates the exec.Cmd, sets up stdin for interactive mode,
// and obtains stdout/stderr pipes.
func (e *Executor) prepareCommand(task *queue.Task, ctx context.Context) (*exec.Cmd, io.ReadCloser, io.ReadCloser, error) {
	cmd := exec.CommandContext(ctx, task.Args[0], task.Args[1:]...)
	if task.Interactive {
		stdin, sw := io.Pipe()
		cmd.Stdin = stdin
		task.StdInWriter = sw
		if ch, ok := task.Ctx.Value(contextKeyReady).(chan struct{}); ok {
			close(ch)
		}
		e.log.Warn("interactive mode: stdin pipe set", "taskID", task.ID)
	}
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()
	return cmd, stdout, stderr, nil
}

// handleStartError sends the start error to the task's message channel and returns it.
func (e *Executor) handleStartError(task *queue.Task, ctx context.Context, err error) error {
	e.log.Error("failed to start command", "error", err)
	msg := models.ClientMessage{
		Type:   msgTypeOutput,
		Stream: streamStderr,
		Data:   err.Error(),
	}
	select {
	case task.MsgChan <- msg:
	case <-ctx.Done():
		return ctx.Err()
	}
	return err
}

// startHealthCheck initiates the health check if a health starter is configured.
func (e *Executor) startHealthCheck(task *queue.Task) {
	if e.health != nil {
		e.health.StartHealthCheck(task.SessionID, healthCheckInterval, healthCheckTimeout)
	}
}

// stopHealthCheck stops the health check if a health starter is configured.
func (e *Executor) stopHealthCheck(task *queue.Task) {
	if e.health != nil {
		e.health.StopHealthCheck(task.SessionID)
	}
}

// readOutput launches goroutines to stream stdout and stderr, then waits for them to finish.
func (e *Executor) readOutput(stdout, stderr io.ReadCloser, task *queue.Task, ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(2)
	go e.streamOutput(stdout, streamStdout, task, ctx, &wg)
	go e.streamOutput(stderr, streamStderr, task, ctx, &wg)
	wg.Wait()
}

// streamOutput reads from the given reader, breaks it into tokens using readToken,
// and sends each token as a ClientMessage over the task's message channel.
func (e *Executor) streamOutput(reader io.Reader, stream string, task *queue.Task, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	r := bufio.NewReader(reader)
	for {
		token, delim, err := readToken(r)
		if err != nil {
			if err == io.EOF {
				e.log.Debug(stream+" EOF", "taskID", task.ID)
			} else {
				e.log.Error(stream+" read error", "taskID", task.ID, "error", err)
			}
			return
		}
		msg := models.ClientMessage{
			Type:   msgTypeOutput,
			Stream: stream,
			Data:   string(token),
			Delim:  string(delim),
		}
		select {
		case task.MsgChan <- msg:
		case <-ctx.Done():
			e.log.Error(stream+" routine canceled", "error", ctx.Err())
			return
		}
	}
}

// readToken reads a token from the buffered reader up to a line delimiter
// (\n, \r, or \r\n). It returns the token bytes, the delimiter bytes, and any error.
func readToken(r *bufio.Reader) (token []byte, delim []byte, err error) {
	var buf bytes.Buffer
	for {
		b, err := r.ReadByte()
		if err != nil {
			return handleReadError(buf, err)
		}
		if b == '\n' {
			return buf.Bytes(), []byte{'\n'}, nil
		}
		if b == '\r' {
			return readCarriageReturn(r, &buf)
		}
		buf.WriteByte(b)
	}
}

// handleReadError processes an error from ReadByte. If EOF and buffer has data,
// it returns the buffered token without a delimiter.
func handleReadError(buf bytes.Buffer, err error) ([]byte, []byte, error) {
	if err == io.EOF && buf.Len() > 0 {
		return buf.Bytes(), nil, nil
	}
	return nil, nil, err
}

// readCarriageReturn handles a carriage return character by peeking ahead
// for a possible line feed (\n). It returns the buffered token and the
// appropriate delimiter.
func readCarriageReturn(r *bufio.Reader, buf *bytes.Buffer) ([]byte, []byte, error) {
	nextB, err := r.ReadByte()
	if err == nil {
		if nextB == '\n' {
			return buf.Bytes(), []byte{'\r', '\n'}, nil
		}
		// Not LF – unread the byte.
		r.UnreadByte()
	}
	return buf.Bytes(), []byte{'\r'}, nil
}
