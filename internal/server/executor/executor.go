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

	"github.com/Galdoba/remser/api/models"
	"github.com/Galdoba/remser/internal/server/queue"
)

type Executor struct {
	log *slog.Logger
}

func New(logger *slog.Logger) *Executor {
	return &Executor{
		log: logger,
	}
}

func (e *Executor) Run(t *queue.Task) error {
	return e.executeTask(t)
}

func (e *Executor) executeTask(t *queue.Task) error {

	e.log.Info("executeTask starting", "taskID", t.ID)
	cmdCtx, cmdCancel := context.WithCancel(t.Ctx)
	defer func() {
		e.log.Warn("executeTask cancel context", "taskID", t.ID)
		cmdCancel()
	}()

	if len(t.Args) == 0 {
		e.log.Error("executeTask task has no args, skipping", "taskID", t.ID)
		msg := models.ClientMessage{
			Type:   "output",
			Stream: "stderr",
			Data:   "task has no args",
		}
		select {
		case t.MsgChan <- msg:
		case <-cmdCtx.Done():
			return cmdCtx.Err()
		}
		return fmt.Errorf("task has no args")
	}

	cmd := exec.CommandContext(cmdCtx, t.Args[0], t.Args[1:]...)

	if t.Interactive {
		stdin, sw := io.Pipe()
		cmd.Stdin = stdin
		t.StdInWriter = sw
		if ch, ok := t.Ctx.Value("ready").(chan struct{}); ok {
			close(ch)
		}
		e.log.Warn("executeTask interactive mode: stdin pipe is set", "taskID", t.ID)
	}

	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		e.log.Error("executeTask failed to start command", "error", err)
		msg := models.ClientMessage{
			Type:   "output",
			Stream: "stderr",
			Data:   err.Error(),
		}
		select {
		case t.MsgChan <- msg:
		case <-cmdCtx.Done():
			return cmdCtx.Err()
		}
		return err
	}
	e.log.Info("executeTask process started", "taskID", t.ID, "processPID", cmd.Process.Pid)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		reader := bufio.NewReader(stdout)
		for {
			token, delim, err := readToken(reader)
			if err != nil {
				if err == io.EOF {
					e.log.Info("executeTask stdout EOF", "taskID", t.ID)
				} else {
					e.log.Error("executeTask stdout read error", "taskID", t.ID, "error", err)
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
				e.log.Error("stdout routine canceled", "error", cmdCtx.Err())
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
					e.log.Info("executeTask stderr EOF", "taskID", t.ID)
				} else {
					e.log.Error("executeTask stderr read error", "taskID", t.ID, "error", err)
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
				e.log.Error("stderr routine canceled", "error", cmdCtx.Err())
				return
			}
		}
	}()

	// Дожидаемся завершения чтения stdout/stderr
	wg.Wait()

	// Если интерактивный режим – закрываем stdin, чтобы разблокировать процесс
	if t.Interactive && t.StdInWriter != nil {
		e.log.Info("executeTask closing stdin to unblock process", "taskID", t.ID)
		t.StdInWriter.Close()
	}

	err := cmd.Wait()
	e.log.Info("executeTask process finished", "taskID", t.ID, "error", err)
	return nil
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
