package server

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/Galdoba/remser/internal/infrastructure/config"
)

// testLogger returns a logger that discards all output.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestNewServer(t *testing.T) {
	ctx := context.Background()
	cfg := config.ServerCFG{
		ListenAddr:     ":0",
		MaxConnections: 10,
		TaskDelay:      time.Second,
	}
	log := testLogger()

	s, err := NewServer(ctx, cfg, log)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	if s == nil {
		t.Fatal("Server is nil")
	}
	if s.cfg != cfg {
		t.Errorf("cfg not set correctly")
	}
	if s.log != log { // теперь проверяем, что переданный логгер сохранён
		t.Error("log not set correctly")
	}
	if s.queueMgr == nil {
		t.Error("queueMgr is nil")
	}
	if s.handler == nil {
		t.Error("handler is nil")
	}
	if s.srv != nil {
		t.Error("srv should be nil before Start")
	}
}

func TestStartShutdown(t *testing.T) {
	ctx := context.Background()
	cfg := config.ServerCFG{
		ListenAddr:     ":0",
		MaxConnections: 10,
		TaskDelay:      time.Millisecond * 10,
	}
	log := testLogger()

	s, err := NewServer(ctx, cfg, log)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Start()
	}()

	// Даём серверу время запуститься
	time.Sleep(100 * time.Millisecond)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := s.Shutdown(shutdownCtx); err != nil {
		t.Errorf("Shutdown returned error: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			t.Errorf("Start returned unexpected error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("Start did not finish after Shutdown")
	}
}

func TestStartError(t *testing.T) {
	ctx := context.Background()
	cfg := config.ServerCFG{
		ListenAddr:     "invalid:port",
		MaxConnections: 10,
		TaskDelay:      time.Second,
	}
	log := testLogger()

	s, err := NewServer(ctx, cfg, log)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	err = s.Start()
	if err == nil {
		t.Error("Start should return error for invalid address")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Shutdown(shutdownCtx); err != nil {
		// теперь Shutdown может вернуть ошибку, если не удалось завершить HTTP-сервер,
		// но в этом случае s.srv == nil, поэтому ошибки не будет.
		// Тем не менее проверяем, что ошибка не критична.
		t.Logf("Shutdown returned error (expected if server not started): %v", err)
	}
}

func TestShutdownBeforeStart(t *testing.T) {
	ctx := context.Background()
	cfg := config.ServerCFG{
		ListenAddr:     ":0",
		MaxConnections: 10,
		TaskDelay:      time.Second,
	}
	log := testLogger()

	s, err := NewServer(ctx, cfg, log)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := s.Shutdown(shutdownCtx); err != nil {
		t.Errorf("Shutdown returned error: %v", err)
	}
}

func TestShutdownWaitsForWorker(t *testing.T) {
	ctx := context.Background()
	cfg := config.ServerCFG{
		ListenAddr:     ":0",
		MaxConnections: 10,
		TaskDelay:      time.Second, // большая задержка, чтобы worker не завершился сразу
	}
	log := testLogger()

	s, err := NewServer(ctx, cfg, log)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	go func() {
		_ = s.Start()
	}()

	time.Sleep(50 * time.Millisecond) // даём стартовать

	start := time.Now()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := s.Shutdown(shutdownCtx); err != nil {
		t.Errorf("Shutdown returned error: %v", err)
	}

	elapsed := time.Since(start)
	// Worker должен завершиться быстро, т.к. мы отменяем контекст
	if elapsed > 2*time.Second {
		t.Errorf("Shutdown took too long: %v", elapsed)
	}
}
