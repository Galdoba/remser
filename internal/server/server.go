package server

import (
	"context"
	"log/slog"
	"net/http"
	"sync"

	"github.com/Galdoba/remser/internal/infrastructure/config"
	"github.com/Galdoba/remser/internal/server/executor"
	"github.com/Galdoba/remser/internal/server/handler"
	"github.com/Galdoba/remser/internal/server/queue"
)

// Пакетные константы для исключения магических строк.
const (
	wsPath       = "/ws"
	compQueue    = "queue"
	compExecutor = "executor"
	compHandler  = "handler"
)

// Server manages the WebSocket server lifecycle.
// It owns the HTTP server, the task queue, and the worker goroutine.
// The zero value is not usable; use NewServer to create an instance.
type Server struct {
	cfg      config.ServerCFG
	srv      *http.Server
	handler  *handler.TaskHandler
	queueMgr *queue.QueueManager
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	log      *slog.Logger
}

// NewServer creates a new Server instance.
// It initialises the task queue, executor, and HTTP handler.
// The provided logger is used for all internal logging.
// Returns an error if the queue or executor initialisation fails.
// The server is not started until Start is called.
func NewServer(ctx context.Context, cfg config.ServerCFG, log *slog.Logger) (*Server, error) {
	ctx, cancel := context.WithCancel(ctx)

	// Create queue manager with a nil runner first; the runner is set later
	// after the executor is constructed to avoid circular dependency.
	qm := queue.NewQueueManager(cfg, log.With("component", compQueue), nil)

	exec := executor.New(log.With("component", compExecutor), qm)
	qm.SetRunner(exec)

	h := handler.NewTaskHandler(cfg, qm, log.With("component", compHandler))

	return &Server{
		cfg:      cfg,
		handler:  h,
		queueMgr: qm,
		ctx:      ctx,
		cancel:   cancel,
		log:      log,
	}, nil
}

// startWorker runs the queue worker in a separate goroutine.
// It blocks until the context is cancelled.
func (s *Server) startWorker() {
	s.wg.Go(func() {
		s.queueMgr.Worker(s.ctx)
	})
}

// Start begins the worker loop and starts the HTTP server on the configured address.
// It blocks until the server fails or is shut down.
// If the server fails to start, an error is returned.
// Calling Start multiple times is not supported and may cause undefined behaviour.
func (s *Server) Start() error {
	s.startWorker()

	mux := http.NewServeMux()
	mux.HandleFunc(wsPath, s.handler.WebSocketHandler)

	s.srv = &http.Server{
		Addr:    s.cfg.ListenAddr,
		Handler: mux,
	}

	s.log.Info("WebSocket server listening", "address", s.cfg.ListenAddr)
	if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Shutdown gracefully stops the HTTP server, cancels the worker context,
// waits for all goroutines to finish, and releases queue resources.
// It blocks until shutdown is complete or the provided context is cancelled.
// If the HTTP server shutdown fails, the error is returned.
// The Server cannot be reused after Shutdown.
func (s *Server) Shutdown(ctx context.Context) error {
	s.log.Info("shutting down...")
	var err error
	if s.srv != nil {
		if shutdownErr := s.srv.Shutdown(ctx); shutdownErr != nil {
			s.log.Error("failed to shutdown HTTP server", "error", shutdownErr)
			err = shutdownErr
		}
	}
	s.cancel()
	s.wg.Wait()
	s.queueMgr.Shutdown()
	return err
}
