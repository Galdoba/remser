package server

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"sync"

	"github.com/Galdoba/remser/api/models"
	"github.com/Galdoba/remser/internal/infrastructure/config"
	"github.com/Galdoba/remser/internal/infrastructure/logger"
	"github.com/Galdoba/remser/internal/server/executor"
	"github.com/Galdoba/remser/internal/server/handler"
	"github.com/Galdoba/remser/internal/server/queue"
)

type Config = models.Config

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

func NewServer(ctx context.Context, cfg config.ServerCFG) (*Server, error) {
	s := &Server{}
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	logger, err := logger.NewServerLogger()
	if err != nil {
		return nil, err
	}
	qm := queue.NewQueueManager(cfg, logger.With("component", "queue"), executor.New(logger.With("component", "executor")))
	s.log = logger
	h := handler.NewTaskHandler(cfg, qm, logger.With("component", "handler"))
	return &Server{
		cfg:      cfg,
		handler:  h,
		queueMgr: qm,
		ctx:      ctx,
		cancel:   cancel,
		log:      logger,
	}, nil
}

func (s *Server) Start() error {
	s.wg.Go(func() {
		s.queueMgr.Worker(s.ctx)
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handler.WebSocketHandler)

	s.srv = &http.Server{
		Addr:    s.cfg.ListenAddr,
		Handler: mux,
	}

	log.Printf("WebSocket server listening on %s/ws", s.cfg.ListenAddr)
	if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.cancel()
	err := s.srv.Shutdown(ctx)
	s.wg.Wait()
	return err
}
