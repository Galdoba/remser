package server

import (
	"context"
	"log"
	"net/http"
	"sync"

	"github.com/Galdoba/remser/api/models"
	"github.com/Galdoba/remser/internal/server/handler"
)

type Config = models.Config

type Server struct {
	cfg     models.Config
	srv     *http.Server
	handler *handler.TaskHandler
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

func NewServer(cfg Config) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	h := handler.NewTaskHandler(cfg, ctx)
	return &Server{
		cfg:     cfg,
		handler: h,
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (s *Server) Start() error {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.handler.Worker(s.ctx)
	}()

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