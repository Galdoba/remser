package remserd

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Galdoba/remser/internal/infrastructure/config"
	"github.com/Galdoba/remser/internal/server"
	"github.com/urfave/cli/v3"
)

var Serve = &cli.Command{
	Name:    "serve",
	Aliases: []string{},
	Usage:   "setup and run remser server",
	Flags: []cli.Flag{
		FlagServerAddr,
	},
	Action: serveActionFunc(),
}

func serveActionFunc() cli.ActionFunc {
	return func(ctx context.Context, c *cli.Command) error {
		cfg, err := config.Server()
		if err != nil {
			return err
		}

		srv, err := server.NewServer(ctx, cfg)
		if err != nil {
			return fmt.Errorf("failed to setup server: %w", err)
		}

		go func() {
			if err := srv.Start(); err != nil {
				log.Fatalf("Server error: %v", err)
			}
		}()

		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Fatalf("Shutdown error: %v", err)
		}
		log.Println("Server stopped")
		return nil
	}
}

var FlagServerAddr = &cli.StringFlag{
	Name:    "address",
	Usage:   "set server listen addres",
	Aliases: []string{"a"},
}
