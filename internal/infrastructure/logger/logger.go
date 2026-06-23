package logger

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/Galdoba/appcontext/xdg"
	"github.com/Galdoba/remser/internal/infrastructure"
)

func NewClientLogger() (*slog.Logger, error) {
	path := xdg.Location(xdg.ForCache(), xdg.WithProgramName(infrastructure.AppName), xdg.WithFileName(infrastructure.ClientAppName+".log"))
	f := setupWriter(path)
	if f == nil {
		return nil, fmt.Errorf("failed to create writer for logger")
	}
	defer f.Close()

	logger := slog.New(slog.NewJSONHandler(f, &slog.HandlerOptions{
		AddSource: false,
		Level:     slog.LevelDebug,
	}))
	return logger, nil
}

func NewServerLogger() (*slog.Logger, error) {
	path := xdg.Location(xdg.ForCache(), xdg.WithProgramName(infrastructure.AppName), xdg.WithFileName(infrastructure.ServerAppName+".log"))
	f := setupWriter(path)
	if f == nil {
		return nil, fmt.Errorf("failed to create writer for logger")
	}
	defer f.Close()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		AddSource: false,
		Level:     slog.LevelInfo,
	}))
	return logger, nil
}

func setupWriter(path string) *os.File {
	if err := os.MkdirAll(filepath.Dir(path), 0777); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create log directory: %v\n", err)
		return nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open log file: %v", err)
		return nil
	}
	return f
}
