package config

import "time"

type ServerCFG struct {
	ListenAddr     string
	MaxConnections int64
	TaskDelay      time.Duration
}

func Server() (ServerCFG, error) {
	cfg, err := Load()
	if err != nil {
		return ServerCFG{}, err
	}
	return cfg.Server, nil
}
