package config

import (
	"github.com/Galdoba/appcontext/configmanager"
	"github.com/Galdoba/remser/internal/infrastructure"
)

type Config struct {
	Client ClientCFG
	Server ServerCFG
}

func Load() (Config, error) {
	return configmanager.LazyInit(infrastructure.AppName, Config{
		Client: ClientCFG{
			Address: "",
		},
		Server: ServerCFG{
			ListenAddr:     ":8080",
			TaskDelay:      1,
			MaxConnections: 100,
		},
	})
}
