package commands

import (
	"github.com/Galdoba/remser/internal/infrastructure"
	"github.com/Galdoba/remser/internal/presentation/cli/commands/remserd"
	"github.com/Galdoba/remser/pkg/text"
	"github.com/urfave/cli/v3"
)

const (
	description = "Server (remserd) creates a queue of commands with one or several workers. When connection if accepted from client (remserc) task is placed in this queue. Task is handled on server side, with stdout and stderr relayed to client. When client's task is complete, connection is terminated."
)

func RemserD() *cli.Command {
	return &cli.Command{
		Commands: []*cli.Command{
			remserd.Serve,
		},
		Name:                   "",
		Aliases:                []string{},
		Usage:                  "server for remote commands queue",
		UsageText:              "remserd {command} [command options...]",
		Version:                infrastructure.Version,
		Description:            text.Split(description, 80),
		Authors:                []any{"galdoba"},
		UseShortOptionHandling: true,
	}
}
