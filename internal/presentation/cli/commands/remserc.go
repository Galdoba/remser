package commands

import (
	"github.com/Galdoba/remser/internal/infrastructure"
	"github.com/Galdoba/remser/internal/presentation/cli/commands/remserc"
	"github.com/Galdoba/remser/pkg/text"
	"github.com/urfave/cli/v3"
)

func RemserC() *cli.Command {
	return &cli.Command{
		Commands: []*cli.Command{
			remserc.Run,
		},
		Name:                   "",
		Aliases:                []string{},
		Usage:                  "client for remote commands queue",
		UsageText:              "remserc {command} [command options...] [args...]",
		Version:                infrastructure.Version,
		Description:            text.Split(description, 80),
		Authors:                []any{"galdoba"},
		UseShortOptionHandling: true,
	}
}
