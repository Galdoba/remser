package remserc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/Galdoba/remser/internal/client"
	"github.com/Galdoba/remser/internal/infrastructure/config"
	"github.com/Galdoba/remser/pkg/text"
	"github.com/google/uuid"
	"github.com/urfave/cli/v3"
	"golang.org/x/term"
)

var Run = &cli.Command{
	Name:    "run",
	Aliases: []string{},
	Usage:   "connect to remsemd and run cli command",
	Flags: []cli.Flag{
		FlagClientAddr,
		FlagClientInteractive,
		FlagClientID,
		FlagClientAddr,
		FlagClientInteractive,
		FlagClientID,
		FlagSSHHost,
		FlagSSHPort,
		FlagSSHUser,
		FlagSSHPassword,
		FlagSSHKeyFile,
		FlagSSHInsecure,
		FlagSSHRemoteAddr,
	},
	Action:    runActionFunc(),
	Copyright: "",
}

func runActionFunc() cli.ActionFunc {
	return func(ctx context.Context, c *cli.Command) error {
		cfg, err := config.Client()
		if err != nil {
			return err
		}

		args := c.Args().Slice()
		if len(args) == 0 {
			fmt.Fprintln(os.Stderr, "Usage: client -address <url> [-interactive] [-client-id <id>] [command args...]")
			return fmt.Errorf("no command specified")
		}

		addr := text.FirstNonEmpty(c.String(flagClientAddress), cfg.Address)
		interactive := c.Bool(flagClientInteractive) || cfg.Interactive
		clientID := text.FirstNonEmpty(c.String(flagClientID), cfg.ClientID)

		// Если ID не задан — генерируем уникальный
		if clientID == "" {
			clientID = "remser-" + uuid.New().String()[:8]
		}

		if interactive && clientID == "" {
			clientID = fmt.Sprintf("interactive-%d-%d", os.Getpid(), time.Now().UnixNano())
		}
		sshCfg := config.SSH{
			User:                  text.FirstNonEmpty(c.String("ssh-user"), cfg.Ssh.User),
			Host:                  text.FirstNonEmpty(c.String("ssh-host"), cfg.Ssh.Host),
			Port:                  text.FirstNonEmpty(c.String("ssh-port"), cfg.Ssh.Port),
			Password:              text.FirstNonEmpty(c.String("ssh-password"), cfg.Ssh.Password),
			KeyFile:               text.FirstNonEmpty(c.String("ssh-key"), cfg.Ssh.KeyFile),
			InsecureIgnoreHostKey: c.Bool("ssh-insecure") || cfg.Ssh.InsecureIgnoreHostKey,
			RemoteAddress:         text.FirstNonEmpty(c.String("ssh-remote"), cfg.Ssh.RemoteAddress),
		}

		// Сборка опций клиента
		opts := []client.Option{
			client.WithClientID(clientID),
			client.WithInteractive(interactive),
		}
		// При необходимости сюда можно добавить таймауты и другие параметры из конфига
		// opts = append(opts, client.WithPingInterval(cfg.PingInterval))

		// Если SSH хост задан, активируем туннель
		if sshCfg.Host != "" {
			opts = append(opts, client.WithSSHTunnel(sshCfg))
		}

		cl, err := client.NewClient(addr, opts...)
		if err != nil {
			return fmt.Errorf("create client: %w", err)
		}

		// Установка raw-режима терминала для интерактивных сессий
		if interactive {
			fd := int(os.Stdin.Fd())
			oldState, err := term.MakeRaw(fd)
			if err != nil {
				return fmt.Errorf("set raw mode: %w", err)
			}
			defer term.Restore(fd, oldState)
		}

		if err := cl.Execute(ctx, args); err != nil {
			if errors.Is(err, client.ErrTaskFinished) {
				return nil // штатное завершение
			}
			return err
		}
		return nil
	}
}

const (
	flagClientAddress     = "address"
	flagClientInteractive = "interactive"
	flagClientID          = "id"
)

var FlagClientAddr = &cli.StringFlag{
	Name:    flagClientAddress,
	Usage:   "set address for client co connect to",
	Aliases: []string{"a"},
}

var FlagClientInteractive = &cli.BoolFlag{
	Name:    flagClientInteractive,
	Usage:   "allow user input",
	Aliases: []string{"i"},
}

var FlagClientID = &cli.StringFlag{
	Name:  flagClientID,
	Usage: "set client id",
}

var FlagSSHHost = &cli.StringFlag{
	Name:    "ssh-host",
	Usage:   "SSH server host",
	Aliases: []string{"H"},
}

var FlagSSHPort = &cli.StringFlag{
	Name:    "ssh-port",
	Usage:   "SSH server port (default 22)",
	Aliases: []string{"P"},
}

var FlagSSHUser = &cli.StringFlag{
	Name:    "ssh-user",
	Usage:   "SSH username",
	Aliases: []string{"u"},
}

var FlagSSHPassword = &cli.StringFlag{
	Name:  "ssh-password",
	Usage: "SSH password",
}

var FlagSSHKeyFile = &cli.StringFlag{
	Name:    "ssh-key",
	Usage:   "path to SSH private key file",
	Aliases: []string{"k"},
}

var FlagSSHInsecure = &cli.BoolFlag{
	Name:  "ssh-insecure",
	Usage: "skip host key verification (insecure)",
}

var FlagSSHRemoteAddr = &cli.StringFlag{
	Name:  "ssh-remote",
	Usage: "WebSocket server address from SSH's perspective (host:port)",
}
