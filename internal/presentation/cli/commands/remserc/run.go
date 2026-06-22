package remserc

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/Galdoba/remser/internal/client"
	"github.com/Galdoba/remser/internal/infrastructure/config"
	"github.com/Galdoba/remser/pkg/text"
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
			os.Exit(1)
		}

		cl := &client.Client{
			ServerAddr:  text.FirstNonEmpty(c.String(flagClientAddres), cfg.Addres),
			Interactive: (c.Bool(flagClientInteractive) || cfg.Interactive),
			ClientID:    text.FirstNonEmpty(c.String(flagClientID), cfg.ClientID),
		}

		if cl.Interactive {
			id := cl.ClientID
			if id == "" {
				id = fmt.Sprintf("interactive-%d-%d", os.Getpid(), time.Now().UnixNano())
			}
			cl.ClientID = id

			oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to set raw mode: %v\n", err)
				os.Exit(1)
			}
			defer term.Restore(int(os.Stdin.Fd()), oldState)
		}

		if err := cl.Execute(args); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		return nil
	}
}

const (
	flagClientAddres      = "address"
	flagClientInteractive = "interactive"
	flagClientID          = "id"
)

var FlagClientAddr = &cli.StringFlag{
	Name:    flagClientAddres,
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
