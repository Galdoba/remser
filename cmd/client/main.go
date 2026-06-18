package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"golang.org/x/term"

	"github.com/Galdoba/remser/internal/client"
)

func main() {
	address := flag.String("address", "http://localhost:8080", "Server address")
	interactive := flag.Bool("interactive", false, "Enable interactive mode (captures stdin)")
	clientID := flag.String("client-id", "", "Client identifier")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "Usage: client -address <url> [-interactive] [-client-id <id>] [command args...]")
		os.Exit(1)
	}

	c := &client.Client{
		ServerAddr: *address,
	}

	if *interactive {
		id := *clientID
		if id == "" {
			id = fmt.Sprintf("interactive-%d-%d", os.Getpid(), time.Now().UnixNano())
		}
		c.ClientID = id
		c.Interactive = true

		oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to set raw mode: %v\n", err)
			os.Exit(1)
		}
		defer term.Restore(int(os.Stdin.Fd()), oldState)
	}

	// Запуск через WebSocket (внутри будет обработан и интерактивный ввод).
	if err := c.Execute(args); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
