package main

import (
	"context"
	"fmt"
	"os"

	"github.com/Galdoba/remser/internal/presentation/cli/commands"
)

func main() {
	cmd := commands.RemserC()

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "remserc: failed to run: %v", err)
		os.Exit(1)
	}
	os.Exit(0)
}
