package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ipni/storetheindex/assigner/command"
	"github.com/ipni/storetheindex/internal/revision"
	"github.com/urfave/cli/v2"
)

func main() {
	// Set up a context that is canceled when the command is interrupted.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up a signal handler to cancel the context
	go func() {
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, syscall.SIGTERM, syscall.SIGINT)
		select {
		case <-interrupt:
			cancel()
			fmt.Println("Received interrupt signal, shutting down...")
			fmt.Println("(Hit ctrl-c again to force-shutdown the daemon.)")
		case <-ctx.Done():
		}
		// Allow any further SIGTERM or SIGINT to kill process.
		signal.Stop(interrupt)
	}()

	app := &cli.App{
		Name:        "assigner",
		Usage:       "IPNI indexer assigner service",
		Description: "The assigner service is responsible for assigning content advertisement publishers to indexers.",
		Version:     revision.Revision,
		Commands: []*cli.Command{
			command.DaemonCmd,
			command.InitCmd,
		},
	}

	if err := app.RunContext(ctx, os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
