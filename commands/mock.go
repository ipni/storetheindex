package commands

import (
	"context"
	"time"

	"github.com/urfave/cli/v2"
)

var MockCmd = &cli.Command{
	Name:  "mock",
	Usage: "Mock command for test purposes",
	// Flags:  append(EndpointFlags, MockFlags...),
	Action: mockCommand,
}

func mockCommand(c *cli.Context) error {
	ctx, cancel := context.WithCancel(ProcessContext())
	defer cancel()

	exiting := make(chan struct{})
	defer close(exiting)

	go func() {
		select {
		case <-ctx.Done():
		case <-exiting:
			// no need to shutdown in this case.
			return
		}

		log.Infow("shutting down node")

		_, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		log.Infow("node stopped")
	}()

	log.Debugw("Mock command success")
	return nil
}
