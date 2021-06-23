package commands

import (
	"context"
	"time"

	"github.com/adlrocha/indexer-node/node"
	"github.com/urfave/cli/v2"
)

var DaemonCmd = &cli.Command{
	Name:   "daemon",
	Usage:  "Start an indexer daemon, accepting http requests",
	Flags:  DaemonFlags,
	Action: daemonCommand,
}

func daemonCommand(c *cli.Context) error {
	ctx, cancel := context.WithCancel(ProcessContext())
	defer cancel()

	log.Debugw("Starting node as a deamon")
	exiting := make(chan struct{})
	_, err := node.New(ctx, c)
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

	return err

}
