package commands

import (
	"context"
	"net/http"
	"time"

	"github.com/filecoin-project/storetheindex/node"
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

	log.Infow("Starting node deamon")
	exiting := make(chan struct{})
	n, err := node.New(ctx, c)
	if err != nil {
		return err
	}
	defer close(exiting)

	go func() {
		select {
		case <-ctx.Done():
		case <-exiting:
			// no need to shutdown in this case.
			return
		}

		log.Infow("shutting down node")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := n.Shutdown(ctx); err != nil {
			log.Fatalw("failed to shut down rpc server", "err", err)
		}
		log.Infow("node stopped")
	}()

	err = n.Start()
	if err == http.ErrServerClosed {
		err = nil
	}
	return err

}
