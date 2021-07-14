package commands

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/filecoin-project/storetheindex/node"
	"github.com/urfave/cli/v2"
)

// shutdownTimeout is the duration that a graceful shutdown has to complete
const shutdownTimeout = 5 * time.Second

var DaemonCmd = &cli.Command{
	Name:   "daemon",
	Usage:  "Start an indexer daemon, accepting http requests",
	Flags:  DaemonFlags,
	Action: daemonCommand,
}

func daemonCommand(c *cli.Context) error {
	log.Infow("Starting node deamon")
	n, err := node.New(c)
	if err != nil {
		return err
	}

	go func() {
		// Wait for SIGINT (CTRL-c), then close server and exit.
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt)
		<-sigint

		log.Infow("shutting down node")

		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
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
