package command

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/filecoin-project/storetheindex/internal/node"
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

func daemonCommand(cctx *cli.Context) error {
	// TODO: get these from config file
	cacheSize := int(cctx.Int64("cachesize"))
	dir := cctx.String("dir")
	storeType := cctx.String("storage")
	finderAddr := cctx.String("finder_ep")
	adminAddr := cctx.String("admin_ep")
	p2pEnabled := cctx.Bool("enablep2p")

	log.Infow("Starting node deamon")
	n, err := node.New(cacheSize, dir, storeType, finderAddr, adminAddr, p2pEnabled)
	if err != nil {
		return err
	}

	n.Start()

	// Wait for SIGINT (CTRL-c), then close server and exit.
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt)
	<-sigint

	log.Infow("shutting down node")

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	go func() {
		select {
		case <-ctx.Done():
			fmt.Println("Timed out on shutdown, terminating...")
		case <-sigint:
			fmt.Println("Received another interrupt before graceful shutdown, terminating...")
		}
		os.Exit(-1)
	}()

	if err := n.Shutdown(ctx); err != nil {
		log.Fatalw("failed to shut down rpc server", "err", err)
		return err
	}
	log.Infow("node stopped")
	return nil
}
