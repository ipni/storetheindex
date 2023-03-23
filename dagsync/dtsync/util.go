package dtsync

import (
	"context"
	"fmt"
	"strings"
	"time"

	dt "github.com/filecoin-project/go-data-transfer/v2"
	"github.com/filecoin-project/go-data-transfer/v2/channelmonitor"
	datatransfer "github.com/filecoin-project/go-data-transfer/v2/impl"
	dtnetwork "github.com/filecoin-project/go-data-transfer/v2/network"
	gstransport "github.com/filecoin-project/go-data-transfer/v2/transport/graphsync"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-graphsync"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Time to wait for datatransfer to gracefully stop before canceling.
const datatransferStopTimeout = time.Minute

type dtCloseFunc func() error

// configureDataTransferForDagsync configures an existing data transfer
// instance to serve dagsync requests from given linksystem (publisher only).
func configureDataTransferForDagsync(ctx context.Context, dtManager dt.Manager, lsys ipld.LinkSystem, allowPeer func(peer.ID) bool) error {
	err := registerVoucher(dtManager, allowPeer)
	if err != nil {
		return err
	}
	lsc := dagsyncStorageConfiguration{lsys}
	if err = dtManager.RegisterTransportConfigurer(LegsVoucherType, lsc.configureTransport); err != nil {
		return fmt.Errorf("failed to register datatransfer TransportConfigurer: %w", err)
	}
	return nil
}

type dagsyncStorageConfiguration struct {
	linkSystem ipld.LinkSystem
}

func (lsc dagsyncStorageConfiguration) configureTransport(_ dt.ChannelID, _ dt.TypedVoucher) []dt.TransportOption {
	return []dt.TransportOption{gstransport.UseStore(lsc.linkSystem)}
}

func registerVoucher(dtManager dt.Manager, allowPeer func(peer.ID) bool) error {
	val := &dagsyncValidator{
		allowPeer: allowPeer,
	}
	err := dtManager.RegisterVoucherType(LegsVoucherType, val)
	if err != nil {
		// This can happen if a host is both a publisher and a subscriber.
		if strings.Contains(err.Error(), "identifier already registered: "+string(LegsVoucherType)) {
			// Matching the error string is the best we can do until datatransfer exposes some handles
			// to either check for types or re-register vouchers.
			log.Warn("voucher type already registered; skipping datatrasfer voucher registration", "type", LegsVoucherType)
			return nil
		}
		return fmt.Errorf("failed to register dagsync validator voucher type: %w", err)
	}
	return nil
}

func makeDataTransfer(host host.Host, ds datastore.Batching, lsys ipld.LinkSystem, allowPeer func(peer.ID) bool) (dt.Manager, graphsync.GraphExchange, dtCloseFunc, error) {
	gsNet := gsnet.NewFromLibp2pHost(host)
	ctx, cancel := context.WithCancel(context.Background())
	gs := gsimpl.New(ctx, gsNet, lsys)

	dtNet := dtnetwork.NewFromLibp2pHost(host)
	tp := gstransport.NewTransport(host.ID(), gs)

	dtRestartConfig := datatransfer.ChannelRestartConfig(channelmonitor.Config{
		AcceptTimeout:   time.Minute,
		CompleteTimeout: time.Minute,

		// When an error occurs, wait a little while until all related errors
		// have fired before sending a restart message
		RestartDebounce: 10 * time.Second,
		// After sending a restart, wait for at least 1 minute before sending another
		RestartBackoff: time.Minute,
		// After trying to restart 3 times, give up and fail the transfer
		MaxConsecutiveRestarts: 3,
	})

	dtManager, err := datatransfer.NewDataTransfer(ds, dtNet, tp, dtRestartConfig)
	if err != nil {
		cancel()
		return nil, nil, nil, fmt.Errorf("failed to instantiate datatransfer: %w", err)
	}

	err = registerVoucher(dtManager, allowPeer)
	if err != nil {
		cancel()
		return nil, nil, nil, fmt.Errorf("failed to register voucher: %w", err)
	}

	// Tell datatransfer to notify when ready.
	dtReady := make(chan error)
	dtManager.OnReady(func(e error) {
		dtReady <- e
	})

	// Start datatransfer.  The context passed in allows Start to be canceled
	// if fsm migration takes too long.  Timeout for dtManager.Start() is not
	// handled here, so pass context.Background().
	if err = dtManager.Start(ctx); err != nil {
		cancel()
		return nil, nil, nil, fmt.Errorf("failed to start datatransfer: %w", err)
	}
	log.Info("Started data transfer manager successfully.")

	// Wait for datatransfer to be ready.
	log.Info("Awaiting data transfer manager to become ready...")
	err = <-dtReady
	if err != nil {
		log.Errorw("Failed while waiting for data transfer manager to become ready", "err", err)
		cancel()
		return nil, nil, nil, err
	}
	log.Info("Data transfer manager is ready.")

	closeFunc := func() error {
		errCh := make(chan error)
		stopCtx, stopCancel := context.WithTimeout(context.Background(), datatransferStopTimeout)
		go func() {
			errCh <- dtManager.Stop(stopCtx)
		}()
		var err error
		select {
		case err = <-errCh:
		case <-stopCtx.Done():
			err = stopCtx.Err()
		}
		stopCancel()
		cancel()
		if err != nil {
			err = fmt.Errorf("failed to stop datatransfer manager: %w", err)
		}
		return err
	}

	return dtManager, gs, closeFunc, nil
}
