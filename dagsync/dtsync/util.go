package dtsync

import (
	"context"
	"fmt"
	"strings"
	"time"

	dt "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/channelmonitor"
	datatransfer "github.com/filecoin-project/go-data-transfer/impl"
	dtnetwork "github.com/filecoin-project/go-data-transfer/network"
	gstransport "github.com/filecoin-project/go-data-transfer/transport/graphsync"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-graphsync"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

type dtCloseFunc func() error

// configureDataTransferForDagsync configures an existing data transfer
// instance to serve dagsync requests from given linksystem (publisher only).
func configureDataTransferForDagsync(ctx context.Context, dtManager dt.Manager, lsys ipld.LinkSystem, allowPeer func(peer.ID) bool) error {
	v := &Voucher{}
	err := registerVoucher(dtManager, v, allowPeer)
	if err != nil {
		return err
	}
	lsc := dagsyncStorageConfiguration{lsys}
	if err = dtManager.RegisterTransportConfigurer(v, lsc.configureTransport); err != nil {
		return fmt.Errorf("failed to register datatransfer TransportConfigurer: %w", err)
	}
	return nil
}

type storeConfigurableTransport interface {
	UseStore(dt.ChannelID, ipld.LinkSystem) error
}

type dagsyncStorageConfiguration struct {
	linkSystem ipld.LinkSystem
}

func (lsc dagsyncStorageConfiguration) configureTransport(chid dt.ChannelID, voucher dt.Voucher, transport dt.Transport) {
	storeConfigurableTransport, ok := transport.(storeConfigurableTransport)
	if !ok {
		return
	}
	err := storeConfigurableTransport.UseStore(chid, lsc.linkSystem)
	if err != nil {
		log.Errorw("Failed to configure transport to use data store", "err", err)
	}
}

func registerVoucher(dtManager dt.Manager, v *Voucher, allowPeer func(peer.ID) bool) error {
	val := &dagsyncValidator{
		allowPeer: allowPeer,
	}
	err := dtManager.RegisterVoucherType(v, val)
	if err != nil {
		// This can happen if a host is both a publisher and a subscriber.
		if strings.Contains(err.Error(), "identifier already registered: "+string(v.Type())) {
			// Matching the error string is the best we can do until datatransfer exposes some handles
			// to either check for types or re-register vouchers.
			log.Warn("voucher type already registered; skipping datatrasfer voucher registration", "type", v.Type())
			return nil
		}
		return fmt.Errorf("failed to register dagsync validator voucher type: %w", err)
	}
	lvr := &VoucherResult{}
	if err = dtManager.RegisterVoucherResultType(lvr); err != nil {
		return fmt.Errorf("failed to register dagsync voucher result type: %w", err)
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

	err = registerVoucher(dtManager, &Voucher{}, allowPeer)
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

	// Wait for datatransfer to be ready.
	err = <-dtReady
	if err != nil {
		cancel()
		return nil, nil, nil, err
	}

	closeFunc := func() error {
		var err, errs error
		err = dtManager.Stop(context.Background())
		if err != nil {
			log.Errorw("Failed to stop datatransfer manager", "err", err)
			errs = multierror.Append(errs, err)
		}
		cancel()
		return errs
	}

	return dtManager, gs, closeFunc, nil
}
