package libp2pclient

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/storetheindex/internal/p2putil"
	"github.com/gogo/protobuf/proto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-msgio"
)

// peerMessageSender is responsible for sending requests and messages to a particular peer
type peerMessageSender struct {
	stream  network.Stream
	r       msgio.ReadCloser
	ctxLock ctxMutex
	peerID  peer.ID

	invalid bool
}

// invalidate is called before this peerMessageSender is removed from the peerSenders.
// It prevents the peerMessageSender from being reused/reinitialized and then
// forgotten (leaving the stream open).
func (ms *peerMessageSender) invalidate() {
	ms.invalid = true
	if ms.stream != nil {
		_ = ms.stream.Reset()
		ms.stream = nil
	}
}

func (ms *peerMessageSender) prepOrInvalidate(ctx context.Context, h host.Host, protocols []protocol.ID) error {
	if err := ms.ctxLock.Lock(ctx); err != nil {
		return err
	}
	defer ms.ctxLock.Unlock()

	if err := ms.prep(ctx, h, protocols); err != nil {
		ms.invalidate()
		return err
	}
	return nil
}

func (ms *peerMessageSender) prep(ctx context.Context, h host.Host, protocols []protocol.ID) error {
	if ms.invalid {
		return fmt.Errorf("message sender has been invalidated")
	}
	if ms.stream != nil {
		return nil
	}

	// We only want to speak to peers using our primary protocols. We do not want to query any peer that only speaks
	// one of the secondary "server" protocols that we happen to support (e.g. older nodes that we can respond to for
	// backwards compatibility reasons).
	nstr, err := h.NewStream(ctx, ms.peerID, protocols...)
	if err != nil {
		return err
	}

	ms.r = msgio.NewVarintReaderSize(nstr, network.MessageSizeMax)
	ms.stream = nstr

	return nil
}

// sendMessage to a peer without waiting for any response.
func (ms *peerMessageSender) sendMessage(ctx context.Context, msg proto.Message, h host.Host, protos []protocol.ID) error {
	err := ms.ctxLock.Lock(ctx)
	if err != nil {
		return err
	}
	defer ms.ctxLock.Unlock()

	if err = ms.prep(ctx, h, protos); err != nil {
		return err
	}

	if err = p2putil.WriteMsg(ms.stream, msg); err != nil {
		_ = ms.stream.Reset()
		ms.stream = nil

		log.Debugw("error writing message", "error", err, "retrying", true)
		return err
	}

	return nil
}

// SendRequest sends a message and waits for a resonse to be sent back.
func (ms *peerMessageSender) sendRequest(ctx context.Context, msg proto.Message, decodeRsp DecodeResponseFunc, h host.Host, protos []protocol.ID) error {
	err := ms.sendMessage(ctx, msg, h, protos)
	if err != nil {
		log.Debugw("Error sending request", "erorr", err)
		return err
	}

	if err := ms.ctxReadMsg(ctx, decodeRsp); err != nil {
		_ = ms.stream.Reset()
		ms.stream = nil

		log.Debugw("error reading message", "error", err)
		return err
	}

	return nil
}

func (ms *peerMessageSender) ctxReadMsg(ctx context.Context, decodeRsp DecodeResponseFunc) error {
	done := make(chan struct{})
	var err error
	go func(r msgio.ReadCloser) {
		defer close(done)
		var data []byte
		data, err = r.ReadMsg()
		defer r.ReleaseMsg(data)
		if err != nil {
			return
		}
		err = decodeRsp(data)
	}(ms.r)

	t := time.NewTimer(readMessageTimeout)
	defer t.Stop()

	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return ErrReadTimeout
	}

	return err
}
