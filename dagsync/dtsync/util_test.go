package dtsync

import (
	"testing"

	"github.com/ipfs/go-datastore"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p"
	"github.com/stretchr/testify/require"
)

func Test_registerVoucherHandlesAlreadyRegisteredGracefully(t *testing.T) {
	h, err := libp2p.New()
	require.NoError(t, err)

	dt, _, close, err := makeDataTransfer(h, datastore.NewMapDatastore(), cidlink.DefaultLinkSystem(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, close()) })

	v := &Voucher{}
	require.NoError(t, registerVoucher(dt, v, nil))
	require.NoError(t, registerVoucher(dt, v, nil))
}
