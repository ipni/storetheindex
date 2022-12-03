package dtsync

import (
	"testing"

	"github.com/ipfs/go-datastore"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/storetheindex/dagsync/test"
	"github.com/stretchr/testify/require"
)

func Test_registerVoucherHandlesAlreadyRegisteredGracefully(t *testing.T) {
	h := test.MkTestHost()

	dt, _, close, err := makeDataTransfer(h, datastore.NewMapDatastore(), cidlink.DefaultLinkSystem(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, close()) })

	v := &Voucher{}
	require.NoError(t, registerVoucher(dt, v, nil))
	require.NoError(t, registerVoucher(dt, v, nil))
}
