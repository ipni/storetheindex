package dtsync

import (
	// for voucher schema embed
	_ "embed"
	"errors"
	"fmt"

	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	bindnoderegistry "github.com/ipld/go-ipld-prime/node/bindnode/registry"
	"github.com/libp2p/go-libp2p/core/peer"
	cborgen "github.com/whyrusleeping/cbor-gen"
)

//go:generate go run -tags cbg ../tools

//go:embed voucher.ipldsch
var embedSchema []byte

var (
	_ datatransfer.RequestValidator = (*dagsyncValidator)(nil)

	_ cborgen.CBORMarshaler   = (*Voucher)(nil)
	_ cborgen.CBORUnmarshaler = (*Voucher)(nil)

	_ cborgen.CBORMarshaler   = (*VoucherResult)(nil)
	_ cborgen.CBORUnmarshaler = (*VoucherResult)(nil)
)

// A Voucher is used to communicate a new DAG head
type Voucher struct {
	Head *cid.Cid
}

// reads a voucher from an ipld node
func VoucherFromNode(node datamodel.Node) (*Voucher, error) {
	if node == nil {
		return nil, fmt.Errorf("empty voucher")
	}
	dpIface, err := BindnodeRegistry.TypeFromNode(node, &Voucher{})
	if err != nil {
		return nil, fmt.Errorf("invalid voucher: %w", err)
	}
	dp, _ := dpIface.(*Voucher) // safe to assume type
	return dp, nil
}

func (v *Voucher) AsVoucher() datatransfer.TypedVoucher {
	return datatransfer.TypedVoucher{
		Type:    LegsVoucherType,
		Voucher: BindnodeRegistry.TypeToNode(v),
	}
}

const LegsVoucherType = datatransfer.TypeIdentifier("LegsVoucher")

// A VoucherResult responds to a voucher
type VoucherResult struct {
	Code uint64
}

const LegsVoucherResultType = datatransfer.TypeIdentifier("LegsVoucherResult")

// reads a voucher from an ipld node
func VoucherResultFromNode(node datamodel.Node) (*VoucherResult, error) {
	if node == nil {
		return nil, fmt.Errorf("empty voucher result")
	}
	dpIface, err := BindnodeRegistry.TypeFromNode(node, &VoucherResult{})
	if err != nil {
		return nil, fmt.Errorf("invalid voucher: %w", err)
	}
	dp, _ := dpIface.(*VoucherResult) // safe to assume type
	return dp, nil
}

func (vr *VoucherResult) AsVoucher() datatransfer.TypedVoucher {
	return datatransfer.TypedVoucher{
		Type:    LegsVoucherResultType,
		Voucher: BindnodeRegistry.TypeToNode(vr),
	}
}

var BindnodeRegistry = bindnoderegistry.NewRegistry()

func init() {
	for _, r := range []struct {
		typ     interface{}
		typName string
		opts    []bindnode.Option
	}{
		{(*VoucherResult)(nil), "VoucherResult", nil},
		{(*Voucher)(nil), "Voucher", nil},
	} {
		if err := BindnodeRegistry.RegisterType(r.typ, string(embedSchema), r.typName, r.opts...); err != nil {
			panic(err.Error())
		}
	}
}

type dagsyncValidator struct {
	//ctx context.Context
	//ValidationsReceived chan receivedValidation
	allowPeer func(peer.ID) bool
}

func (vl *dagsyncValidator) ValidatePush(
	_ datatransfer.ChannelID,
	_ peer.ID,
	_ datamodel.Node,
	_ cid.Cid,
	_ ipld.Node) (datatransfer.ValidationResult, error) {

	// This is a pull-only DT voucher.
	return datatransfer.ValidationResult{}, errors.New("invalid")
}

func (vl *dagsyncValidator) validate(peerID peer.ID, voucher datamodel.Node) (datatransfer.ValidationResult, error) {

	v, err := VoucherFromNode(voucher)
	if err != nil {
		return datatransfer.ValidationResult{}, err
	}
	if v.Head == nil {
		return datatransfer.ValidationResult{}, errors.New("invalid")
	}

	if vl.allowPeer != nil && !vl.allowPeer(peerID) {
		return datatransfer.ValidationResult{}, errors.New("peer not allowed")
	}

	vr := (&VoucherResult{}).AsVoucher()
	return datatransfer.ValidationResult{
		Accepted:      true,
		VoucherResult: &vr,
	}, nil

}
func (vl *dagsyncValidator) ValidatePull(
	_ datatransfer.ChannelID,
	peerID peer.ID,
	voucher datamodel.Node,
	_ cid.Cid,
	_ ipld.Node) (datatransfer.ValidationResult, error) {
	return vl.validate(peerID, voucher)
}

func (vl *dagsyncValidator) ValidateRestart(
	_ datatransfer.ChannelID,
	state datatransfer.ChannelState) (datatransfer.ValidationResult, error) {
	return vl.validate(state.ChannelID().Initiator, state.Voucher().Voucher)
}
