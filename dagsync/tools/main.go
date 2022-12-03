//go:build cbg
// +build cbg

package main

import (
	"os"
	"path"

	"github.com/ipni/storetheindex/dagsync/dtsync"
	cborgen "github.com/whyrusleeping/cbor-gen"
)

func main() {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	voucher_file := path.Clean(path.Join(wd, "..", "dtsync", "voucher_cbor_gen.go"))
	err = cborgen.WriteMapEncodersToFile(
		voucher_file,
		"dtsync",
		dtsync.Voucher{},
		dtsync.VoucherResult{},
	)
	if err != nil {
		panic(err)
	}
}
