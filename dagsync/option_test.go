package dagsync

import (
	"reflect"
	"testing"

	"github.com/ipld/go-ipld-prime/traversal/selector"
)

func TestSyncRecursionLimit_DefaultsToNone(t *testing.T) {
	opts, err := getOpts(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(selector.RecursionLimitNone(), opts.syncRecLimit) {
		t.Fail()
	}
}
