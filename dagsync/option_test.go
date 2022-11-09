package dagsync

import (
	"reflect"
	"testing"

	"github.com/ipld/go-ipld-prime/traversal/selector"
)

func TestSyncRecursionLimit_DefaultsToNone(t *testing.T) {
	cfg := config{}
	if !reflect.DeepEqual(selector.RecursionLimitNone(), cfg.syncRecLimit) {
		t.Fail()
	}
}
