package head

import (
	"strings"
	"testing"
)

func TestDeriveProtocolID(t *testing.T) {
	protoID := deriveProtocolID("/mainnet")
	if strings.Contains(string(protoID), "//") {
		t.Fatalf("Derived protocol ID %q should not contain \"//\"", protoID)
	}
}
