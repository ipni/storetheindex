package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/ipni/storetheindex/internal/fsutil"
)

// Import configures the import sender: an HTTP server that serves this
// indexer's datastore to an indexer that imports it. The sender is off
// unless ListenAddr is set. None of these values are reloadable.
type Import struct {
	// ListenAddr is the sender's listen address, a multiaddr such as
	// "/ip4/0.0.0.0/tcp/3006" or a "host:port". Empty or "none" leaves the
	// sender off.
	ListenAddr string
	// TokenFile is the path of a file that holds the token which every
	// request must carry as "Authorization: Bearer <token>". It is required
	// when the sender is on. Whitespace around the token is ignored.
	TokenFile string
	// PartBytes is about how many bytes of keys and values each part that
	// the sender opens holds. 0 uses the default of 128 MiB.
	PartBytes uint64
}

// Enabled reports whether the sender is on
func (c Import) Enabled() bool {
	return c.ListenAddr != "" && c.ListenAddr != "none"
}

// ReadToken returns the token that TokenFile holds
func (c Import) ReadToken() (string, error) {
	if c.TokenFile == "" {
		return "", errors.New("the import sender needs Import.TokenFile")
	}
	path, err := fsutil.ExpandHome(c.TokenFile)
	if err != nil {
		return "", err
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("cannot read the import token: %w", err)
	}
	token := strings.TrimSpace(string(b))
	if token == "" {
		return "", fmt.Errorf("the import token file %s is empty", path)
	}
	return token, nil
}
