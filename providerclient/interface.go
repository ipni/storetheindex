package client

import (
	"context"

	"github.com/ipfs/go-cid"
)

// Provider client interface
type Provider interface {
	// GetAdv gets an advertisement by CID from a provider
	GetAdv(ctx context.Context, id cid.Cid) (*AdResponse, error)

	// GetLatestAdv gets the latest advertisement from a provider
	GetLatestAdv(ctx context.Context) (*AdResponse, error)

	// Free any resources used by the client
	Close() error
}
