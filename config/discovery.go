package config

import (
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
)

const (
	defaultLotusGateway     = "https://api.chain.love"
	defaultPollInterval     = Duration(24 * time.Hour)
	defaultRediscoverWait   = Duration(5 * time.Minute)
	defaultDiscoveryTimeout = Duration(2 * time.Minute)
)

// Discovery holds addresses of peers to from which to receive index
// advertisements, which peers to allow and block, and time related settings
// for provider discovery
type Discovery struct {
	// Bootstrap is a Set of nodes to try to connect to at startup
	Bootstrap []string
	// LotusGateway is the address for a lotus gateway used to collect chain
	// information when discovering providers
	LotusGateway string
	// Peers lists nodes to attempt to stay connected with
	Peers []peer.AddrInfo
	// Policy configures which providers are allowed and blocked
	Policy Policy
	// PollInterval is the amount of time to wait without getting any updates
	// from a provider, before sending a request for the latest advertisement.
	// Values are a number ending in "s", "m", "h" for seconds. minutes, hours.
	PollInterval Duration
	// RediscoverWait is the amount of time that must pass before a provider
	// can be discovered following a previous discovery attempt
	RediscoverWait Duration
	// Timeout is the maximum amount of time that the indexer will spend trying
	// to discover and verify a new provider.
	Timeout Duration
}
