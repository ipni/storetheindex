package config

// Discoveryu stores the pubsub router address to subscribe to, and a list of
// allowed discovery clients
type Discovery struct {
	PubsubRouter      string
	AllowClients      []string
	TrustLocalClients bool
}
