package config

// ReverseIndexer stores the configuration for the reverse indexer.
type ReverseIndexer struct {
	// Enabled is true if the reverse indexer is enabled.
	Enabled bool
	// StorePath is the path to the reverse index store.
	StorePath string
}

// NewReverseIndexer ReverseIndexer returns ReverseIndexer with values set to their defaults.
func NewReverseIndexer() ReverseIndexer {
	return ReverseIndexer{
		Enabled:   false,
		StorePath: "reverse_index",
	}
}

func (rx *ReverseIndexer) populateUnset() {
	def := NewReverseIndexer()

	if rx.StorePath == "" {
		rx.StorePath = def.StorePath
	}
}
