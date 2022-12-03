package config

import (
	"io"

	sticfg "github.com/ipni/storetheindex/config"
)

func Init(out io.Writer) (*Config, error) {
	identity, err := sticfg.CreateIdentity(out)
	if err != nil {
		return nil, err
	}

	return InitWithIdentity(identity)
}

func InitWithIdentity(identity sticfg.Identity) (*Config, error) {
	conf := &Config{
		Version:    Version,
		Assignment: NewAssignment(),
		Bootstrap:  sticfg.NewBootstrap(),
		Daemon:     NewDaemon(),
		Identity:   identity,
		Logging:    NewLogging(),
	}

	return conf, nil
}
