package command

import (
	"github.com/urfave/cli/v2"
)

var initFlags = []cli.Flag{
	&cli.BoolFlag{
		Name:     "no-bootstrap",
		Usage:    "Do not configure bootstrap peers",
		EnvVars:  []string{"NO_BOOTSTRAP"},
		Required: false,
	},
	&cli.StringFlag{
		Name:     "pubsub-topic",
		Usage:    "Subscribe to this pubsub topic to receive advertisement notification",
		EnvVars:  []string{"ASSIGNER_PUBSUB_TOPIC"},
		Required: false,
	},
	&cli.BoolFlag{
		Name:     "upgrade",
		Usage:    "Upgrade the config file to the current version, saving the old config as config.prev, and ignoring other flags ",
		Aliases:  []string{"u"},
		Required: false,
	},
}

var daemonFlags = []cli.Flag{
	&cli.StringFlag{
		Name:     "listen-admin",
		Usage:    "Admin HTTP API listen address",
		EnvVars:  []string{"ASSIGNER_LISTEN_ADMIN"},
		Required: false,
	},
	&cli.StringFlag{
		Name:     "listen-http",
		Usage:    "HTTP listen address",
		EnvVars:  []string{"ASSIGNER_LISTEN_HTTP"},
		Required: false,
	},
	&cli.StringFlag{
		Name:     "listen-p2p",
		Usage:    "P2P listen address",
		EnvVars:  []string{"ASSIGNER_LISTEN_P2P"},
		Required: false,
	},
	&cli.BoolFlag{
		Name:     "watch-config",
		Usage:    "Watch for changes to config file and automatically reload",
		EnvVars:  []string{"ASSIGNER_WATCH_CONFIG"},
		Value:    true,
		Required: false,
	},
}
