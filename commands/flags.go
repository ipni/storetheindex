package commands

import (
	_ "github.com/lib/pq"
	"github.com/urfave/cli/v2"
	"github.com/urfave/cli/v2/altsrc"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("dealbot")

var MockFlags []cli.Flag = []cli.Flag{
	altsrc.NewStringFlag(&cli.StringFlag{
		Name:    "mock",
		Usage:   "mocking flag to test flag operation",
		Aliases: []string{"mck"},
	}),
}

var DaemonFlags = []cli.Flag{
	&cli.BoolFlag{
		Name:    "persistence",
		Usage:   "Enable persistence storage",
		Aliases: []string{"p"},
		EnvVars: []string{"ENABLE_PERSISTENCE"},
		Value:   true,
	},
}

var ImportFlags = []cli.Flag{
	&cli.BoolFlag{
		Name:    "",
		Usage:   "Enable persistence storage",
		Aliases: []string{"p"},
		EnvVars: []string{"ENABLE_PERSISTENCE"},
		Value:   true,
	},
}
