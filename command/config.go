package command

import (
	"errors"
	"fmt"

	httpclient "github.com/filecoin-project/storetheindex/api/v0/admin/client/http"
	"github.com/urfave/cli/v2"
)

var ConfigCmd = &cli.Command{
	Name:  "config",
	Usage: "Dynamically modifies and shows the current logging configuration",
	Flags: []cli.Flag{indexerHostFlag},
	Subcommands: []*cli.Command{
		{
			Name:  "set",
			Usage: "Sets a configuration parameter",
			Subcommands: []*cli.Command{
				{
					Name:    "log.level",
					Aliases: []string{"ll"},
					Usage:   "Sets log level for logging subsystems",
					Description: `Configures log level for logging subystems specified as pairs of arguments.
Subsystem may be specified as a regular expression. For example the 
following command will set the level for all subsystems to debug:

	storetheindex config set log-level '.*' debug`,
					ArgsUsage: "<sub-system> <level> [<sub-system> <level>]...",
					Action:    setLogLevels,
				},
			},
		},
		{
			Name:  "get",
			Usage: "Shows a configuration parameter",
			Subcommands: []*cli.Command{
				{
					Name:    "log.subsystems",
					Aliases: []string{"lss"},
					Usage:   "Lists the available logging subsystems",
					Action:  listLogSubSystems,
				},
			},
		},
	},
}

func setLogLevels(cctx *cli.Context) error {
	cl, err := httpclient.New(cctx.String("indexer"))
	if err != nil {
		return err
	}

	args := cctx.Args()
	if !args.Present() {
		return errors.New("at least one <sub-system> <level> argument pair must be specified")
	}

	if args.Len()%2 != 0 {
		return fmt.Errorf("<sub-system> <level> argument must be specified as pairs")
	}

	sysLvl := make(map[string]string, args.Len()/2)
	for i := 0; i < args.Len(); i += 2 {
		subsys := args.Get(i)
		level := args.Get(i + 1)
		sysLvl[subsys] = level
	}
	return cl.SetLogLevels(cctx.Context, sysLvl)
}

func listLogSubSystems(cctx *cli.Context) error {
	cl, err := httpclient.New(cctx.String("indexer"))
	if err != nil {
		return err
	}
	subSystems, err := cl.ListLogSubSystems(cctx.Context)
	if err != nil {
		return err
	}

	for _, ss := range subSystems {
		_, _ = fmt.Fprintln(cctx.App.Writer, ss)
	}
	return nil
}
