package command

import (
	"errors"
	"fmt"

	"github.com/ipni/storetheindex/admin/client"
	"github.com/urfave/cli/v2"
)

var subsystemsCmd = &cli.Command{
	Name:   "subsystems",
	Usage:  "Lists the available logging subsystems",
	Action: listLogSubsystemsAction,
}

var setLevelCmd = &cli.Command{
	Name:  "setlevel",
	Usage: "Sets log level for logging subsystems",
	Description: `Configures log level for logging subystems specified as pairs of arguments.
Subsystem may be specified as a regular expression. For example the 
following command will set the level for all subsystems to debug:

	storetheindex config set log-level '.*' debug`,
	ArgsUsage: "<sub-system> <level> [<sub-system> <level>]...",
	Action:    setLogLevelAction,
}

var LogCmd = &cli.Command{
	Name:  "log",
	Usage: "Show log subsystems and modify log levels",
	Flags: []cli.Flag{indexerHostFlag},
	Subcommands: []*cli.Command{
		subsystemsCmd,
		setLevelCmd,
	},
}

func setLogLevelAction(cctx *cli.Context) error {
	cl, err := client.New(cliIndexer(cctx, "admin"))
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

func listLogSubsystemsAction(cctx *cli.Context) error {
	cl, err := client.New(cliIndexer(cctx, "admin"))
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
