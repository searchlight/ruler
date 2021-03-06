package cmds

import (
	"flag"

	utilerrors "github.com/appscode/go/util/errors"
	"github.com/spf13/cobra"
)

func NewRootCmd() *cobra.Command {
	var rootCmd = &cobra.Command{
		Use:               "ruler [command]",
		Short:             `ruler for m3db`,
		DisableAutoGenTag: true,
	}

	rootCmd.PersistentFlags().AddGoFlagSet(flag.CommandLine)
	// ref: https://github.com/kubernetes/kubernetes/issues/17162#issuecomment-225596212
	utilerrors.Must(flag.CommandLine.Parse([]string{}))
	rootCmd.AddCommand(NewCmdRun())

	return rootCmd
}
