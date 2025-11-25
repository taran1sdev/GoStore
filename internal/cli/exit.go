package cli

import (
	"os"

	"github.com/spf13/cobra"
)

var exitCmd = &cobra.Command{
	Run: func(cmd *cobra.Command, args []string) {
		os.Exit(0)
	},
}

func init() {
	rootCmd.AddCommand(exitCmd)
}
