package cli

import (
	"os"

	"github.com/spf13/cobra"
)

var exitCmd = &cobra.Command{
	Use: "exit",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := db.Close(); err != nil {
			return err
		}

		os.Exit(0)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(exitCmd)
}
