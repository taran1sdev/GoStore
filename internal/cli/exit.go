package cli

import (
	"os"

	"github.com/spf13/cobra"
)

var exitCmd = &cobra.Command{
	Use:   "exit",
	Short: "Exit the application",
	Run: func(cmd *cobra.Command, args []string) {
		os.Exit(0)
	},
}
