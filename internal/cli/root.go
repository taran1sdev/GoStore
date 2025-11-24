package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.store/internal/engine"
)

var dbPath string
var db *engine.Database

var rootCmd = &cobra.Command{
	Use:   "gostore <path>",
	Short: "GoStore - Simple Key Value Store",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		dbPath = args[0]

		var err error
		db, err = engine.Open(dbPath)
		if err != nil {
			return fmt.Errorf("Failed to open Database: %s", err)
		}

		startREPL(cmd)
		return nil
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println("Error: ", err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(setCmd)
	rootCmd.AddCommand(getCmd)
	rootCmd.AddCommand(exitCmd)
}
