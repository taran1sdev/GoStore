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
	Args:         cobra.ExactArgs(1),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if db != nil {
			return fmt.Errorf("Command not found")
		}

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
	rootCmd.SetHelpCommand(&cobra.Command{Hidden: true})
	rootCmd.PersistentFlags().BoolP("help", "h", false, "help message")
	rootCmd.PersistentFlags().MarkHidden("help")
	rootCmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		fmt.Println("Usage:")
		fmt.Println("	gostore <path to db>")
	})
}
