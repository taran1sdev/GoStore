package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

var deleteCmd = &cobra.Command{
	Use:   "delete <dbname>",
	Args:  cobra.ExactArgs(1),
	Short: "Delete an existing database",
	RunE: func(cmd *cobra.Command, args []string) error {
		dbname := args[0]

		dbDir := filepath.Join(cfg.DataDir, dbname)

		if err := os.RemoveAll(dbDir); err != nil {
			return err
		}

		logFile := filepath.Join(cfg.LogDir, dbname+".log")
		_ = os.Remove(logFile)

		fmt.Printf("Database %s deleted\n", dbname)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(deleteCmd)
}
