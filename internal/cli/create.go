package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"go.store/internal/storage"
)

var createCmd = &cobra.Command{
	Use:   "create <dbname>",
	Args:  cobra.ExactArgs(1),
	Short: "Create a new database",
	RunE: func(cmd *cobra.Command, args []string) error {
		dbname := args[0]

		dbDir := filepath.Join(cfg.DataDir, dbname)
		if err := os.MkdirAll(dbDir, 0o755); err != nil {
			return err
		}

		dbPath := filepath.Join(dbDir, dbname+".db")

		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			f, cErr := storage.CreateDatabase(dbPath)
			if cErr != nil {
				return cErr
			}
			f.Close()
		} else {
			return fmt.Errorf("%s already exists", dbname)
		}

		fmt.Printf("Database %s created\n", dbname)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(createCmd)
}
