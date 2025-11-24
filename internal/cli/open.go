package cli

import (
	"github.com/spf13/cobra"
	"go.store/internal/engine"
)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use: "open",
		Short: "Open a DB file, if the file does not exist a new DB file will be created",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (*Database, error) {
			path := args[0]
			db, err := engine.Open(path)
			if err != nil {
				return nil, err
			}
			return db, nil
		},
	}
	cmd.Flags().String("path", "file.db", "path to .db file")
	return cmd
}


