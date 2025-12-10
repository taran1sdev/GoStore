package cli

import (
	"fmt"
	"slices"

	"github.com/spf13/cobra"
	"go.store/internal/auth"
)

var grantCmd = &cobra.Command{
	Use:   "grant <username> <dbname>",
	Args:  cobra.ExactArgs(2),
	Short: "Grant user access to db",
	RunE: func(cmd *cobra.Command, args []string) error {
		username, dbname := args[0], args[1]

		fs, err := auth.NewFileStore(cfg.UserFile)
		if err != nil {
			return err
		}

		u := fs.GetUser(username)
		if u == nil {
			return fmt.Errorf("Could not find user %s")
		}

		if !slices.Contains(u.AccessDB, dbname) {
			u.AccessDB = append(u.AccessDB, dbname)
		}

		if err := fs.SaveUser(u); err != nil {
			return err
		}

		fmt.Printf("Granted %s access to %s\n", username, dbname)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(grantCmd)
}
