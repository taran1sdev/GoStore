package cli

import (
	"fmt"
	"slices"

	"github.com/spf13/cobra"
	"go.store/internal/auth"
)

var revokeCmd = &cobra.Command{
	Use:   "revoke <username> <dbname>",
	Args:  cobra.ExactArgs(2),
	Short: "Revoke user access to a database",
	RunE: func(cmd *cobra.Command, args []string) error {
		username, dbname := args[0], args[1]

		fs, err := auth.NewFileStore(cfg.UserFile)
		if err != nil {
			return err
		}

		u := fs.GetUser(username)
		if u == nil {
			return fmt.Errorf("Could not find user: %s", username)
		}

		if idx := slices.Index(u.AccessDB, dbname); idx != -1 {
			u.AccessDB = append(u.AccessDB[idx:], u.AccessDB[idx+1:]...)
		}

		if err := fs.SaveUser(u); err != nil {
			return err
		}

		fmt.Printf("Revoked %s access to %s", username, dbname)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(revokeCmd)
}
