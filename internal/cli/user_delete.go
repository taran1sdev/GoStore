package cli

import (
	"fmt"

	"github.com/spf13/cobra"
	"go.store/internal/auth"
)

var userDelCmd = &cobra.Command{
	Use:   "delete-user <username>",
	Args:  cobra.ExactArgs(1),
	Short: "Delete a GoStore user",
	RunE: func(cmd *cobra.Command, args []string) error {
		username := args[0]

		fs, err := auth.NewFileStore(cfg.UserFile)
		if err != nil {
			return err
		}

		if u := fs.GetUser(username); u == nil {
			return fmt.Errorf("User does not exist")
		}

		if err := fs.DeleteUser(username); err != nil {
			return err
		}

		fmt.Printf("User %s deleted", username)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(userDelCmd)
}
