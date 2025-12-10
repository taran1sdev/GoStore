package cli

import (
	"fmt"

	"github.com/spf13/cobra"
	"go.store/internal/auth"
)

// Later let's give a -p option to include password in cmdline - if ommited we will
// prompt for password with protection
var userCreateCmd = &cobra.Command{
	Use:   "create-user <username> <password> <role>",
	Args:  cobra.ExactArgs(3),
	Short: "Create a new GoStore user",
	RunE: func(cmd *cobra.Command, args []string) error {
		username, password, roleStr := args[0], args[1], args[2]

		fs, err := auth.NewFileStore(cfg.UserFile)
		if err != nil {
			return err
		}

		if u := fs.GetUser(username); u == nil {
			return fmt.Errorf("User already exists")
		}

		hash, err := auth.HashPassword(password)
		if err != nil {
			return err
		}

		u := &auth.User{
			Username: username,
			Password: string(hash),
			Role:     auth.Role(roleStr),
			AccessDB: []string{},
		}

		if err := fs.SaveUser(u); err != nil {
			return err
		}

		fmt.Printf("User %s created\n", username)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(userCreateCmd)
}
