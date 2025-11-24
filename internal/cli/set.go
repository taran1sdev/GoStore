package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

var setCmd = &cobra.Command{
	Use:   "set <key> <value>",
	Short: "Create a new <key> <value> pair",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		err := db.Set(args[0], []byte(args[1]))
		if err != nil {
			return err
		}

		fmt.Printf("%s created successfully!\n", args[0])
		return nil
	},
}
