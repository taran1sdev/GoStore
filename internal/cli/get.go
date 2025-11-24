package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

var getCmd = &cobra.Command{
	Use:   "get <key>",
	Short: "Retrieve value associated with <key>",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		val, err := db.Get(args[0])
		if err != nil {
			return err
		}

		fmt.Println(string(val))
		return nil
	},
}
