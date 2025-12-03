package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

var delCommand = &cobra.Command{
	Use:  "delete",
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		err := db.Delete(args[0])
		if err != nil {
			return err
		}

		fmt.Printf("%s has been deleted!\n", args[0])
		return nil
	},
}

func init() {
	rootCmd.AddCommand(delCommand)
}
