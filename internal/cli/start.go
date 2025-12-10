package cli

import (
	"log"

	"github.com/spf13/cobra"
	"go.store/internal/server"
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start GoStore server",
	RunE: func(cmd *cobra.Command, args []string) error {
		srv, err := server.New(cfg)
		if err != nil {
			return err
		}

		log.Printf("Server stared on %s\n", cfg.Addr)
		return srv.Listen()
	},
}

func init() {
	rootCmd.AddCommand(startCmd)
}
