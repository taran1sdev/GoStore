package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.store/internal/config"
)

var (
	homeFlag   string
	configFlag string
	cfg        *config.Config
)

var rootCmd = &cobra.Command{
	Use:   "gostore",
	Short: "GoStore CLI",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		var err error
		cfg, err = config.LoadConfig(homeFlag, configFlag)
		return err
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}

func init() {
	rootCmd.CompletionOptions.DisableDefaultCmd = true
	rootCmd.SilenceUsage = true
	rootCmd.SilenceErrors = true

	rootCmd.PersistentFlags().StringVar(&homeFlag, "home", "", "GoStore home directory")
	rootCmd.PersistentFlags().StringVar(&configFlag, "config", "", "Path to config.yaml")

}
