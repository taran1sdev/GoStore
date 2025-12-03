package cli

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

// Create our own help menu when run interactive
func replHelp() {
	fmt.Println("Commands")
	fmt.Println("\tset <key> <value>")
	fmt.Println("\tget <key>")
	fmt.Println("\tdelete <key>")
	fmt.Println("\texit")
}

// Starts an interactive command session
// Forwards commands to cobra
func startREPL(root *cobra.Command) {
	reader := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("GoStore> ")

		if !reader.Scan() {
			return
		}

		// Get the command typed by the user
		input := strings.TrimSpace(reader.Text())

		// Check for blank input
		if input == "" {
			continue
		}

		if input == "help" {
			replHelp()
			continue
		}

		args := strings.Fields(input)

		// Pass the command back to root
		root.SetArgs(args)

		// Execute the command
		// We don't have to do anything with the error here - it's handled in root
		_ = root.ExecuteContext(context.Background())
	}
}
