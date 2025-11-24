package cli

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

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

		args := strings.Fields(input)

		// Pass the command back to root
		root.SetArgs(args)

		// Execute the command
		// We don't have to do anything with the error here - it's handled in root
		_ = root.ExecuteContext(context.Background())
	}
}
