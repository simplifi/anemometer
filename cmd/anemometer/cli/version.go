package cli

import (
	"fmt"

	"github.com/simplifi/anemometer/pkg/anemometer/version"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("Version %s", version.Version)
	},
}
