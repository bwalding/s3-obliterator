package main

import (
	"fmt"
	"os"

	"github.com/bwalding/s3-obliterator/internal/commands/removekeys"
	"github.com/spf13/cobra"
)

func main() {

	rootCmd := &cobra.Command{
		Use:   "s3-obliterator",
		Short: "A CLI tool to obliterate S3 buckets",
		Run: func(cmd *cobra.Command, args []string) {
			// Add your code here

		},
	}

	rootCmd.AddCommand(removekeys.CreateCommand())

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
