package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "gbq-analyzer",
	Short: "A CLI tool for BigQuery data analytics",
	Long:  "gbq-analyzer helps you analyze data in Google BigQuery with simple command directly in your terminal",
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	// Global flags
	rootCmd.PersistentFlags().StringP("project", "p", "", "Google Cloud Project ID")
}
