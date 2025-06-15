package cmd

import (
	"fmt"
	"gbq-analizer/internal/bigquery"

	"github.com/spf13/cobra"
)

var testCmd = &cobra.Command{
	Use:   "test",
	Short: "Test BigQuery connection",
	RunE: func(cmd *cobra.Command, args []string) error {
		projectID, _ := cmd.Flags().GetString("project")
		if projectID == "" {
			return fmt.Errorf("project ID required. Use --project flag")
		}

		client, err := bigquery.NewClient(projectID)
		if err != nil {
			return err
		}
		defer client.Close()

		return client.TestConnection()
	},
}

func init() {
	rootCmd.AddCommand(testCmd)
}
