package cmd

import (
	"fmt"
	"gbq-analizer/internal/bigquery"
	"strings"

	"github.com/spf13/cobra"
)

var analyzeCmd = &cobra.Command{
	Use:   "analyze [dataset.table]",
	Short: "Generate and run smart analytical queries for any table",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		projectID, _ := cmd.Flags().GetString("project")
		if projectID == "" {
			return fmt.Errorf("project ID required for running queries. Use --project flag with your GCP project")
		}

		client, err := bigquery.NewClient(projectID)
		if err != nil {
			return err
		}
		defer client.Close()

		parts := strings.Split(args[0], ".")
		if len(parts) < 2 {
			return fmt.Errorf("use format: dataset.table or project.dataset.table")
		}

		var datasetID, tableID string
		if len(parts) == 2 {
			datasetID, tableID = parts[0], parts[1]
		} else {
			tableID = parts[len(parts)-1]
			datasetID = strings.Join(parts[:len(parts)-1], ".")
		}

		return client.AnalyzeTable(datasetID, tableID)
	},
}

func init() {
	rootCmd.AddCommand(analyzeCmd)
	analyzeCmd.Flags().BoolP("sample", "s", false, "Show sample data first")
}
