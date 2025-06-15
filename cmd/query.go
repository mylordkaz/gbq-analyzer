package cmd

import (
	"fmt"
	"gbq-analizer/internal/bigquery"

	"github.com/spf13/cobra"
)

var queryCmd = &cobra.Command{
	Use:   "query [SQL]",
	Short: "Execute a SQL query against BigQuery",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		projectID, _ := cmd.Flags().GetString("project")
		if projectID == "" {
			return fmt.Errorf("project ID is required. Use --project flag")
		}

		sql := args[0]
		limit, _ := cmd.Flags().GetInt("limit")

		client, err := bigquery.NewClient(projectID)
		if err != nil {
			return err
		}
		defer client.Close()

		return client.ExecuteQuery(sql, limit)
	},
}

func init() {
	rootCmd.AddCommand(queryCmd)
	queryCmd.Flags().IntP("limit", "l", 10, "Limit number of results")
}
