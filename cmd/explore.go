package cmd

import (
	"fmt"

	"gbq-analizer/internal/bigquery"

	"github.com/spf13/cobra"
)

var exploreCmd = &cobra.Command{
	Use:   "explore [dataset] [table]",
	Short: "Explore BigQuery datasets and tables",
	RunE: func(cmd *cobra.Command, args []string) error {
		projectID, _ := cmd.Flags().GetString("project")
		if projectID == "" {
			return fmt.Errorf("project ID required")
		}

		client, err := bigquery.NewClient(projectID)
		if err != nil {
			return err
		}
		defer client.Close()

		switch len(args) {
		case 0:
			return client.ListDatasets()
		case 1:
			return client.ListTables(args[0])
		case 2:
			return client.DescribeTable(args[0], args[1])
		default:
			return fmt.Errorf("too many arguments")
		}
	},
}

func init() {
	rootCmd.AddCommand(exploreCmd)
	exploreCmd.Flags().BoolP("sample", "s", false, "Show sample rows from table")
}
