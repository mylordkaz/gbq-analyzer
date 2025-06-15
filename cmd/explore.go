package cmd

import (
	"fmt"
	"strings"

	"gbq-analizer/internal/bigquery"

	"github.com/spf13/cobra"
)

var exploreCmd = &cobra.Command{
	Use:   "explore [dataset] [table]",
	Short: "Explore BigQuery datasets and tables",
	Long: `Explore BigQuery datasets and tables.

	Without -p flag: explores public datasets (bigquery-public-data)
	With -p flag: explores your project's private datasets`,
	RunE: func(cmd *cobra.Command, args []string) error {
		projectID, _ := cmd.Flags().GetString("project")

		if projectID != "" {
			return explorePrivateDatasets(projectID, args)
		}
		return explorePublicDatasets(args)
	},
}

func explorePublicDatasets(args []string) error {
	// Use default project name
	client, err := bigquery.NewClient("public")
	if err != nil {
		return err
	}
	defer client.Close()

	switch len(args) {
	case 0:
		return client.ListDatasets()
	case 1:
		dataset := args[0]
		if !strings.HasPrefix(dataset, "bigquery-public-data.") {
			dataset = "bigquery-public-data." + dataset
		}
		return client.ListTables(dataset)
	case 2:
		dataset := args[0]
		if !strings.HasPrefix(dataset, "bigquery-public-data.") {
			dataset = "bigquery-public-data." + dataset
		}
		return client.DescribeTable(dataset, args[1])
	default:
		return fmt.Errorf("too much arguments")
	}
}

func explorePrivateDatasets(projectID string, args []string) error {
	client, err := bigquery.NewClient(projectID)
	if err != nil {
		return err
	}
	defer client.Close()

	switch len(args) {
	case 0:
		return client.ListPrivateDatasets()
	case 1:
		return client.ListTables(args[0])
	case 2:
		return client.DescribeTable(args[0], args[1])
	default:
		return fmt.Errorf("too many arguments")
	}
}

func init() {
	rootCmd.AddCommand(exploreCmd)
	exploreCmd.Flags().BoolP("sample", "s", false, "Show sample rows from table")
}
