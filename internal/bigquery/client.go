package bigquery

import (
	"context"
	"fmt"
	"os"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/olekukonko/tablewriter"
	"google.golang.org/api/iterator"
)

type Client struct {
	client    *bigquery.Client
	projectID string
	ctx       context.Context
}

func NewClient(projectID string) (*Client, error) {
	ctx := context.Background()

	if projectID == "" {
		projectID = "bigquery-public-data"
	}

	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %v", err)
	}

	return &Client{
		client:    client,
		projectID: projectID,
		ctx:       ctx,
	}, nil
}

func (c *Client) Close() error {
	return c.client.Close()
}

// Helpers
func (c *Client) isPublicDataset(datasetID string) bool {
	return strings.HasPrefix(datasetID, "bigquery-public-data.")
}

func (c *Client) getDatasetReference(datasetID string) (*bigquery.Dataset, func(), error) {
	if c.isPublicDataset(datasetID) {
		publicClient, err := bigquery.NewClient(c.ctx, "bigquery-public-data")
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create public data client: %v", err)
		}

		datasetName := strings.TrimPrefix(datasetID, "bigquery-public-data.")
		cleanup := func() { publicClient.Close() }
		return publicClient.Dataset(datasetName), cleanup, nil
	}
	return c.client.Dataset(datasetID), func() {}, nil
}

// Test query to verify connection
func (c *Client) TestConnection() error {
	query := c.client.Query("SELECT 1 as test_value")

	it, err := query.Read(c.ctx)
	if err != nil {
		return fmt.Errorf("test query failed: %v", err)
	}

	var row []bigquery.Value
	err = it.Next(&row)
	if err != nil {
		return fmt.Errorf("failed to read test value: %v", err)
	}

	fmt.Printf("BigQuery connection successfull! Test result: %v\n", row[0])
	return nil
}

func (c *Client) ExecuteQuery(sql string, limit int) error {
	query := c.client.Query(sql)

	it, err := query.Read(c.ctx)
	if err != nil {
		return fmt.Errorf("query execute failed: %v", err)
	}

	table := tablewriter.NewWriter(os.Stdout)

	rowCount := 0
	headerSet := false

	for {
		var row []bigquery.Value
		err = it.Next(&row)
		if err != nil {
			break
		}
		if !headerSet {
			schema := it.Schema
			headers := make([]string, len(schema))
			for i, field := range schema {
				headers[i] = field.Name
			}
			table.Header(headers)
			headerSet = true
		}

		rowStrings := make([]string, len(row))
		for i, val := range row {
			rowStrings[i] = fmt.Sprintf("%v", val)
		}
		table.Append(rowStrings)

		rowCount++
		if rowCount >= limit {
			break
		}
	}
	if rowCount == 0 {
		fmt.Println("No results found.")
		return nil
	}

	table.Render()
	fmt.Printf("\nRows returned: %d\n", rowCount)
	return nil
}

func (c *Client) ListDatasets() error {
	// Create a client specifically for the public data project
	publicClient, err := bigquery.NewClient(c.ctx, "bigquery-public-data")
	if err != nil {
		return fmt.Errorf("failed to create public data client: %v", err)
	}
	defer publicClient.Close()

	fmt.Println("=== Available Public Datasets ===")
	it := publicClient.Datasets(c.ctx)

	count := 0
	for {
		dataset, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("error listing public datasets: %v", err)
		}
		fmt.Printf("- bigquery-public-data.%s\n", dataset.DatasetID)
		count++

		// Limit output since there are many
		if count >= 20 {
			fmt.Println("... (showing first 20, there are many more)")
			break
		}
	}

	return nil
}

func (c *Client) ListTables(datasetID string) error {
	dataset, cleanup, err := c.getDatasetReference(datasetID)
	if err != nil {
		return err
	}
	defer cleanup()

	it := dataset.Tables(c.ctx)
	fmt.Printf("Tables in dataset '%s':\n", datasetID)

	count := 0
	for {
		table, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("error listing tables: %v", err)
		}
		fmt.Printf("- %s\n", table.TableID)
		count++
	}

	if count == 0 {
		fmt.Println("No tables found")
	}

	return nil
}

func (c *Client) DescribeTable(datasetID, tableID string) error {
	dataset, cleanup, err := c.getDatasetReference(datasetID)
	if err != nil {
		return err
	}
	defer cleanup()

	table := dataset.Table(tableID)
	metadata, err := table.Metadata(c.ctx)
	if err != nil {
		return fmt.Errorf("failed to get table metadata %v:", err)
	}

	fmt.Printf("\nTable: %s.%s\n", datasetID, tableID)
	fmt.Printf("Description: %s\n", metadata.Description)
	fmt.Printf("Rows: %d\n", metadata.NumRows)
	fmt.Printf("Size: %d bytes\n", metadata.NumBytes)

	fmt.Println("\nSchema")
	for _, field := range metadata.Schema {
		fmt.Printf(" %s (%s) - %s\n", field.Name, field.Type, field.Description)
	}
	return nil
}

func (c *Client) ListPrivateDatasets() error {
	fmt.Printf("=== Datasets in project: %s ===\n", c.projectID)
	it := c.client.Datasets(c.ctx)

	count := 0
	for {
		dataset, err := it.Next()
		if err != nil {
			break
		}
		fmt.Printf("- %s\n", dataset.DatasetID)
		count++
	}

	if count == 0 {
		fmt.Println("No datasets found in this project")
	} else {
		fmt.Printf("\nTotal datasets: %d\n", count)
	}
	return nil
}
