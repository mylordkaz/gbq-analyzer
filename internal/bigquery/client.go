package bigquery

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/bigquery"
	"github.com/olekukonko/tablewriter"
)

type Client struct {
	client    *bigquery.Client
	projectID string
	ctx       context.Context
}

func NewClient(projectID string) (*Client, error) {
	ctx := context.Background()

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
			headers := make([]string, len(row))
			for i := range row {
				headers[i] = fmt.Sprintf("Column_%d", i+1)
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
