package bigquery

import (
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"context"
	"google.golang.org/api/option"
)

var ctx context.Context
var Authfile string
var client *bigquery.Client

type Rows []Row
type Row struct{
	Gid string `bigquery:"gid"`
	CreatedAt civil.DateTime `bigquery:"created_at"`
}

func Init(project string) (err error) {
	ctx = context.Background()
	auth := option.WithCredentialsFile(Authfile)

	// Try to create a client with auth file
	client, err = bigquery.NewClient(ctx, project, auth)
	if err != nil {
		client, err = bigquery.NewClient(ctx, project)
	}
	if err != nil { return }
	return
}

func Query(query string, params *[]bigquery.QueryParameter) (*bigquery.RowIterator, error)  {
	q := client.Query(query)
	if params != nil { q.Parameters = *params }
	return q.Read(ctx)
}

func Insert(rows Rows, dataset string, table string) error {
	inserter := client.Dataset(dataset).Table(table).Inserter()
	return inserter.Put(ctx, rows)
}

