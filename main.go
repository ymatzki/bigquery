package main

import (
	"context"
	"net/http"
	"os"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
)

const (
	datasetName = "bq"
	location    = "US"
	tableName   = "test"
)

type TableSchema struct {
	Name string `bigquery:name`
}

func main() {
	projectID, existed := os.LookupEnv("PROJECT_ID")
	if !existed {
		panic("PROJECT_ID not set")
	}
	if _, existed := os.LookupEnv("GOOGLE_APPLICATION_CREDENTIALS"); !existed {
		panic("GOOGLE_APPLICATION_CREDENTIALS not set")
	}

	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	ds, err := createDataSet(ctx, *client)
	if err != nil {
		panic(err)
	}
	t, err := createTable(ctx, *ds)
	if err != nil {
		panic(err)
	}
	err = putData(ctx, *t)
	if err != nil {
		panic(err)
	}
}

func createDataSet(ctx context.Context, client bigquery.Client) (*bigquery.Dataset, error) {
	ds := client.Dataset(datasetName)
	if err := ds.Create(ctx, &bigquery.DatasetMetadata{
		Name:     datasetName,
		Location: location,
	}); err != nil {
		e, ok := err.(*googleapi.Error)
		if !ok {
			return nil, err
		}
		if e.Code != http.StatusConflict {
			return nil, err
		}
	}
	return ds, nil
}

func createTable(ctx context.Context, ds bigquery.Dataset) (*bigquery.Table, error) {
	t := ds.Table(tableName)
	err := t.Create(ctx, &bigquery.TableMetadata{
		Name:     tableName,
		Location: location,
		Schema: bigquery.Schema{
			&bigquery.FieldSchema{
				Name: "name",
				Type: bigquery.StringFieldType,
			},
		},
	})
	if err != nil {
		e, ok := err.(*googleapi.Error)
		if !ok {
			return nil, err
		}
		if e.Code != http.StatusConflict {
			return nil, err
		}
	}
	return t, nil
}

func putData(ctx context.Context, t bigquery.Table) error {
	items := []*TableSchema{{Name: "foo"}}
	err := t.Inserter().Put(ctx, items)
	if err != nil {
		return err
	}
	return nil
}
