package main

import (
	"context"
	"encoding/csv"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
)

const (
	datasetName = "bq"
	location    = "US"
	tableName   = "test"
	tmpCsv      = "/tmp/bq.csv"
)

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

	if err := createCsv(tmpCsv); err != nil {
		panic(err)
	}

	if err := importCsv(ctx, ds.Table(tableName), tmpCsv); err != nil {
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

func importCsv(ctx context.Context, t *bigquery.Table, f string) error {
	file, err := os.Open(filepath.Clean(f))
	if err != nil {
		return err
	}
	source := bigquery.NewReaderSource(file)
	source.AutoDetect = true
	source.SkipLeadingRows = 1
	loader := t.LoaderFrom(source)

	job, err := loader.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	if err := status.Err(); err != nil {
		return err
	}
	return nil
}

func createCsv(f string) error {
	file, err := os.OpenFile(filepath.Clean(f), os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer closeFile(file)

	err = file.Truncate(0)
	if err != nil {
		return err
	}

	w := csv.NewWriter(file)

	var header = []string{"name"}
	w.Write(header)
	w.Write([]string{"foo"})
	w.Write([]string{"bar"})
	w.Flush()

	return nil
}

func closeFile(file *os.File) {
	if err := file.Close(); err != nil {
		log.Fatal(err)
	}
}
