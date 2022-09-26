package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/timeplus-io/go-client/client"
)

func createStream(timeplusClient *client.TimeplusClient, streamName string) error {
	streamDef := client.StreamDef{
		Name: streamName,
		Columns: []client.ColumnDef{
			{
				Name: "device",
				Type: "string",
			},
			{
				Name: "region",
				Type: "string",
			},
			{
				Name: "city",
				Type: "string",
			},
			{
				Name: "version",
				Type: "string",
			},
			{
				Name: "lat",
				Type: "float32",
			},
			{
				Name: "lon",
				Type: "float32",
			},
			{
				Name: "battery",
				Type: "float32",
			},
			{
				Name: "humidity",
				Type: "float32",
			},
			{
				Name: "temperature",
				Type: "int16",
			},
			{
				Name: "hydraulic_pressure",
				Type: "float32",
			},
			{
				Name: "atmospheric_pressure",
				Type: "float32",
			},
		},
		TTLExpression:          "to_datetime(_tp_time) + INTERVAL 2 HOUR",
		LogStoreRetentionBytes: 12884901888,
		LogStoreRetentionMS:    86400000,
	}

	if !timeplusClient.ExistStream(streamName) {
		if err := timeplusClient.CreateStream(streamDef); err != nil {
			fmt.Printf("failed to create stream %s\n", err)
			return err
		} else {
			fmt.Printf("create stream %s succeeded\n", streamName)
			return nil
		}
	} else {
		fmt.Printf("stream %s already exists\n", streamName)
	}
	return nil
}

func doIngest(timeplusClient *client.TimeplusClient, streamName string, batchSize int, iterations int, concurrency int) {
	payload := &client.IngestPayload{
		Stream: streamName,
		Data: client.IngestData{
			Columns: []string{
				"device",
				"region",
				"city",
				"version",
				"lat",
				"lon",
				"battery",
				"humidity",
				"temperature",
				"hydraulic_pressure",
				"atmospheric_pressure",
			},
		},
	}

	for i := 0; i < batchSize; i++ {
		payload.Data.Data = append(payload.Data.Data, []any{"device-1", "north-ca-1", "san franscisco", "1.0", 176.78, 239.79, 79.7, 17.56, 75, 100.5, 200.7})
	}

	var wg sync.WaitGroup
	for j := 0; j < concurrency; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				if err := timeplusClient.InsertData(payload); err != nil {
					fmt.Printf("failed to ingest data, error=%+v", err)
					break
				}
			}
		}()
	}

	wg.Wait()
}

func ingest(timeplusClient *client.TimeplusClient, tenant string, streamName string, numStreams int, batchSize int, iterations int, concurrency int) {
	for n := 0; n < numStreams; n++ {
		if err := createStream(timeplusClient, fmt.Sprintf("%s%d", streamName, n+1)); err != nil {
			return
		}
	}

	start := time.Now().UnixNano()

	var wg sync.WaitGroup
	for n := 0; n < numStreams; n++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			doIngest(timeplusClient, fmt.Sprintf("%s%d", streamName, i+1), batchSize, iterations, concurrency)
		}(n)
	}
	wg.Wait()

	took_ms := (time.Now().UnixNano() - start) / 1000000
	total := int64(iterations) * int64(batchSize) * int64(concurrency) * int64(numStreams)
	eps := (total * 1000) / took_ms

	fmt.Printf("Ingested to %d streams workspace=%s with total %d rows in %d milliseconds, eps=%d\n", numStreams, tenant, total, took_ms, eps)
}

func main() {
	apiKeys := []string{}
	tenants := []string{}

	timeplusAddress := "https://beta.timeplus.cloud"

	streamName := "perf"
	numStreams := 10
	batchSize := 1000
	iterations := 100
	concurrency := 10

	var wg sync.WaitGroup
	for i := 0; i < len(apiKeys); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			timeplusClient := client.NewCient(timeplusAddress, tenants[n], apiKeys[n])
			ingest(timeplusClient, tenants[n], streamName, numStreams, batchSize, iterations, concurrency)
		}(i)
	}
	wg.Wait()
}
