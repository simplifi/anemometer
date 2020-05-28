package monitor

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	_ "github.com/lib/pq" // Postgres driver
	"github.com/simplifi/anemometer/pkg/anemometer/config"
	_ "github.com/vertica/vertica-sql-go" // Vertica driver
)

// Monitor runs a query and pushes results to DataDog as metrics/tags
type Monitor struct {
	databaseConn  *sql.DB
	statsdClient  *statsd.Client
	name          string
	dbType        string
	dbURI         string
	sleepDuration int
	metric        string
	sql           string
}

// New Monitor, pass in the MonitorConfig
func New(statsdConfig config.StatsdConfig, monitorConfig config.MonitorConfig) (*Monitor, error) {
	databaseConn, err := createDBConn(monitorConfig.DatabaseConfig.Type, monitorConfig.DatabaseConfig.URI)
	if err != nil {
		return nil, err
	}

	statsdClient, err := createStatsdClient(
		statsdConfig.Address,
		statsdConfig.Tags,
	)
	if err != nil {
		return nil, err
	}

	monitor := Monitor{
		databaseConn:  databaseConn,
		statsdClient:  statsdClient,
		name:          monitorConfig.Name,
		sleepDuration: monitorConfig.SleepDuration,
		metric:        monitorConfig.Metric,
		sql:           monitorConfig.SQL,
	}

	return &monitor, nil
}

func createDBConn(dbType string, dbURI string) (*sql.DB, error) {
	conn, err := sql.Open(dbType, dbURI)
	if err != nil {
		return nil, err
	}

	conn.Ping()
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func createStatsdClient(address string, tags []string) (*statsd.Client, error) {
	client, err := statsd.New(
		address,
		statsd.WithTags(tags),
	)
	if err != nil {
		return nil, err
	}

	return client, nil
}

// Start the Monitor
func (m *Monitor) Start() {
	for {
		log.Printf("INFO: Sleeping for %d seconds", m.sleepDuration)
		time.Sleep(time.Duration(m.sleepDuration) * time.Second)

		// Execute our query
		rows, err := m.databaseConn.Query(m.sql)
		if err != nil {
			log.Printf("ERROR: %v", err)
			sendErrorMetric(m.statsdClient, m.name)
			continue
		}
		cols, _ := rows.Columns()

		// // Iterate on the resulting rows
		for rows.Next() {

			// Convert our result row into a map
			rowMap, err := rowsToMap(cols, rows)
			if err != nil {
				log.Printf("ERROR: %v", err)
				sendErrorMetric(m.statsdClient, m.name)
				continue
			}

			// Grab the metric column from the results and convert it
			metric, err := getMetric(rowMap)
			if err != nil {
				log.Printf("ERROR: %v", err)
				sendErrorMetric(m.statsdClient, m.name)
				continue
			}

			// Aggregate all the tags from the query
			tags := getTags(rowMap)

			// Push the metric to Datadog
			if err = m.statsdClient.Gauge(m.metric, metric, tags, 1); err != nil {
				log.Printf("ERROR: %v", err)
				sendErrorMetric(m.statsdClient, m.name)
			}
		}

	}
}

// Sends an error metric to StatsD
func sendErrorMetric(statsdClient *statsd.Client, name string) {
	statsdClient.Gauge(
		"anemometer.error",
		1,
		[]string{fmt.Sprintf("name:%s", name)},
		1,
	)
}

// We cannot use a struct for query results since our queries can change based
// on the provided configuration.
// This function converts the rows into a map so it will be easier to work with.
func rowsToMap(cols []string, rows *sql.Rows) (map[string]interface{}, error) {
	// Create a slice of interface{}'s to represent each column,
	// and a second slice to contain pointers to each item in the columns slice.
	columns := make([]interface{}, len(cols))
	columnPointers := make([]interface{}, len(cols))
	for i := range columns {
		columnPointers[i] = &columns[i]
	}

	// Scan the result into the column pointers...
	if err := rows.Scan(columnPointers...); err != nil {
		return nil, err
	}

	// Create our map, and retrieve the value for each column from the pointers slice,
	// storing it in the map with the name of the column as the key.
	m := make(map[string]interface{})
	for i, colName := range cols {
		val := columnPointers[i].(*interface{})
		m[colName] = *val
	}

	return m, nil
}

// Function to aggregate tag columns
// Assume that any column not named "metric" is a tag
func getTags(results map[string]interface{}) []string {
	var tags []string

	for name, value := range results {
		// Ignore the metric column, we only care about tags here
		if name == "metric" {
			continue
		}

		// Aggregate all the tag columns
		tags = append(tags, fmt.Sprintf("%v:%v", name, value))
	}

	return tags
}

// Function to pull the 'metric' column's value, convert, and return it as float
// If conversion isn't possible, or column is missing, this will return an error
func getMetric(results map[string]interface{}) (float64, error) {
	var metric float64

	if val, ok := results["metric"]; ok {
		// We have a metric column, so we'll convert it to float64
		switch val.(type) {
		case int:
			metric = float64(val.(int))
		case int8:
			metric = float64(val.(int8))
		case int16:
			metric = float64(val.(int16))
		case int32:
			metric = float64(val.(int32))
		case int64:
			metric = float64(val.(int64))
		case float32:
			metric = float64(val.(float32))
		case float64:
			metric = val.(float64)
		case bool:
			if val.(bool) {
				metric = 1
			} else {
				metric = 0
			}
		default:
			return -1, fmt.Errorf("Failed to convert metric column value: '%v'", val)
		}
	} else {
		return -1, fmt.Errorf("No metric column found")
	}

	return metric, nil
}
