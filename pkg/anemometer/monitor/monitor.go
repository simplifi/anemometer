package monitor

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	_ "github.com/lib/pq"           // Postgres driver
	_ "github.com/mattn/go-sqlite3" // SQLite driver
	"github.com/simplifi/anemometer/pkg/anemometer/config"
	_ "github.com/vertica/vertica-sql-go" // Vertica driver
	_ "github.com/viant/bigquery"         // BigQuery driver
)

// Monitor runs a query and pushes results to DataDog as metrics/tags
type Monitor struct {
	databaseConn  *sql.DB
	statsdClient  statsd.ClientInterface
	name          string
	sleepDuration int
	metric        string
	metricType    string
	eventConfig   config.EventConfig
	// Computed once per monitor because it is used for every returned row.
	metricExcludedColumns map[string]struct{}
	sql                   string
}

// New Monitor, pass in the MonitorConfig
func New(statsdConfig config.StatsdConfig, monitorConfig config.MonitorConfig) (*Monitor, error) {
	if monitorConfig.EventConfig.Enabled {
		if _, err := getEventAlertType(monitorConfig.EventConfig.AlertType); err != nil {
			return nil, err
		}
		if _, err := getEventPriority(monitorConfig.EventConfig.Priority); err != nil {
			return nil, err
		}
	}

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
		databaseConn:          databaseConn,
		statsdClient:          statsdClient,
		name:                  monitorConfig.Name,
		sleepDuration:         monitorConfig.SleepDuration,
		metric:                monitorConfig.Metric,
		metricType:            monitorConfig.MetricType,
		eventConfig:           monitorConfig.EventConfig,
		metricExcludedColumns: newMetricExcludedColumns(monitorConfig.EventConfig),
		sql:                   monitorConfig.SQL,
	}

	return &monitor, nil
}

func createDBConn(dbType string, dbURI string) (*sql.DB, error) {
	conn, err := sql.Open(dbType, dbURI)
	if err != nil {
		return nil, err
	}

	err = conn.Ping()
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func createStatsdClient(address string, tags []string) (statsd.ClientInterface, error) {
	client, err := statsd.New(
		address,
		statsd.WithTags(tags),
	)
	if err != nil {
		return nil, err
	}

	return client, nil
}

// sendMetric sends the appropriate metric type to Datadog based on the configured metric type
func (m *Monitor) sendMetric(rowMap map[string]interface{}, tags []string, debug bool) error {
	metricFloat, err := getMetricFloat64(rowMap)
	if err != nil {
		return err
	}

	timestamp, err := getTimestamp(rowMap)
	if err != nil {
		return err
	}

	if debug {
		log.Printf("DEBUG: [%s] Publishing %s metric - Name: %s, Value: %v, Tags: %v",
			m.name, m.metricType, m.metric, metricFloat, tags)
	}

	switch m.metricType {
	case "count":
		return m.statsdClient.CountWithTimestamp(m.metric, int64(metricFloat), tags, 1, timestamp)
	case "histogram":
		return m.statsdClient.Histogram(m.metric, metricFloat, tags, 1)
	case "distribution":
		return m.statsdClient.Distribution(m.metric, metricFloat, tags, 1)
	case "gauge":
		return m.statsdClient.GaugeWithTimestamp(m.metric, metricFloat, tags, 1, timestamp)
	default:
		return fmt.Errorf("unknown metric type: %s", m.metricType)
	}
}

// sendEvent sends a Datadog event built from the configured event columns.
func (m *Monitor) sendEvent(rowMap map[string]interface{}, tags []string, debug bool) error {
	if !m.eventConfig.Enabled {
		return nil
	}

	title, err := getEventField(rowMap, m.eventConfig.TitleColumn, m.eventConfig.Title, m.name, true)
	if err != nil {
		return err
	}

	text, err := getEventField(rowMap, m.eventConfig.TextColumn, m.eventConfig.Text, "", false)
	if err != nil {
		return err
	}

	alertType, err := getEventAlertType(m.eventConfig.AlertType)
	if err != nil {
		return err
	}

	priority, err := getEventPriority(m.eventConfig.Priority)
	if err != nil {
		return err
	}

	event := statsd.NewEvent(title, text)
	event.AlertType = alertType
	event.Priority = priority
	event.SourceTypeName = m.eventConfig.SourceTypeName
	event.Tags = tags

	aggregationKey, err := getEventField(rowMap, m.eventConfig.AggregationKeyColumn, m.eventConfig.AggregationKey, "", false)
	if err != nil {
		return err
	}
	event.AggregationKey = aggregationKey

	hostname, err := getEventField(rowMap, m.eventConfig.HostnameColumn, m.eventConfig.Hostname, "", false)
	if err != nil {
		return err
	}
	event.Hostname = hostname

	if debug {
		log.Printf("DEBUG: [%s] Publishing event - Title: %s, AlertType: %s, Priority: %s, Tags: %v",
			m.name, event.Title, event.AlertType, event.Priority, event.Tags)
	}

	return m.statsdClient.Event(event)
}

// Start the Monitor
func (m *Monitor) Start(debug bool) {
	for {
		log.Printf("INFO: [%s] Sleeping for %d seconds", m.name, m.sleepDuration)
		time.Sleep(time.Duration(m.sleepDuration) * time.Second)

		m.runOnce(debug)
	}
}

func (m *Monitor) runOnce(debug bool) {
	rows, err := m.databaseConn.Query(m.sql)
	if err != nil {
		log.Printf("ERROR: [%s] %v", m.name, err)
		sendErrorMetric(m.statsdClient, m.name)
		return
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("ERROR: [%s] %v", m.name, err)
			sendErrorMetric(m.statsdClient, m.name)
		}
	}()

	cols, err := rows.Columns()
	if err != nil {
		log.Printf("ERROR: [%s] %v", m.name, err)
		sendErrorMetric(m.statsdClient, m.name)
		return
	}

	// Iterate on the resulting rows
	for rows.Next() {
		// Convert our result row into a map
		rowMap, err := rowsToMap(cols, rows)
		if err != nil {
			log.Printf("ERROR: [%s] %v", m.name, err)
			sendErrorMetric(m.statsdClient, m.name)
			continue
		}

		m.processRow(rowMap, debug)
	}

	if err := rows.Err(); err != nil {
		log.Printf("ERROR: [%s] %v", m.name, err)
		sendErrorMetric(m.statsdClient, m.name)
	}
}

func (m *Monitor) processRow(rowMap map[string]interface{}, debug bool) {
	// Send the metric to Datadog using the configured metric type.
	if err := m.sendMetric(rowMap, m.getMetricTags(rowMap), debug); err != nil {
		log.Printf("ERROR: [%s] %v", m.name, err)
		sendErrorMetric(m.statsdClient, m.name)
	}

	if !m.eventConfig.Enabled {
		return
	}

	eventTags, err := m.getEventTags(rowMap)
	if err != nil {
		log.Printf("ERROR: [%s] %v", m.name, err)
		sendErrorMetric(m.statsdClient, m.name)
		return
	}

	if err = m.sendEvent(rowMap, eventTags, debug); err != nil {
		log.Printf("ERROR: [%s] %v", m.name, err)
		sendErrorMetric(m.statsdClient, m.name)
	}
}

// Sends an error metric to StatsD
func sendErrorMetric(statsdClient statsd.ClientInterface, name string) {
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
// Assume that any column not named "metric" or "timestamp" is a tag
func getTags(results map[string]interface{}) []string {
	return getTagsExcluding(results, reservedMetricColumns())
}

func (m *Monitor) getMetricTags(results map[string]interface{}) []string {
	excludedColumns := m.metricExcludedColumns
	if excludedColumns == nil {
		excludedColumns = newMetricExcludedColumns(m.eventConfig)
	}

	return getTagsExcluding(results, excludedColumns)
}

func (m *Monitor) getEventTags(results map[string]interface{}) ([]string, error) {
	tags := make([]string, 0, len(m.eventConfig.Tags)+len(m.eventConfig.TagColumns))
	tags = append(tags, m.eventConfig.Tags...)

	for _, column := range m.eventConfig.TagColumns {
		value, ok := getColumnString(results, column)
		if !ok {
			return nil, fmt.Errorf("event tag column not found: %s", column)
		}

		tags = append(tags, fmt.Sprintf("%v:%v", column, value))
	}

	return tags, nil
}

func reservedMetricColumns() map[string]struct{} {
	return map[string]struct{}{
		"metric":    {},
		"timestamp": {},
	}
}

func newMetricExcludedColumns(eventConfig config.EventConfig) map[string]struct{} {
	excludedColumns := reservedMetricColumns()

	if eventConfig.Enabled {
		for _, name := range eventMetricExcludedColumns(eventConfig) {
			excludedColumns[name] = struct{}{}
		}
	}

	return excludedColumns
}

func eventMetricExcludedColumns(eventConfig config.EventConfig) []string {
	columns := []string{
		"event_title",
		"event_text",
		"event_aggregation_key",
		"event_hostname",
	}

	for _, column := range []string{
		eventConfig.TitleColumn,
		eventConfig.TextColumn,
		eventConfig.AggregationKeyColumn,
		eventConfig.HostnameColumn,
	} {
		if column != "" {
			columns = append(columns, column)
		}
	}

	return columns
}

func getTagsExcluding(results map[string]interface{}, excludedColumns map[string]struct{}) []string {
	var tags []string

	for name, value := range results {
		// Ignore the metric and timestamp columns, we only care about tags here
		if _, ok := excludedColumns[name]; ok {
			continue
		}

		// Aggregate all the tag columns
		tags = append(tags, fmt.Sprintf("%v:%v", name, value))
	}

	return tags
}

func getEventField(results map[string]interface{}, column string, fallback string, defaultValue string, required bool) (string, error) {
	if column != "" {
		value, ok := getColumnString(results, column)
		if !ok {
			return "", fmt.Errorf("event column not found: %s", column)
		}

		if value != "" {
			return value, nil
		}
	}

	if fallback != "" {
		return fallback, nil
	}

	if defaultValue != "" {
		return defaultValue, nil
	}

	if required {
		return "", fmt.Errorf("event title is required")
	}

	return "", nil
}

func getColumnString(results map[string]interface{}, column string) (string, bool) {
	value, ok := results[column]
	if !ok {
		return "", false
	}

	if value == nil {
		return "", true
	}

	switch v := value.(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	case time.Time:
		return v.Format(time.RFC3339), true
	default:
		return fmt.Sprintf("%v", v), true
	}
}

func getEventAlertType(alertType string) (statsd.EventAlertType, error) {
	switch strings.ToLower(alertType) {
	case "", "info":
		return statsd.Info, nil
	case "error":
		return statsd.Error, nil
	case "warning":
		return statsd.Warning, nil
	case "success":
		return statsd.Success, nil
	default:
		return "", fmt.Errorf("unknown event alert type: %s", alertType)
	}
}

func getEventPriority(priority string) (statsd.EventPriority, error) {
	switch strings.ToLower(priority) {
	case "", "normal":
		return statsd.Normal, nil
	case "low":
		return statsd.Low, nil
	default:
		return "", fmt.Errorf("unknown event priority: %s", priority)
	}
}

// Function to pull the 'metric' column's value, convert, and return it as float64
// If conversion isn't possible, or column is missing, this will return an error
func getMetricFloat64(results map[string]interface{}) (float64, error) {
	var metric float64

	if val, ok := results["metric"]; ok {
		// We have a metric column, so we'll convert it to float64
		switch v := val.(type) {
		case int:
			metric = float64(v)
		case int8:
			metric = float64(v)
		case int16:
			metric = float64(v)
		case int32:
			metric = float64(v)
		case int64:
			metric = float64(v)
		case float32:
			metric = float64(v)
		case float64:
			metric = v
		case bool:
			if v {
				metric = 1
			} else {
				metric = 0
			}
		default:
			return -1, fmt.Errorf("failed to convert metric column value: '%v'", val)
		}
	} else {
		return -1, fmt.Errorf("no metric column found")
	}

	return metric, nil
}

// Function to pull the 'timestamp' column's value, convert, and return it as time.Time
// If conversion isn't possible, or column is missing, this will return an error
func getTimestamp(results map[string]interface{}) (time.Time, error) {
	if val, ok := results["timestamp"]; ok {
		switch v := val.(type) {
		case time.Time:
			return v, nil
		case string:
			if v == "" {
				return time.Time{}, fmt.Errorf("failed to convert timestamp column value: empty string")
			}
			timestamp, err := time.Parse(time.RFC3339, v)
			if err != nil {
				return time.Time{}, fmt.Errorf("failed to convert timestamp column value: '%v'", val)
			}
			return timestamp, nil
		case int64:
			return time.Unix(v, 0).UTC(), nil
		case int32:
			return time.Unix(int64(v), 0).UTC(), nil
		case int:
			return time.Unix(int64(v), 0).UTC(), nil
		case float64:
			return time.Unix(int64(v), 0).UTC(), nil
		case sql.NullTime:
			if v.Valid {
				return v.Time, nil
			}
			return time.Now(), nil
		default:
			return time.Time{}, fmt.Errorf("failed to convert timestamp column value: '%v' - unsupported type: %T", val, val)
		}
	} else {
		return time.Now(), nil
	}
}
