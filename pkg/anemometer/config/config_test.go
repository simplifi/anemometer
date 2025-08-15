package config

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigRead(t *testing.T) {
	content := []byte(`
---
statsd:
  address: 127.0.0.1:8125
monitors:
  - name: my-test-monitor
    database:
      type: postgres
      uri: postgresql://user:password@localhost:5432/database
    sleep_duration: 300
    metric: my.test.metric
    sql: SELECT 'foo' AS dag_id, 100 AS metric
`)
	tmpfile, _ := ioutil.TempFile("", "config")

	defer os.Remove(tmpfile.Name()) // clean up
	defer tmpfile.Close()
	tmpfile.Write(content)

	cfg, err := Read(tmpfile.Name())
	assert.NoError(t, err)

	// The StatsD address should match
	assert.Equal(t, "127.0.0.1:8125", cfg.StatsdConfig.Address)

	// There should be exactly 1 monitor
	assert.Equal(t, 1, len(cfg.Monitors))

	// The monitor config should match
	assert.Equal(t, "my-test-monitor", cfg.Monitors[0].Name)
	assert.Equal(t, "postgres", cfg.Monitors[0].DatabaseConfig.Type)
	assert.Equal(t, "postgresql://user:password@localhost:5432/database", cfg.Monitors[0].DatabaseConfig.URI)
	assert.Equal(t, 300, cfg.Monitors[0].SleepDuration)
	assert.Equal(t, "my.test.metric", cfg.Monitors[0].Metric)
	assert.Equal(t, "SELECT 'foo' AS dag_id, 100 AS metric", cfg.Monitors[0].SQL)
	// Should default to gauge when no metric_type specified
	assert.Equal(t, "gauge", cfg.Monitors[0].MetricType)
}

func TestMetricTypes(t *testing.T) {
	tests := []struct {
		name          string
		configYAML    string
		expectedTypes []string
		description   string
	}{
		{
			name: "default_gauge",
			configYAML: `
statsd:
  address: 127.0.0.1:8125
monitors:
  - name: test-default
    database:
      type: postgres
      uri: postgresql://test
    sleep_duration: 300
    metric: test.metric
    sql: SELECT 1 as metric`,
			expectedTypes: []string{"gauge"},
			description:   "Missing metric_type should default to gauge",
		},
		{
			name: "case_normalization",
			configYAML: `
statsd:
  address: 127.0.0.1:8125
monitors:
  - name: test-upper
    database:
      type: postgres
      uri: postgresql://test
    sleep_duration: 300
    metric: test.metric
    metric_type: COUNT
    sql: SELECT 1 as metric
  - name: test-mixed
    database:
      type: postgres
      uri: postgresql://test
    sleep_duration: 300
    metric: test.metric
    metric_type: Histogram
    sql: SELECT 1 as metric`,
			expectedTypes: []string{"count", "histogram"},
			description:   "Case should be normalized to lowercase",
		},
		{
			name: "all_valid_types",
			configYAML: `
statsd:
  address: 127.0.0.1:8125
monitors:
  - name: test-gauge
    database:
      type: postgres
      uri: postgresql://test
    sleep_duration: 300
    metric: test.gauge
    metric_type: gauge
    sql: SELECT 1 as metric
  - name: test-count
    database:
      type: postgres
      uri: postgresql://test
    sleep_duration: 300
    metric: test.count
    metric_type: count
    sql: SELECT 1 as metric
  - name: test-histogram
    database:
      type: postgres
      uri: postgresql://test
    sleep_duration: 300
    metric: test.histogram
    metric_type: histogram
    sql: SELECT 1 as metric
  - name: test-distribution
    database:
      type: postgres
      uri: postgresql://test
    sleep_duration: 300
    metric: test.distribution
    metric_type: distribution
    sql: SELECT 1 as metric`,
			expectedTypes: []string{"gauge", "count", "histogram", "distribution"},
			description:   "All valid metric types should be preserved",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temp config file
			tmpfile, err := ioutil.TempFile("", "config")
			assert.NoError(t, err)
			defer os.Remove(tmpfile.Name())
			defer tmpfile.Close()

			_, err = tmpfile.Write([]byte(tt.configYAML))
			assert.NoError(t, err)

			// Read and parse config
			cfg, err := Read(tmpfile.Name())
			assert.NoError(t, err)

			// Check number of monitors
			assert.Equal(t, len(tt.expectedTypes), len(cfg.Monitors))

			// Check each monitor's metric type
			for i, expected := range tt.expectedTypes {
				assert.Equal(t, expected, cfg.Monitors[i].MetricType,
					"Monitor %d: expected type '%s', got '%s'", i, expected, cfg.Monitors[i].MetricType)
			}
		})
	}
}
