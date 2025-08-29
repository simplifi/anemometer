package monitor

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	mock_statsd "github.com/DataDog/datadog-go/v5/statsd/mocks"
	"github.com/golang/mock/gomock"
	_ "github.com/mattn/go-sqlite3"
	"github.com/simplifi/anemometer/pkg/anemometer/config"
	"github.com/stretchr/testify/assert"
)

func TestMonitorNew(t *testing.T) {
	testStatsdConfig := config.StatsdConfig{
		Address: "localhost:8125",
	}

	testDatabaseConfig := config.DatabaseConfig{
		Type: "sqlite3",
		URI:  ":memory:",
	}

	testMonitorCfg := config.MonitorConfig{
		Name:           "test-monitor",
		DatabaseConfig: testDatabaseConfig,
		SleepDuration:  100,
		Metric:         "my.test.metric",
		SQL:            "SELECT 100 AS metric, 'tag' AS my_tag",
	}

	monitor, err := New(testStatsdConfig, testMonitorCfg)

	assert.NoError(t, err)
	assert.NotNil(t, monitor)
}

// Comprehensive integration test using SQLite + Mock StatsD
func TestMonitorIntegration(t *testing.T) {
	// Test different metric types
	testCases := []struct {
		name       string
		metricType string
		sqlQuery   string
		setupMock  func(*mock_statsd.MockClientInterface)
	}{
		{
			name:       "count-metric",
			metricType: "count",
			sqlQuery:   "SELECT 42 AS metric, 'us-east' AS region",
			setupMock: func(m *mock_statsd.MockClientInterface) {
				m.EXPECT().CountWithTimestamp("app.test.count-metric", int64(42), []string{"region:us-east"}, float64(1), gomock.Any()).Return(nil)
			},
		},
		{
			name:       "gauge-metric",
			metricType: "gauge",
			sqlQuery:   "SELECT 85.5 AS metric, 'premium' AS tier",
			setupMock: func(m *mock_statsd.MockClientInterface) {
				m.EXPECT().GaugeWithTimestamp("app.test.gauge-metric", 85.5, []string{"tier:premium"}, float64(1), gomock.Any()).Return(nil)
			},
		},
		{
			name:       "histogram-metric",
			metricType: "histogram",
			sqlQuery:   "SELECT 95.0 AS metric, 'all' AS segment",
			setupMock: func(m *mock_statsd.MockClientInterface) {
				m.EXPECT().Histogram("app.test.histogram-metric", 95.0, []string{"segment:all"}, float64(1)).Return(nil)
			},
		},
		{
			name:       "distribution-metric",
			metricType: "distribution",
			sqlQuery:   "SELECT 75.25 AS metric, 'baseline' AS category",
			setupMock: func(m *mock_statsd.MockClientInterface) {
				m.EXPECT().Distribution("app.test.distribution-metric", 75.25, []string{"category:baseline"}, float64(1)).Return(nil)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStatsD := mock_statsd.NewMockClientInterface(ctrl)
			tc.setupMock(mockStatsD)

			databaseConn, err := createDBConn("sqlite3", ":memory:")
			assert.NoError(t, err)
			defer databaseConn.Close()

			monitor := &Monitor{
				databaseConn:  databaseConn,
				statsdClient:  mockStatsD,
				name:          tc.name,
				sleepDuration: 100,
				metric:        "app.test." + tc.name,
				metricType:    tc.metricType,
				sql:           tc.sqlQuery,
			}

			// Execute query and send metric (simulates one monitor cycle)
			rows, err := monitor.databaseConn.Query(monitor.sql)
			assert.NoError(t, err)
			defer rows.Close()

			cols, err := rows.Columns()
			assert.NoError(t, err)

			for rows.Next() {
				rowMap, err := rowsToMap(cols, rows)
				assert.NoError(t, err)

				tags := getTags(rowMap)
				err = monitor.sendMetric(rowMap, tags, false)
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetMetricFloat64(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]interface{}
		expected float64
		hasError bool
	}{
		{
			name:     "int_value",
			input:    map[string]interface{}{"metric": 42},
			expected: 42.0,
			hasError: false,
		},
		{
			name:     "int64_value",
			input:    map[string]interface{}{"metric": int64(123456789)},
			expected: 123456789.0,
			hasError: false,
		},
		{
			name:     "float64_value",
			input:    map[string]interface{}{"metric": 3.14159},
			expected: 3.14159,
			hasError: false,
		},
		{
			name:     "float32_value",
			input:    map[string]interface{}{"metric": float32(2.718)},
			expected: float64(float32(2.718)), // Account for float32 precision
			hasError: false,
		},
		{
			name:     "bool_true",
			input:    map[string]interface{}{"metric": true},
			expected: 1.0,
			hasError: false,
		},
		{
			name:     "bool_false",
			input:    map[string]interface{}{"metric": false},
			expected: 0.0,
			hasError: false,
		},
		{
			name:     "missing_metric_column",
			input:    map[string]interface{}{"other_column": 42},
			expected: 0.0,
			hasError: true,
		},
		{
			name:     "string_value_should_error",
			input:    map[string]interface{}{"metric": "not_a_number"},
			expected: 0.0,
			hasError: true,
		},
		{
			name:     "negative_int",
			input:    map[string]interface{}{"metric": -42},
			expected: -42.0,
			hasError: false,
		},
		{
			name:     "zero_value",
			input:    map[string]interface{}{"metric": 0},
			expected: 0.0,
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := getMetricFloat64(tt.input)

			if tt.hasError {
				assert.Error(t, err, "Expected error but got none")
				return
			}

			assert.NoError(t, err, "Unexpected error: %v", err)
			assert.Equal(t, tt.expected, result, "Expected %f, got %f", tt.expected, result)
		})
	}
}

func TestGetTags(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]interface{}
		expected []string
	}{
		{
			name: "simple_tags",
			input: map[string]interface{}{
				"metric":      42,
				"environment": "production",
				"service":     "web",
			},
			expected: []string{"environment:production", "service:web"},
		},
		{
			name: "mixed_types",
			input: map[string]interface{}{
				"metric":  3.14,
				"count":   123,
				"enabled": true,
				"region":  "us-east-1",
			},
			expected: []string{"count:123", "enabled:true", "region:us-east-1"},
		},
		{
			name: "only_metric_column",
			input: map[string]interface{}{
				"metric": 1,
			},
			expected: []string{},
		},
		{
			name:     "empty_map",
			input:    map[string]interface{}{},
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getTags(tt.input)
			assert.ElementsMatch(t, tt.expected, result)
		})
	}
}

func TestGetTimestamp(t *testing.T) {
	// Fixed timestamp for testing
	expectedTime := time.Date(2023, 12, 25, 10, 30, 0, 0, time.UTC)
	expectedTimeString := expectedTime.Format(time.RFC3339)
	unixTimestamp := expectedTime.Unix()

	tests := []struct {
		name     string
		input    map[string]interface{}
		expected time.Time
		hasError bool
	}{
		// String timestamp tests
		{
			name: "valid_rfc3339_timestamp_string",
			input: map[string]interface{}{
				"metric":    42,
				"timestamp": expectedTimeString,
			},
			expected: expectedTime,
			hasError: false,
		},
		{
			name: "empty_timestamp_string",
			input: map[string]interface{}{
				"metric":    42,
				"timestamp": "",
			},
			expected: time.Time{},
			hasError: true,
		},
		{
			name: "invalid_timestamp_format_string",
			input: map[string]interface{}{
				"metric":    42,
				"timestamp": "2023-12-25 10:30:00", // Not RFC3339
			},
			expected: time.Time{},
			hasError: true,
		},

		// time.Time timestamp tests
		{
			name: "actual_time_type",
			input: map[string]interface{}{
				"metric":    42,
				"timestamp": expectedTime,
			},
			expected: expectedTime,
			hasError: false,
		},

		// int64 unix timestamp tests
		{
			name: "unix_timestamp_int64",
			input: map[string]interface{}{
				"metric":    42,
				"timestamp": unixTimestamp,
			},
			expected: time.Unix(unixTimestamp, 0).UTC(),
			hasError: false,
		},
		{
			name: "zero_unix_timestamp",
			input: map[string]interface{}{
				"metric":    42,
				"timestamp": int64(0),
			},
			expected: time.Unix(0, 0).UTC(),
			hasError: false,
		},

		// sql.NullTime tests
		{
			name: "valid_null_time",
			input: map[string]interface{}{
				"metric":    42,
				"timestamp": sql.NullTime{Time: expectedTime, Valid: true},
			},
			expected: expectedTime,
			hasError: false,
		},
		{
			name: "invalid_null_time_uses_now",
			input: map[string]interface{}{
				"metric":    42,
				"timestamp": sql.NullTime{Time: time.Time{}, Valid: false},
			},
			expected: time.Time{}, // We'll check this is close to time.Now()
			hasError: false,
		},

		// Numeric unix timestamp tests
		{
			name: "unix_timestamp_int32",
			input: map[string]interface{}{
				"metric":    42,
				"timestamp": int32(unixTimestamp),
			},
			expected: time.Unix(unixTimestamp, 0).UTC(),
			hasError: false,
		},
		{
			name: "unix_timestamp_int",
			input: map[string]interface{}{
				"metric":    42,
				"timestamp": int(unixTimestamp),
			},
			expected: time.Unix(unixTimestamp, 0).UTC(),
			hasError: false,
		},
		{
			name: "unix_timestamp_float64",
			input: map[string]interface{}{
				"metric":    42,
				"timestamp": float64(unixTimestamp),
			},
			expected: time.Unix(unixTimestamp, 0).UTC(),
			hasError: false,
		},
		// Truly unsupported type tests
		{
			name: "unsupported_type_float32",
			input: map[string]interface{}{
				"metric":    42,
				"timestamp": float32(unixTimestamp),
			},
			expected: time.Time{},
			hasError: true,
		},
		{
			name: "unsupported_type_bool",
			input: map[string]interface{}{
				"metric":    42,
				"timestamp": true,
			},
			expected: time.Time{},
			hasError: true,
		},
		{
			name: "unsupported_type_slice",
			input: map[string]interface{}{
				"metric":    42,
				"timestamp": []int{1, 2, 3},
			},
			expected: time.Time{},
			hasError: true,
		},

		// Missing timestamp column
		{
			name: "missing_timestamp_column_uses_now",
			input: map[string]interface{}{
				"metric": 42,
			},
			expected: time.Time{}, // We'll check this is close to time.Now()
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := getTimestamp(tt.input)

			if tt.hasError {
				assert.Error(t, err, "Expected error but got none")
				return
			}

			assert.NoError(t, err, "Unexpected error: %v", err)

			if tt.name == "missing_timestamp_column_uses_now" || tt.name == "invalid_null_time_uses_now" {
				// Check that the timestamp is close to now (within 1 second)
				now := time.Now()
				timeDiff := now.Sub(result)
				if timeDiff < 0 {
					timeDiff = -timeDiff
				}
				assert.True(t, timeDiff < time.Second, "Timestamp should be close to now, got %v", result)
			} else {
				assert.Equal(t, tt.expected, result, "Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// Test timestamp-specific functionality with integration tests
func TestMonitorIntegrationWithTimestamp(t *testing.T) {
	expectedTime := time.Date(2023, 12, 25, 10, 30, 0, 0, time.UTC)
	expectedTimeString := expectedTime.Format(time.RFC3339)

	testCases := []struct {
		name       string
		metricType string
		sqlQuery   string
		setupMock  func(*mock_statsd.MockClientInterface)
	}{
		{
			name:       "count-with-explicit-timestamp",
			metricType: "count",
			sqlQuery:   fmt.Sprintf("SELECT 100 AS metric, '%s' AS timestamp, 'prod' AS environment", expectedTimeString),
			setupMock: func(m *mock_statsd.MockClientInterface) {
				m.EXPECT().CountWithTimestamp("app.test.count-with-explicit-timestamp", int64(100), []string{"environment:prod"}, float64(1), expectedTime).Return(nil)
			},
		},
		{
			name:       "gauge-with-explicit-timestamp",
			metricType: "gauge",
			sqlQuery:   fmt.Sprintf("SELECT 75.5 AS metric, '%s' AS timestamp, 'staging' AS environment", expectedTimeString),
			setupMock: func(m *mock_statsd.MockClientInterface) {
				m.EXPECT().GaugeWithTimestamp("app.test.gauge-with-explicit-timestamp", 75.5, []string{"environment:staging"}, float64(1), expectedTime).Return(nil)
			},
		},
		{
			name:       "count-without-timestamp-uses-now",
			metricType: "count",
			sqlQuery:   "SELECT 50 AS metric, 'dev' AS environment",
			setupMock: func(m *mock_statsd.MockClientInterface) {
				m.EXPECT().CountWithTimestamp("app.test.count-without-timestamp-uses-now", int64(50), []string{"environment:dev"}, float64(1), gomock.Any()).Return(nil)
			},
		},
		{
			name:       "gauge-without-timestamp-uses-now",
			metricType: "gauge",
			sqlQuery:   "SELECT 25.25 AS metric, 'test' AS environment",
			setupMock: func(m *mock_statsd.MockClientInterface) {
				m.EXPECT().GaugeWithTimestamp("app.test.gauge-without-timestamp-uses-now", 25.25, []string{"environment:test"}, float64(1), gomock.Any()).Return(nil)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStatsD := mock_statsd.NewMockClientInterface(ctrl)
			tc.setupMock(mockStatsD)

			databaseConn, err := createDBConn("sqlite3", ":memory:")
			assert.NoError(t, err)
			defer databaseConn.Close()

			monitor := &Monitor{
				databaseConn:  databaseConn,
				statsdClient:  mockStatsD,
				name:          tc.name,
				sleepDuration: 100,
				metric:        "app.test." + tc.name,
				metricType:    tc.metricType,
				sql:           tc.sqlQuery,
			}

			// Execute query and send metric (simulates one monitor cycle)
			rows, err := monitor.databaseConn.Query(monitor.sql)
			assert.NoError(t, err)
			defer rows.Close()

			cols, _ := rows.Columns()

			for rows.Next() {
				rowMap, err := rowsToMap(cols, rows)
				assert.NoError(t, err)

				tags := getTags(rowMap)
				err = monitor.sendMetric(rowMap, tags, false)
				assert.NoError(t, err)
			}
		})
	}
}

func TestMetricTypeHandling(t *testing.T) {
	tests := []struct {
		name       string
		metricType string
		expectErr  bool
	}{
		{name: "gauge_type", metricType: "gauge", expectErr: false},
		{name: "count_type", metricType: "count", expectErr: false},
		{name: "histogram_type", metricType: "histogram", expectErr: false},
		{name: "distribution_type", metricType: "distribution", expectErr: false},
		{name: "unknown_type", metricType: "unknown", expectErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock StatsD client
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStatsD := mock_statsd.NewMockClientInterface(ctrl)

			// Set up mock expectations for valid metric types
			if !tt.expectErr {
				switch tt.metricType {
				case "gauge":
					mockStatsD.EXPECT().GaugeWithTimestamp("test.metric", 42.0, []string{"environment:test"}, float64(1), gomock.Any()).Return(nil)
				case "count":
					mockStatsD.EXPECT().CountWithTimestamp("test.metric", int64(42), []string{"environment:test"}, float64(1), gomock.Any()).Return(nil)
				case "histogram":
					mockStatsD.EXPECT().Histogram("test.metric", 42.0, []string{"environment:test"}, float64(1)).Return(nil)
				case "distribution":
					mockStatsD.EXPECT().Distribution("test.metric", 42.0, []string{"environment:test"}, float64(1)).Return(nil)
				}
			}

			monitor := &Monitor{
				name:         "test",
				metric:       "test.metric",
				metricType:   tt.metricType,
				statsdClient: mockStatsD,
			}

			rowMap := map[string]interface{}{
				"metric":      42.0,
				"environment": "test",
			}
			tags := []string{"environment:test"}

			err := monitor.sendMetric(rowMap, tags, false)

			if tt.expectErr {
				assert.Error(t, err, "Expected error for unknown metric type")
				assert.Contains(t, err.Error(), "unknown metric type", "Error should mention unknown metric type")
			} else {
				assert.NoError(t, err, "Should not error for known metric type")
			}
		})
	}
}
