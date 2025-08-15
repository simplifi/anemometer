package monitor

import (
	"testing"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	_ "github.com/mattn/go-sqlite3"
	"github.com/simplifi/anemometer/pkg/anemometer/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockStatsDClient is a testify mock implementation of statsd.ClientInterface
type MockStatsDClient struct {
	mock.Mock
}

func (m *MockStatsDClient) Gauge(name string, value float64, tags []string, rate float64) error {
	args := m.Called(name, value, tags, rate)
	return args.Error(0)
}

func (m *MockStatsDClient) Count(name string, value int64, tags []string, rate float64) error {
	args := m.Called(name, value, tags, rate)
	return args.Error(0)
}

func (m *MockStatsDClient) Histogram(name string, value float64, tags []string, rate float64) error {
	args := m.Called(name, value, tags, rate)
	return args.Error(0)
}

func (m *MockStatsDClient) Distribution(name string, value float64, tags []string, rate float64) error {
	args := m.Called(name, value, tags, rate)
	return args.Error(0)
}

// Minimal implementations for unused methods to satisfy the interface
func (m *MockStatsDClient) Decr(name string, tags []string, rate float64) error {
	args := m.Called(name, tags, rate)
	return args.Error(0)
}

func (m *MockStatsDClient) Incr(name string, tags []string, rate float64) error {
	args := m.Called(name, tags, rate)
	return args.Error(0)
}

func (m *MockStatsDClient) Set(name string, value string, tags []string, rate float64) error {
	args := m.Called(name, value, tags, rate)
	return args.Error(0)
}

func (m *MockStatsDClient) Timing(name string, value time.Duration, tags []string, rate float64) error {
	args := m.Called(name, value, tags, rate)
	return args.Error(0)
}

func (m *MockStatsDClient) TimeInMilliseconds(name string, value float64, tags []string, rate float64) error {
	args := m.Called(name, value, tags, rate)
	return args.Error(0)
}

func (m *MockStatsDClient) Event(e *statsd.Event) error {
	args := m.Called(e)
	return args.Error(0)
}

func (m *MockStatsDClient) SimpleEvent(title, text string) error {
	args := m.Called(title, text)
	return args.Error(0)
}

func (m *MockStatsDClient) ServiceCheck(sc *statsd.ServiceCheck) error {
	args := m.Called(sc)
	return args.Error(0)
}

func (m *MockStatsDClient) SimpleServiceCheck(name string, status statsd.ServiceCheckStatus) error {
	args := m.Called(name, status)
	return args.Error(0)
}

func (m *MockStatsDClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockStatsDClient) Flush() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockStatsDClient) SetWriteTimeout(d time.Duration) error {
	args := m.Called(d)
	return args.Error(0)
}

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
		setupMock  func(*MockStatsDClient)
	}{
		{
			name:       "count-metric",
			metricType: "count",
			sqlQuery:   "SELECT 42 AS metric, 'us-east' AS region",
			setupMock: func(m *MockStatsDClient) {
				m.On("Count", "app.test.count-metric", int64(42), []string{"region:us-east"}, float64(1)).Return(nil)
			},
		},
		{
			name:       "gauge-metric",
			metricType: "gauge",
			sqlQuery:   "SELECT 85.5 AS metric, 'premium' AS tier",
			setupMock: func(m *MockStatsDClient) {
				m.On("Gauge", "app.test.gauge-metric", 85.5, []string{"tier:premium"}, float64(1)).Return(nil)
			},
		},
		{
			name:       "histogram-metric",
			metricType: "histogram",
			sqlQuery:   "SELECT 95.0 AS metric, 'all' AS segment",
			setupMock: func(m *MockStatsDClient) {
				m.On("Histogram", "app.test.histogram-metric", 95.0, []string{"segment:all"}, float64(1)).Return(nil)
			},
		},
		{
			name:       "distribution-metric",
			metricType: "distribution",
			sqlQuery:   "SELECT 75.25 AS metric, 'baseline' AS category",
			setupMock: func(m *MockStatsDClient) {
				m.On("Distribution", "app.test.distribution-metric", 75.25, []string{"category:baseline"}, float64(1)).Return(nil)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockStatsD := &MockStatsDClient{}
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

			mockStatsD.AssertExpectations(t)
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

			assert.Equal(t, len(tt.expected), len(result), "Expected %d tags, got %d", len(tt.expected), len(result))

			// Check that all expected tags are present (order doesn't matter)
			expectedMap := make(map[string]bool)
			for _, tag := range tt.expected {
				expectedMap[tag] = true
			}

			for _, tag := range result {
				assert.True(t, expectedMap[tag], "Unexpected tag: %s", tag)
				delete(expectedMap, tag)
			}

			assert.Equal(t, 0, len(expectedMap), "Missing expected tags: %v", expectedMap)
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
			mockStatsD := &MockStatsDClient{}

			// Set up mock expectations for valid metric types
			if !tt.expectErr {
				switch tt.metricType {
				case "gauge":
					mockStatsD.On("Gauge", "test.metric", 42.0, []string{"environment:test"}, float64(1)).Return(nil)
				case "count":
					mockStatsD.On("Count", "test.metric", int64(42), []string{"environment:test"}, float64(1)).Return(nil)
				case "histogram":
					mockStatsD.On("Histogram", "test.metric", 42.0, []string{"environment:test"}, float64(1)).Return(nil)
				case "distribution":
					mockStatsD.On("Distribution", "test.metric", 42.0, []string{"environment:test"}, float64(1)).Return(nil)
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
				mockStatsD.AssertExpectations(t)
			}
		})
	}
}
