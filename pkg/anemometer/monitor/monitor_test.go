package monitor

import (
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
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
	expectedTime := time.Date(2023, 12, 25, 10, 30, 0, 0, time.UTC)
	expectedTimeString := expectedTime.Format(time.RFC3339)
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

			monitor.runOnce(false)
		})
	}
}

func TestMonitorIntegrationWithEvent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStatsD := mock_statsd.NewMockClientInterface(ctrl)
	mockStatsD.EXPECT().GaugeWithTimestamp(
		"postgres.long_running_query",
		1.0,
		stringSliceMatcher{expected: []string{"database_name:analytics", "duration_bucket:2h_plus"}},
		float64(1),
		gomock.Any(),
	).Return(nil)
	mockStatsD.EXPECT().Event(statsdEventMatcher{
		expected: statsd.Event{
			Title:          "Long running Postgres query",
			Text:           "Database: analytics\nUser: reporting_user\nPID: 41273",
			AggregationKey: "postgres-long-running-query:analytics:41273",
			Priority:       statsd.Normal,
			SourceTypeName: "anemometer",
			AlertType:      statsd.Warning,
			Tags: []string{
				"alert_type:long_running_query",
				"database_name:analytics",
				"duration_bucket:2h_plus",
			},
		},
	}).Return(nil)

	databaseConn, err := createDBConn("sqlite3", ":memory:")
	assert.NoError(t, err)
	defer databaseConn.Close()

	monitor := &Monitor{
		databaseConn:  databaseConn,
		statsdClient:  mockStatsD,
		name:          "postgres-long-running-queries",
		sleepDuration: 100,
		metric:        "postgres.long_running_query",
		metricType:    "gauge",
		eventConfig: config.EventConfig{
			Enabled:              true,
			TitleColumn:          "event_title",
			TextColumn:           "event_text",
			AlertType:            "warning",
			Priority:             "normal",
			SourceTypeName:       "anemometer",
			AggregationKeyColumn: "event_aggregation_key",
			Tags:                 []string{"alert_type:long_running_query"},
			TagColumns:           []string{"database_name", "duration_bucket"},
		},
		sql: `
			SELECT 1 AS metric,
			       'analytics' AS database_name,
			       '2h_plus' AS duration_bucket,
			       'Long running Postgres query' AS event_title,
			       'Database: analytics
User: reporting_user
PID: 41273' AS event_text,
			       'postgres-long-running-query:analytics:41273' AS event_aggregation_key
		`,
	}

	monitor.runOnce(false)
}

func TestProcessRowMetricFailureDoesNotSkipEvent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStatsD := mock_statsd.NewMockClientInterface(ctrl)
	mockStatsD.EXPECT().GaugeWithTimestamp("postgres.long_running_query", 1.0, []string{"database_name:analytics"}, float64(1), gomock.Any()).Return(fmt.Errorf("metric send failed"))
	mockStatsD.EXPECT().Gauge("anemometer.error", 1.0, []string{"name:postgres-long-running-queries"}, float64(1)).Return(nil)
	mockStatsD.EXPECT().Event(statsdEventMatcher{
		expected: statsd.Event{
			Title:          "Long running Postgres query",
			Text:           "Database: analytics",
			AggregationKey: "postgres-long-running-query:analytics:41273",
			Priority:       statsd.Normal,
			SourceTypeName: "anemometer",
			AlertType:      statsd.Warning,
			Tags:           []string{"database_name:analytics"},
		},
	}).Return(nil)

	monitor := &Monitor{
		statsdClient: mockStatsD,
		name:         "postgres-long-running-queries",
		metric:       "postgres.long_running_query",
		metricType:   "gauge",
		eventConfig: config.EventConfig{
			Enabled:              true,
			Title:                "Long running Postgres query",
			TextColumn:           "event_text",
			AlertType:            "warning",
			Priority:             "normal",
			SourceTypeName:       "anemometer",
			AggregationKeyColumn: "event_aggregation_key",
			TagColumns:           []string{"database_name"},
		},
	}

	monitor.processRow(map[string]interface{}{
		"metric":                1,
		"database_name":         "analytics",
		"event_text":            "Database: analytics",
		"event_aggregation_key": "postgres-long-running-query:analytics:41273",
	}, false)
}

func TestMonitorNewRejectsInvalidEventConfig(t *testing.T) {
	tests := []struct {
		name        string
		eventConfig config.EventConfig
		expectedErr string
	}{
		{
			name: "invalid_alert_type",
			eventConfig: config.EventConfig{
				Enabled:   true,
				AlertType: "user_update",
			},
			expectedErr: "unknown event alert type: user_update",
		},
		{
			name: "invalid_priority",
			eventConfig: config.EventConfig{
				Enabled:  true,
				Priority: "high",
			},
			expectedErr: "unknown event priority: high",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			monitor, err := New(config.StatsdConfig{}, config.MonitorConfig{
				EventConfig: tt.eventConfig,
			})

			assert.Nil(t, monitor)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErr)
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

func TestGetMetricTagsWithEventConfig(t *testing.T) {
	rowMap := map[string]interface{}{
		"metric":                1,
		"timestamp":             "2023-12-25T10:30:00Z",
		"event_title":           "Long running Postgres query",
		"event_text":            "PID: 41273",
		"event_aggregation_key": "postgres-long-running-query:analytics:41273",
		"database_name":         "analytics",
		"duration_bucket":       "2h_plus",
		"pid":                   41273,
		"user_name":             "reporting_user",
	}

	monitor := &Monitor{
		eventConfig: config.EventConfig{
			Enabled:              true,
			TitleColumn:          "event_title",
			TextColumn:           "event_text",
			AggregationKeyColumn: "event_aggregation_key",
			TagColumns:           []string{"database_name", "duration_bucket"},
		},
	}

	assert.ElementsMatch(t, []string{
		"database_name:analytics",
		"duration_bucket:2h_plus",
		"pid:41273",
		"user_name:reporting_user",
	}, monitor.getMetricTags(rowMap))
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

func TestEventTypeHandling(t *testing.T) {
	tests := []struct {
		name      string
		alertType string
		priority  string
		expectErr bool
	}{
		{name: "defaults", alertType: "", priority: "", expectErr: false},
		{name: "warning_normal", alertType: "warning", priority: "normal", expectErr: false},
		{name: "success_low", alertType: "success", priority: "low", expectErr: false},
		{name: "unknown_alert_type", alertType: "user_update", priority: "normal", expectErr: true},
		{name: "unknown_priority", alertType: "warning", priority: "high", expectErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStatsD := mock_statsd.NewMockClientInterface(ctrl)
			if !tt.expectErr {
				mockStatsD.EXPECT().Event(gomock.Any()).Return(nil)
			}

			monitor := &Monitor{
				name:         "test-event-monitor",
				statsdClient: mockStatsD,
				eventConfig: config.EventConfig{
					Enabled:    true,
					Title:      "Test event",
					Text:       "Test event body",
					AlertType:  tt.alertType,
					Priority:   tt.priority,
					TagColumns: []string{},
				},
			}

			err := monitor.sendEvent(map[string]interface{}{}, []string{}, false)

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

type statsdEventMatcher struct {
	expected statsd.Event
}

func (m statsdEventMatcher) Matches(value interface{}) bool {
	event, ok := value.(*statsd.Event)
	if !ok {
		return false
	}

	return event.Title == m.expected.Title &&
		event.Text == m.expected.Text &&
		event.Timestamp.Equal(m.expected.Timestamp) &&
		event.Hostname == m.expected.Hostname &&
		event.AggregationKey == m.expected.AggregationKey &&
		event.Priority == m.expected.Priority &&
		event.SourceTypeName == m.expected.SourceTypeName &&
		event.AlertType == m.expected.AlertType &&
		reflect.DeepEqual(event.Tags, m.expected.Tags)
}

func (m statsdEventMatcher) String() string {
	return fmt.Sprintf("matches statsd event %+v", m.expected)
}

type stringSliceMatcher struct {
	expected []string
}

func (m stringSliceMatcher) Matches(value interface{}) bool {
	actual, ok := value.([]string)
	if !ok {
		return false
	}

	if len(actual) != len(m.expected) {
		return false
	}

	counts := make(map[string]int, len(m.expected))
	for _, value := range m.expected {
		counts[value]++
	}
	for _, value := range actual {
		counts[value]--
		if counts[value] < 0 {
			return false
		}
	}

	return true
}

func (m stringSliceMatcher) String() string {
	return fmt.Sprintf("matches string slice %v", m.expected)
}
