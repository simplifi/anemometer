package monitor

import (
	"testing"

	"github.com/simplifi/anemometer/pkg/anemometer/config"
	"github.com/stretchr/testify/assert"
)

func TestMonitorNew(t *testing.T) {
	testStatsdConfig := config.StatsdConfig{
		Address: "localhost:8125",
	}

	testDatabaseConfig := config.DatabaseConfig{
		Type: "postgres",
		URI:  "postgresql://user:password@localhost:5432/database",
	}

	testMonitorCfg := config.MonitorConfig{
		Name:           "test-monitor",
		DatabaseConfig: testDatabaseConfig,
		SleepDuration:  100,
		Metric:         "my.test.metric",
		SQL:            "select 'tag' AS my_tag, 100 AS metric",
	}

	monitor, err := New(testStatsdConfig, testMonitorCfg)

	assert.NoError(t, err)
	assert.NotNil(t, monitor)
}
