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
}
