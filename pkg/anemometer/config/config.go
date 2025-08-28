package config

import (
	"os"
	"strings"

	"github.com/spf13/viper"
)

/*
Example configuration file:
---
statsd:
  address: 127.0.0.1:8125
monitors:
  - name: example-monitor
    database:
      type: postgres
      uri: postgresql://username:password@localhost:5432/database
    sleep_duration: 300
    metric: database.queries
    metric_type: gauge
    sql: >
      SELECT    'production' AS environment,
                usename AS user_name,
                COUNT(0) AS metric
      FROM      pg_stat_activity
      WHERE     query != '<IDLE>'
      GROUP BY  usename
*/

// Config is used to store configuration for the Monitors
type Config struct {
	StatsdConfig StatsdConfig    `mapstructure:"statsd"`
	Monitors     []MonitorConfig `mapstructure:"monitors"`
}

// StatsdConfig holds statsd specific configuration
type StatsdConfig struct {
	Address string   `mapstructure:"address"`
	Tags    []string `mapstructure:"tags"`
}

// DatabaseConfig holds database connection specific configuration
type DatabaseConfig struct {
	Type string `mapstructure:"type"`
	URI  string `mapstructure:"uri"`
}

// MonitorConfig holds Monitor specific configuration
type MonitorConfig struct {
	Name           string         `mapstructure:"name"`
	DatabaseConfig DatabaseConfig `mapstructure:"database"`
	SleepDuration  int            `mapstructure:"sleep_duration"`
	Metric         string         `mapstructure:"metric"`
	MetricType     string         `mapstructure:"metric_type"`
	SQL            string         `mapstructure:"sql"`
}

// Read a config file and return a Config
func Read(configPath string) (*Config, error) {
	configFile, readErr := os.Open(configPath)
	if readErr != nil {
		return nil, readErr
	}

	viper.SetConfigType("yaml")
	parseErr := viper.ReadConfig(configFile)
	if parseErr != nil {
		return nil, parseErr
	}

	config := &Config{}

	unmarshalErr := viper.Unmarshal(config)
	if unmarshalErr != nil {
		return nil, unmarshalErr
	}

	// Set default metric types and normalize case for backwards compatibility
	for i := range config.Monitors {
		if config.Monitors[i].MetricType == "" {
			config.Monitors[i].MetricType = "gauge"
		} else {
			config.Monitors[i].MetricType = strings.ToLower(config.Monitors[i].MetricType)
		}
	}

	return config, nil
}
