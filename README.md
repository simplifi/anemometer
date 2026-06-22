# Anemometer

[![Go Report Card](https://goreportcard.com/badge/github.com/simplifi/anemometer)](https://goreportcard.com/report/github.com/simplifi/anemometer)
[![Release](https://img.shields.io/github/release/simplifi/anemometer.svg)](https://github.com/simplifi/anemometer/releases/latest)

<img src="assets/anemometer.png" width="100">

Anemometer is a tool for running SQL queries and pushing results as metrics and
optional events to Datadog

## Why "Anemometer"

> An anemometer is a device used for measuring wind speed and direction.

This project was originally created to help us monitor some tables in Airflow,
but was later updated so it could work generically with any database.

## Supported Databases

We currently support the following databases:

- Postgres
- Vertica
- BigQuery
- SQLite (mostly for local development and testing)

### Adding support for another database

Support for any of the databases listed [here](https://go.dev/wiki/SQLDrivers)
can be added fairly easily!

- Read [How to contribute](#how-to-contribute)
- Add the driver to
  [`go.mod`](https://github.com/simplifi/anemometer/blob/master/go.mod), and
  ensure [`go.sum`](https://github.com/simplifi/anemometer/blob/master/go.sum)
  gets updated
- Add import for the new driver to
  [`monitor.go`](https://github.com/simplifi/anemometer/blob/master/pkg/anemometer/monitor/monitor.go)
- Update this README to add the new database
- Submit a Pull Request

# Setup

The latest version of Anemometer can be found on the
[Releases](https://github.com/simplifi/anemometer/releases) tab.

## Example Configuration:

```yaml
statsd:
  address: 127.0.0.1:8125
  tags:
    - environment:production
monitors:
  # Gauge metric (default) - values that can go up or down
  - name: airflow-dag-disabled
    database:
      type: postgres
      uri: postgresql://username:password@localhost:5432/database?sslmode=disable
    sleep_duration: 300
    metric: airflow.dag.disabled
    metric_type: gauge # optional - this is the default
    sql: >
      SELECT  dag_id AS dag_id,
              CASE WHEN is_paused AND NOT is_subdag THEN 1 ELSE 0 END AS metric
      FROM    dag

  # Histogram metric - statistical distribution on each host
  - name: airflow-task-queued-seconds
    database:
      type: postgres
      uri: postgresql://username:password@localhost:5432/database?sslmode=disable
    sleep_duration: 300
    metric: airflow.task.queued_seconds
    metric_type: histogram
    sql: >
      SELECT dag_id AS dag_id,
             task_id AS task_id,
             EXTRACT(EPOCH FROM (current_timestamp - queued_dttm)) AS metric
      FROM   task_instance WHERE  state = 'queued'

  # Count metric - track number of events
  - name: failed-tasks-count
    database:
      type: postgres
      uri: postgresql://username:password@localhost:5432/database?sslmode=disable
    sleep_duration: 60
    metric: airflow.task.failed
    metric_type: count
    sql: >
      SELECT dag_id AS dag_id,
             COUNT(*) AS metric
      FROM   task_instance WHERE  state = 'failed'
        AND  end_date > NOW() - INTERVAL '1 hour'
      GROUP BY dag_id

  # Event monitor - emit the configured metric plus one event for each returned row
  - name: postgres-long-running-queries
    database:
      type: postgres
      uri: postgresql://username:password@localhost:5432/database?sslmode=disable
    sleep_duration: 1800
    metric: postgres.long_running_query
    metric_type: gauge
    event:
      enabled: true
      title_column: event_title
      text_column: event_text
      alert_type: warning
      priority: normal
      source_type_name: anemometer
      aggregation_key_column: event_aggregation_key
      tags:
        - alert_type:long_running_query
      tag_columns:
        - database_name
        - duration_bucket
    sql: >
      SELECT 1 AS metric,
             datname AS database_name,
             CASE
               WHEN now() - query_start > interval '6 hours' THEN '6h_plus'
               WHEN now() - query_start > interval '4 hours' THEN '4h_plus'
               ELSE '2h_plus'
             END AS duration_bucket,
             'Long running Postgres query' AS event_title,
             'Database: ' || datname || E'\n' ||
             'User: ' || usename || E'\n' ||
             'PID: ' || pid || E'\n' ||
             'Runtime: ' || (now() - query_start)::text || E'\n\n' ||
             query AS event_text,
             'postgres-long-running-query:' || datname || ':' || pid AS event_aggregation_key
      FROM pg_stat_activity
      WHERE state = 'active'
        AND now() - query_start > interval '2 hours'
```

### `statsd`

This is where you tell Anemometer where to send StatsD metrics

- `address` - The address:port on which StatsD is listening (usually
  `127.0.0.1:8125`)
- `tags` - Default tags to send with every metric and event, optional

### `monitors`

This is where you tell Anemometer about the monitor(s) configuration

- `name` - The name of this monitor, mainly used in logging
- `database.type` - The type of database connection to be used (`postgres`,
  `vertica`, `bigquery`, and `sqlite3` are currently supported)
- `database.uri` - The URI connection string used to connect to the database
  (usually follows `protocol://username:password@hostname:port/database`)
- `sleep_duration` - How long to wait between pushes to StatsD (in seconds)
- `metric` - The name of the metric to be sent to StatsD
- `metric_type` - The type of metric to send to Datadog (optional, defaults to
  `gauge`)
- `event` - Optional Datadog event configuration. When enabled, one event is sent
  for each row returned by the SQL query.
- `sql` - The SQL query to execute when populating the metric's values/tags (see
  [SQL Query Structure](#sql-query-structure))

## SQL Query Structure

Anemometer makes the following assumptions about the results of your query:

- Exactly one column will be named `metric`, and the value is convertable to
  `float64` (no strings)
- An optional column named `timestamp` can be included to explicitly provide a timestamp for the metrics (only supported for `gauge` and `count` types)
- All other columns will be aggregated into tags and sent to StatsD
- The tags will take the form of `column_name:value`
- Event payload columns such as `event_title`, `event_text`,
  `event_aggregation_key`, and configured title/text/aggregation/hostname
  columns are not used as metric tags by default.

## Metric Types

Anemometer supports all four Datadog metric types. You can specify the metric
type using the `metric_type` configuration option:

### Gauge (default)

- **Use case**: Values that can go up or down (e.g., CPU usage, memory usage,
  queue depth)
- **Configuration**: `metric_type: gauge` (or omit for default)
- **StatsD format**: `metric_name:value|g|#tags`
- **Timestamp support**: ✅ Supports optional `timestamp` column

### Count

- **Use case**: Track how many times something happened (e.g., requests, errors,
  events)
- **Configuration**: `metric_type: count`
- **StatsD format**: `metric_name:value|c|#tags`
- **Note**: Values are converted to integers
- **Timestamp support**: ✅ Supports optional `timestamp` column

### Histogram

- **Use case**: Track statistical distribution of values on each host (e.g.,
  request latency, file sizes)
- **Configuration**: `metric_type: histogram`
- **StatsD format**: `metric_name:value|h|#tags`
- **Timestamp support**: ❌ Uses current time only

### Distribution

- **Use case**: Track statistical distribution of values across your
  infrastructure (e.g., request latency across all hosts)
- **Configuration**: `metric_type: distribution`
- **StatsD format**: `metric_name:value|d|#tags`
- **Timestamp support**: ❌ Uses current time only

**Note**: The `metric_type` field is optional and defaults to `gauge` for
backwards compatibility. Existing configurations will continue to work without
any changes.

## Event Support

Anemometer can also send Datadog events through DogStatsD. This is useful for
alert conditions where the SQL query itself controls whether anything should be
emitted. For example, a long-running-query monitor can return only sessions that
have been active for more than two hours. If the query returns no rows, no events
are sent. If the query returns three rows, three events are sent.

Events are sent in addition to the configured metric. Event-enabled monitors
still need a `metric` setting and a SQL `metric` column.

The monitor's `sleep_duration` controls how often Anemometer re-checks the query
and therefore how often a still-true condition can re-notify through a Datadog
event monitor.

### Event configuration

- `enabled` - Set to `true` to send one event for each returned SQL row
- `title` - Static event title. Used when `title_column` is not configured
- `title_column` - SQL result column containing the event title
- `text` - Static event body. Used when `text_column` is not configured
- `text_column` - SQL result column containing the event body
- `alert_type` - Event type: `info`, `warning`, `error`, or `success` (defaults
  to `info`)
- `priority` - Event priority: `normal` or `low` (defaults to `normal`)
- `source_type_name` - Event source type (defaults to `anemometer`)
- `aggregation_key` - Static key used by Datadog to group related events
- `aggregation_key_column` - SQL result column containing the aggregation key
- `hostname` - Static hostname for the event
- `hostname_column` - SQL result column containing the hostname
- `tags` - Static event-only tags
- `tag_columns` - SQL result columns to use as event-only tags

Static `event.tags` are sent only with events. Use `event.tag_columns` for
low-cardinality SQL result fields that should be available to Datadog event
monitor queries and notification templates. Put high-cardinality details such as
PID, exact runtime, client address, and query text in `event_text` instead of
tags.

## Timestamp Support

Anemometer supports custom timestamps for `gauge` and `count` metrics by including an optional `timestamp` column in your SQL query results. This allows you to send metrics with specific timestamps rather than using the current time.

### Supported timestamp formats:

- **RFC3339 strings**: `"2023-12-25T10:30:00Z"`
- **Unix timestamps**: `1703505000` (as `int64`, `int32`, `int`, or `float64`)
- **Database time objects**: Direct `time.Time` or `sql.NullTime` values

### Example with timestamp:

```sql
SELECT 'production' AS environment,
       COUNT(*) AS metric,
       '2023-12-25T10:30:00Z' AS timestamp
FROM   users
WHERE  created_at BETWEEN '2023-12-25 00:00:00' AND '2023-12-25 23:59:59'
```

### Important Datadog considerations:

⚠️ **Historical Data Limitations**: By default, Datadog will reject metrics with timestamps older than 4 hours from the current time. To send historical metrics, you need to enable "Allow metrics with timestamps in past" in your Datadog organization settings.

📖 For more information, see [Datadog's documentation on historical metric in gestion](https://docs.datadoghq.com/metrics/custom_metrics/historical_metrics/).

### Query Example

#### Single row result

To monitor the number of records in your user's table you might do something
like this:

```SQL
SELECT 'production' AS environment,
       'users' AS table_name,
       COUNT(0) AS metric
FROM   users
```

Resulting in the following:

```
 environment | table_name | metric
-------------+------------+--------
 production  | users      |     99
```

Assuming we named our metric `table.records` with `metric_type: gauge`, this
would result in the following data being sent to StatsD:
`table.records:99|g|#environment:production,table_name:users`

#### Multiple row result

To monitor the number of queries each user is running in your database you might
do something like this:

```SQL
SELECT   'production' AS environment,
         usename AS user_name,
         COUNT(0) AS metric
FROM     pg_stat_activity
WHERE    query != '<IDLE>'
GROUP BY usename
```

Resulting in the following:

```
 environment | user_name | metric
-------------+-----------+--------
 production  | cjonesy   |    160
 production  | postgres  |      6
```

Assuming we named our metric `database.queries` with `metric_type: count`, this
would result in the following data being sent to StatsD:
`database.queries:160|c|#environment:production,user_name:cjonesy`
`database.queries:6|c|#environment:production,user_name:postgres`

Notice that one metric is sent for each row in the query.

# Usage

### Basic Usage

```
Anemometer (A SQL -> StatsD metrics generator)

Usage:
  anemometer [command]

Available Commands:
  help        Help about any command
  start       Start the Anemometer agent
  version     Print the version number

Flags:
  -h, --help   help for anemometer

Use "anemometer [command] --help" for more information about a command.
```

### To start the agent:

```shell script
anemometer start -c /path/to/your/config.yml
```

### Using Docker

You can run Anemometer using Docker with the image from GitHub Container
Registry:

```shell script
docker run -v /path/to/your/config.yml:/config.yml ghcr.io/simplifi/anemometer start -c /config.yml
```

This mounts your local configuration file into the container and runs the start
command.

# Development

### Testing locally

If you want to test this out locally you can run the following to start
Anemometer:

```shell script
anemometer start -c /path/to/config.yml
```

You can see the metrics that would be sent by watching the statsd port on
localhost:

```shell script
nc -u -l 8125
```

### Compiling

```shell script
make build
```

### Running Tests

To run all the standard tests:

```shell script
make test
```

### Releasing

This project is using [goreleaser](https://goreleaser.com). GitHub release
creation is automated using Travis CI. New releases are automatically created
when new tags are pushed to the repo.

```shell script
$ TAG=0.1.0 make tag
```

## How to contribute

This project has some clear Contribution Guidelines and expectations that you
can read here ([CONTRIBUTING](CONTRIBUTING.md)).

The contribution guidelines outline the process that you'll need to follow to
get a patch merged.

And you don't just have to write code. You can help out by writing
documentation, tests, or even by giving feedback about this work.

Thank you for contributing!
