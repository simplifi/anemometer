# Anemometer

[![Go Report Card](https://goreportcard.com/badge/github.com/simplifi/anemometer)](https://goreportcard.com/report/github.com/simplifi/anemometer)
[![Release](https://img.shields.io/github/release/simplifi/anemometer.svg)](https://github.com/simplifi/anemometer/releases/latest)

<img src="assets/anemometer.png" width="100">

Anemometer is a tool for running SQL queries and pushing results as metrics to
Datadog

## Why "Anemometer"

> An anemometer is a device used for measuring wind speed and direction.

This project was originally created to help us monitor some tables in Airflow,
but was later updated so it could work generically with any database.

## Supported Databases

We currently support the following databases:

- Postgres
- Vertica
- BigQuery

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
  - name: airflow-dag-disabled
    database:
      type: postgres
      uri: postgresql://username:password@localhost:5432/database?sslmode=disable
    sleep_duration: 300
    metric: airflow.dag.disabled
    sql: >
      SELECT  dag_id AS dag_id,
              CASE WHEN is_paused AND NOT is_subdag THEN 1 ELSE 0 END AS metric
      FROM    dag
  - name: airflow-task-queued-seconds
    database:
      type: postgres
      uri: postgresql://username:password@localhost:5432/database?sslmode=disable
    sleep_duration: 300
    metric: airflow.task.queued_seconds
    sql: >
      SELECT dag_id AS dag_id,
             task_id AS task_id,
             EXTRACT(EPOCH FROM (current_timestamp - queued_dttm)) AS metric
      FROM   task_instance WHERE  state = 'queued'
```

### `statsd`

This is where you tell Anemometer where to send StatsD metrics

- `address` - The address:port on which StatsD is listening (usually
  `127.0.0.1:8125`)
- `tags` - Default tags to send with every metric, optional

### `monitors`

This is where you tell Anemometer about the monitor(s) configuration

- `name` - The name of this monitor, mainly used in logging
- `database.type` - The type of database connection to be used (`postgres` and
  `vertica` are currently supported)
- `database.uri` - The URI connection string used to connect to the database
  (usually follows `protocol://username:password@hostname:port/database`)
- `sleep_duration` - How long to wait between pushes to StatsD (in seconds)
- `metric` - The name of the metric to be sent to StatsD
- `sql` - The SQL query to execute when populating the metric's values/tags (see
  [SQL Query Structure](#sql-query-structure))

## SQL Query Structure

Anemometer makes the following assumptions about the results of your query:

- Exactly one column will be named `metric`, and the value is convertable to
  `float64` (no strings)
- All other columns will be aggregated into tags and sent to StatsD
- The tags will take the form of `column_name:value`

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

Assuming we named our metric `table.records`, this would result in the following
data being sent to StatsD:
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

Assuming we named our metric `database.queries`, this would result in the
following data being sent to StatsD:
`database.queries:160|g|#environment:production,user_name:cjonesy`
`database.queries:6|g|#environment:production,user_name:postgres`

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
