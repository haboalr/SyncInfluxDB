# SyncInfluxDB

[![Check Style](https://github.com/haboalr/SyncInfluxDB/workflows/Check%20Style/badge.svg)](https://github.com/haboalr/SyncInfluxDB/actions)
[![Run Tests](https://github.com/haboalr/SyncInfluxDB/workflows/Run%20Tests/badge.svg)](https://github.com/haboalr/SyncInfluxDB/actions)

Symcon library containing the following modules:

- **Sync InfluxDB** ([documentation](SyncInfluxDB/README.md))
  Transfers the archive of selected variables to InfluxDB 1.x / 2.x / 3.x with metadata as tags. InfluxDB counterpart of [symcon/SyncMySQL](https://github.com/symcon/SyncMySQL).

## Tests

```
git submodule update --init
phpunit tests
```

`tests/SyncInfluxDBTest.php` runs the module against a mock InfluxDB HTTP server (`tests/mock/influx.php`, started automatically via `php -S`).
