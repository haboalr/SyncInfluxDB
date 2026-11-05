# Sync InfluxDB

Transfers the raw archive values of selected Symcon variables to InfluxDB. InfluxDB counterpart of [symcon/SyncMySQL](https://github.com/symcon/SyncMySQL).

### Contents

1. [Features](#1-features)
2. [Requirements](#2-requirements)
3. [Installation](#3-installation)
4. [Configuration](#4-configuration)
5. [Data model in InfluxDB](#5-data-model-in-influxdb)
6. [PHP function reference](#6-php-function-reference)
7. [Differences to SyncMySQL](#7-differences-to-syncmysql)

### 1. Features

* Cyclic transfer of the archived raw values (`AC_GetLoggedValues`) of a configurable list of variables
* InfluxDB 1.x (`/write`), 2.x / Cloud (`/api/v2/write`) and 3 Core / Enterprise (`/api/v3/write_lp`)
* Batched line protocol, optional gzip
* Full history backfill with adaptive read windows (works around the 10.000 row limit of `AC_GetLoggedValues`) and a per-run value budget so a large backfill does not block the timer
* At-least-once delivery: the sync position only advances after InfluxDB acknowledged the write. Re-sent points have the same series key and timestamp and are overwritten by InfluxDB, so no duplicates arise
* Identification, unit, aggregation type, meter, zone, tariff, usage type and free extra tags per variable are written as tags
* Optional BKS identification format (Hochschule Biberach / Smart Energy Box) incl. validation and splitting into individual tags, same groups as the `format` table of SyncMySQL
* Like SyncMySQL: enables archive logging and sets the aggregation type of listed variables (can be switched off), creates links to the synchronized variables below the instance

### 2. Requirements

* Symcon 6.0 or newer
* InfluxDB 1.x, 2.x/Cloud or 3.x reachable via HTTP(S)
* A bucket/database and a token (2.x: write permission on the bucket, read permission for "Test connection")

### 3. Installation

* Module Control → add `https://github.com/haboalr/SyncInfluxDB`
* Add instance → "Sync InfluxDB"

### 4. Configuration

**InfluxDB connection**

| Name                  | Description |
| --------------------- | ----------- |
| Active                | Enables the sync timer |
| InfluxDB version      | 1.x, 2.x / Cloud (also usable for 3.x via its v2-compatible endpoint), 3 Core / Enterprise |
| URL                   | e.g. `http://influxdb:8086` (3.x default port is 8181) |
| Don't verify certificate | Disables TLS certificate checks |
| Organization          | 2.x only |
| Bucket / Database     | Target bucket (2.x) or database (1.x, 3.x). 3.x creates the database on the first write |
| API token             | 2.x / 3.x (`Token` resp. `Bearer` header) |
| Username / Password   | 1.x only (Basic auth) |
| Interval              | Sync interval in seconds |

**Write options**

| Name                  | Description |
| --------------------- | ----------- |
| Default measurement   | Measurement for all variables without an own measurement (default `symcon`) |
| First sync of a new variable | Transfer the complete archive history, or only values logged from now on |
| Identification format | Custom (free text), BKS (validated) or Desigo / BACnet object path |
| Split identification into separate tags | BKS: writes `property`, `ifp_room_designation`, `function_identifier`, `data_point`, ...<br>Desigo: `B'A'Ahu02'FanSu'DP (Differenzdruckfühler)/Present Value` → `building=B`, `plant=A`, `equip=Ahu02`, `component=FanSu`, `point=DP`, `point_path=B'A'Ahu02'FanSu'DP`, `description=Differenzdruckfühler`; dotted members such as `SpTSu.SpErcSu` give `block=SpTSu`, `point=SpErcSu` |
| Lines per write request | Batch size (default 5000) |
| Max. values per sync run | Throttle for backfills; the rest follows in the next run |
| gzip request body     | Compresses write requests |
| Enable archive logging ... | Same behaviour as SyncMySQL |

**Meters / Zones** – master data, referenced from the variable list by meter number / location. Save and reopen the form after adding entries.

**Synchronized variables**

| Column        | Tag / meaning |
| ------------- | ------------- |
| Active        | Only active rows are synchronized |
| Identification | `ident` |
| VariableID    | `variableid` |
| Aggregation Type | `aggregation` = `average` / `counter` (also set in the archive) |
| Unit          | `unit` |
| Meter         | `meter`, `meter_bks` |
| Zone          | `zone`, `economic_unit`, `building_unit` |
| Tariff, Usage Type, Identification (Legacy) | `tariff`, `usage_type`, `ident_legacy` |
| Measurement   | Overrides the default measurement for this variable |
| Extra tags    | `key=value, key=value` – e.g. `brick=urn:bldg#AHU1_SAT, system=AHU1`. Overrides generated tags with the same key |
| Synced until  | Timestamp of the last transferred value |
| State         | OK / Variable missing / Duplicate / Format mismatch / Not logged |

**Add multiple variables** – select a category, instance or variable and add every variable below it in one step (optionally all sub-levels, optionally only already archived ones). The identification is pre-filled with the path relative to the selected object (e.g. `AHU1/Fan/Speed`), the unit is taken from the profile/presentation suffix when it matches a known unit, and the aggregation type from the archive. Variables already in the list and string variables are skipped.

Actions: **Sync now**, **Test connection** (checks credentials and that the bucket/database exists), **Reset sync state** (re-transfer from a point in time, for one or all variables).

### 5. Data model in InfluxDB

One field `value` (always float, booleans as 1.0/0.0), timestamp in seconds, tags as above. Empty tags are omitted.

```
symcon,aggregation=counter,ident=HZG-WMZ01,meter=4711,unit=kWh,variableid=12345,zone=Haus\ D value=18234.5 1730800000
```

Flux example (daily consumption of all counters in a zone):

```flux
from(bucket: "symcon")
  |> range(start: -30d)
  |> filter(fn: (r) => r._measurement == "symcon" and r.aggregation == "counter" and r.zone == "Haus D")
  |> aggregateWindow(every: 1d, fn: last)
  |> difference(nonNegative: true)
```

### 6. PHP function reference

`void SINF_Sync(int $InstanceID);`
Transfers all new archive values now.

`void SINF_TestConnection(int $InstanceID);`
Checks reachability, credentials and bucket/database.

`string SINF_AddVariables(int $InstanceID, int $ParentID, bool $Recursive, bool $OnlyLogged);`
Adds all variables below `$ParentID` to the list (same as "Add multiple variables") and applies the configuration.

`void SINF_ResetSyncState(int $InstanceID, int $VariableID, int $Timestamp);`
Re-transfers values from `$Timestamp` on. `$VariableID = 0` resets all variables, `$Timestamp = 0` re-transfers the complete history.

`array SINF_GetSyncState(int $InstanceID);`
Returns `VariableID => timestamp of the last transferred value`.

### 7. Differences to SyncMySQL

* Configuration (variable list, meters, zones) is stored in the instance, not in the database. InfluxDB only receives measurements.
* Meter replacement history is not kept as a table. Points are tagged with the meter assigned at the time of writing, so a meter change is visible in the data from that point on.
* SyncMySQL inserts one row per statement. This module writes batches of thousands of lines per request and pages through the archive, so backfills of several years are transferred completely (SyncMySQL's single `AC_GetLoggedValues` call is limited to 10.000 rows).
