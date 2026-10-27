# Diagnose a run

Use log messages to see what portolan-cli does, and timings to see where a run
spends its time and memory. Both write to stderr or to a file. Stdout keeps the
normal output, so a script that parses `--format json` output still works.

## Log messages

By default, portolan-cli prints warnings and errors to stderr. Put a verbosity
flag before the command name:

| Flag | Level | Loggers |
|---|---|---|
| none | WARNING | all |
| `-v` | INFO | `portolan_cli` |
| `-vv` | DEBUG | `portolan_cli`, `urllib3`, `httpx`, and `obstore` |
| `-q`, `--quiet` | ERROR | all |

```bash
portolan -vv add demographics/
```

Each line shows the seconds since start, the level, and the logger name:

```text
+0.412s DEBUG portolan_cli.add: ...
```

At `-vv`, Python warnings also go through the log as the `py.warnings` logger.

Some commands, such as `check`, `add`, and `readme`, have their own `--verbose`
flag. That flag controls how much the command prints, and the root flag
controls the log level. `portolan -v check --verbose` sets both.

These environment variables also control logging:

| Variable | Effect |
|---|---|
| `PORTOLAN_LOG` | Sets the level to `debug`, `info`, or `warning` when you give no flag. A flag takes precedence. |
| `PORTOLAN_LOG_FILE` | Also writes each record to this path as one JSON object per line, with the run ID. |
| `PORTOLAN_GDAL_DEBUG` | Set to `1` to turn on GDAL debug output (`CPL_DEBUG=ON`). The GDAL messages go to stderr as `rasterio._env` records at any verbosity. |

## Timings

Timings are off by default. To turn them on, set `PORTOLAN_TIMINGS=1` or pass
`--timings` before the command name:

```bash
portolan --timings add demographics/
```

In text mode, a table of the stages prints to stderr when the command exits:

```text
span                  wall s  cpu s  rss MB
portolan.add           4.210  5.902   412.3
  add.collect          0.031  0.030   188.0
  add.process          3.104  4.880   412.3
```

Wall time is elapsed time. CPU time is the CPU time of the whole process,
worker threads included.
`rss MB` is the resident memory high-water mark of the process at the end of
the stage.

`PORTOLAN_TIMINGS=mem` also records the Python memory growth of each stage
with `tracemalloc`. The growth is the peak traced memory of the stage less the
traced memory when the stage starts. Memory that an earlier stage holds does
not count. This mode makes the run slower.

### The meta block

With timings on, JSON output gets a `meta` key next to `data`. The error
envelope gets it too. Without timings, the envelope has no `meta` key.

```bash
PORTOLAN_TIMINGS=1 portolan --format json add demographics/
```

| Field | Holds |
|---|---|
| `timings` | The span tree. Each span has `name`, `start_s`, `wall_s`, `cpu_s`, `rss_peak_mb`, `tid`, `attrs`, and `children`. |
| `counters` | Counts of repeated work for the whole run. |
| `peak_rss_mb` | The resident memory high-water mark of the process. |
| `version` | The portolan-cli version. |
| `run_id` | The ID that also appears in `PORTOLAN_LOG_FILE` records. |

The envelope prints before the command span closes, so that span has
`"open": true` and its duration stops at the print. In `mem` mode, each span
also has `mem_growth_mb`.

The counters are:

| Counter | Counts |
|---|---|
| `checksum.files`, `checksum.bytes` | Files and bytes hashed with SHA-256 |
| `versions_json.reads` | Reads of a local or remote `versions.json` |
| `config.yaml_parses` | Parses of a `config.yaml` or `metadata.yaml` file |
| `thumbnail.draws` | Thumbnail images drawn |
| `raster.opens` | Rasters opened by the metadata readers |
| `parquet.opens` | Parquet files opened by the metadata readers |

### Trace files

Set `PORTOLAN_TRACE_FILE` to write the spans as a Chrome trace. This variable
also turns timings on.

```bash
PORTOLAN_TRACE_FILE=trace.json portolan add demographics/
```

Open the file in [Perfetto](https://ui.perfetto.dev). Each thread gets its own
row. The rows show how the worker threads of `add`, `push`, and `extract`
overlap.

### Limits

- Windows has no `resource` module, so `rss_peak_mb` is `null` there.
- With more than one worker, `check --fix` converts files in worker
  processes. Spans and counters inside those processes do not reach the
  parent run.
- rashid runs all validation rules in one call, so `check.validate` is one
  span for all the rules.
- In `mem` mode, the `tracemalloc` peak is for the whole process. With more
  than one worker, the peak of a span can include memory that other threads
  allocated.
