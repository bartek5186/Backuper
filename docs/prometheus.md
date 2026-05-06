# Backuper Prometheus Metrics

Backuper exposes passive Prometheus metrics on:

```text
GET /metrics
```

The metrics endpoint only reads the local status file configured by `app.status_file`.
It does not execute `restic`, does not scan backup directories, and does not query the remote repository.
The status file is updated when jobs start and finish.

Example config:

```json
{
  "app": {
    "name": "backuper",
    "timezone": "Europe/Warsaw",
    "max_concurrent_jobs": 1,
    "status_file": "./backups/backuper_status.json"
  }
}
```

Example Docker Compose scrape target when Prometheus runs in the same Compose network:

```yaml
scrape_configs:
  - job_name: backuper
    metrics_path: /metrics
    static_configs:
      - targets:
          - backuper:8080
```

If Prometheus runs on the host and the service is bound to localhost:

```yaml
scrape_configs:
  - job_name: backuper
    metrics_path: /metrics
    static_configs:
      - targets:
          - 127.0.0.1:8080
```

Useful alert expressions:

```promql
backuper_job_healthy == 0
```

```promql
backuper_job_last_success_age_seconds > 86400
```

```promql
backuper_metrics_collection_success == 0
```

```promql
backuper_jobs_running >= backuper_max_concurrent_jobs
```

`/api/restic/snapshots` remains an active diagnostic endpoint and does query restic when called manually.
Do not scrape it if you want the service to stay quiet while idle.

Import `docs/grafana-backuper-dashboard.json` in Grafana and select your Prometheus datasource.
