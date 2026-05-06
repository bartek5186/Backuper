# Backuper Prometheus Metrics

Backuper exposes Prometheus metrics on:

```text
GET /metrics
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

Import `docs/grafana-backuper-dashboard.json` in Grafana and select your Prometheus datasource.
