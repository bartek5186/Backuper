# Backuper

Backuper is a lightweight backup orchestrator for production servers.

It is designed to coordinate backups of:

- MySQL / MariaDB databases
- user-uploaded files
- large video files

Backuper does **not** implement database dumping or file snapshotting from scratch. Instead, it orchestrates proven external tools:

- `mariadb-dump` or `mysqldump` for database dumps
- `restic` for file and snapshot backups
- Healthchecks for job monitoring and failure detection

## Goals

Backuper is built for a simple first version with the following priorities:

- predictable backups
- minimal configuration
- clear job separation
- easy monitoring
- private transport to a NAS or other servers through Tailscale / SSH

## Architecture

Backuper acts as the control layer. The actual backup work is delegated to external tools.

### Database flow

1. Backuper runs `mariadb-dump` or `mysqldump`
2. the dump is saved locally
3. the dump is compressed with gzip
4. `restic` backs up the dump directory into the remote repository

### File backup flow

Backuper runs separate backup jobs for:

- uploads
- videos

Each directory is backed up with `restic` into the same or a separate repository.

### Monitoring flow

Each job can report its state to Healthchecks:

- start
- success
- failure

This makes it possible to detect:

- failed jobs
- stuck jobs
- missing scheduled runs

## Why this design

Backuper intentionally avoids reimplementing backup engines.

That means:

- no custom MySQL dump implementation
- no custom file deduplication engine
- no custom repository format

This keeps the program smaller, safer, and easier to maintain.

## Main components

### 1. Database dump jobs

A database job is responsible only for producing a consistent local dump.

Typical command:

```bash
mariadb-dump --single-transaction --quick --routines --triggers --events myapp | gzip > /var/backups/myapp/db/myapp_db_2026-03-25_02-00-00.sql.gz
```

### 2. Restic backup jobs

A restic job is responsible for snapshotting one or more directories.

Typical examples:

- dump directory
- uploads directory
- videos directory

### 3. Retention

Retention is handled through `restic forget --prune`.

A simple first-version policy can look like this:

- database snapshots: keep 14 daily backups
- uploads snapshots: keep 48 hourly and 14 daily backups
- videos snapshots: keep 14 daily backups

### 4. Healthchecks integration

Every job can have its own Healthchecks ID.

Backuper should notify Healthchecks when a job:

- starts
- finishes successfully
- fails

## Minimal config structure

The first version uses a small JSON configuration file.

Main sections:

- `app`
- `paths`
- `database`
- `restic`
- `healthchecks`
- `jobs`
- `retention`

## Example config

```json
{
  "app": {
    "name": "backuper",
    "timezone": "Europe/Warsaw"
  },
  "paths": {
    "db_dump_dir": "/var/backups/myapp/db",
    "uploads_dir": "/srv/myapp/uploads",
    "videos_dir": "/srv/myapp/videos"
  },
  "database": {
    "dump_tool": "mariadb-dump",
    "host": "127.0.0.1",
    "port": 3306,
    "name": "myapp",
    "username": "backup_user",
    "password_env": "BACKUP_DB_PASSWORD",
    "dump_flags": [
      "--single-transaction",
      "--quick",
      "--routines",
      "--triggers",
      "--events"
    ],
    "gzip": true
  },
  "restic": {
    "binary": "/usr/bin/restic",
    "repository": "sftp:backup@100.88.10.24:/volume1/backups/restic/myapp",
    "password_env": "RESTIC_PASSWORD"
  },
  "healthchecks": {
    "base_url": "https://hc-ping.com",
    "jobs": {
      "db_dump": "11111111-1111-1111-1111-111111111111",
      "restic_db": "22222222-2222-2222-2222-222222222222",
      "restic_uploads": "33333333-3333-3333-3333-333333333333",
      "restic_videos": "44444444-4444-4444-4444-444444444444"
    }
  },
  "jobs": [
    {
      "name": "db_dump",
      "type": "database_dump",
      "schedule": "0 2 * * *",
      "timeout_minutes": 90,
      "output_dir": "/var/backups/myapp/db",
      "healthcheck_id": "11111111-1111-1111-1111-111111111111"
    },
    {
      "name": "restic_db",
      "type": "restic_backup",
      "schedule": "20 2 * * *",
      "timeout_minutes": 120,
      "sources": [
        "/var/backups/myapp/db"
      ],
      "tags": [
        "db"
      ],
      "healthcheck_id": "22222222-2222-2222-2222-222222222222"
    },
    {
      "name": "restic_uploads",
      "type": "restic_backup",
      "schedule": "15 * * * *",
      "timeout_minutes": 180,
      "sources": [
        "/srv/myapp/uploads"
      ],
      "tags": [
        "uploads"
      ],
      "healthcheck_id": "33333333-3333-3333-3333-333333333333"
    },
    {
      "name": "restic_videos",
      "type": "restic_backup",
      "schedule": "30 */6 * * *",
      "timeout_minutes": 720,
      "sources": [
        "/srv/myapp/videos"
      ],
      "tags": [
        "videos"
      ],
      "healthcheck_id": "44444444-4444-4444-4444-444444444444"
    }
  ],
  "retention": {
    "db_keep_daily": 14,
    "uploads_keep_hourly": 48,
    "uploads_keep_daily": 14,
    "videos_keep_daily": 14
  }
}
```

## Config section overview

### `app`

General application metadata.

### `paths`

Local directories used by Backuper.

### `database`

Database dump configuration. This controls how Backuper invokes `mariadb-dump` or `mysqldump`.

### `restic`

Shared restic configuration. This defines the repository and the password source.

### `healthchecks`

Job monitoring configuration. Each job can be mapped to a dedicated Healthchecks check.

### `jobs`

Defines all executable jobs. The first version supports two main job types:

- `database_dump`
- `restic_backup`

### `retention`

Retention rules used when running `restic forget --prune`.

## Suggested first-version job set

The simplest useful setup is:

1. `db_dump`
2. `restic_db`
3. `restic_uploads`
4. `restic_videos`

This keeps the system easy to reason about and easy to debug.

## Expected behavior

Backuper should:

- execute jobs on schedule
- enforce one running instance per job
- collect exit status and output
- fail clearly when external tools fail
- report failures to Healthchecks
- keep backup logic separated by data type

## Not in scope for v1

The first version intentionally skips:

- SMTP email notifications
- advanced disk-space policies
- restore verification automation
- multi-repository routing per job
- a web UI
- custom storage engines

These can be added later without changing the core design.

## Recommended next steps

A practical implementation order:

1. config loading and validation
2. database dump runner
3. restic backup runner
4. Healthchecks integration
5. retention runner
6. lock handling and structured logs

## Summary

Backuper is a thin orchestration layer around stable backup tools.

That makes it suitable for production use without turning the project into a full backup engine.

