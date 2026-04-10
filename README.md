# Backuper

Backuper is a lightweight backup orchestrator for production servers.

It is designed to coordinate backups of:

- MySQL / MariaDB databases
- user-uploaded files
- large video files

Backuper does **not** implement database dumping or file snapshotting from scratch. Instead, it orchestrates proven external tools:

- `mariadb-dump` or `mysqldump` for database dumps
- `restic` for file and snapshot backups

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

A restic job is responsible for snapshotting one or more source paths.

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

## Minimal config structure

The first version uses a small JSON configuration file.

Main sections:

- `app`
- `database`
- `restic`
- `jobs`

## Example config

```json
{
  "app": {
    "name": "backuper",
    "timezone": "Europe/Warsaw"
  },
  "database": {
    "dump_tool": "mariadb-dump",
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
    "repository": "sftp:backup@100.88.10.24:/volume1/backups/restic/myapp"
  },
  "jobs": [
    {
      "name": "db_dump",
      "type": "database_dump",
      "schedule": "0 2 * * *",
      "timeout_minutes": 90,
      "database_name": "myapp",
      "host": "127.0.0.1",
      "port": 3306,
      "user": "backup_user",
      "password": "MYAPP_DB_PASSWORD",
      "retention_days": 7
    },
    {
      "name": "restic_db",
      "type": "restic_backup",
      "schedule": "20 2 * * *",
      "timeout_minutes": 120,
      "sources": [
        "./backups/db_dump"
      ],
      "tags": [
        "db"
      ],
      "retention": {
        "keep_daily": 14
      }
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
      "retention": {
        "keep_hourly": 48,
        "keep_daily": 14
      }
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
      "retention": {
        "keep_daily": 14
      }
    }
  }
}
```

## Config section overview

### `app`

General application metadata.

### `database`

Shared database dump defaults. This controls how Backuper invokes `mariadb-dump`
or `mysqldump` for every `database_dump` job.
This section contains only shared behavior such as `dump_tool`, `dump_flags` and `gzip`.

### `restic`

Shared restic configuration. This defines the repository.
The restic repository password is always read from `RESTIC_PASSWORD`.
If the repository uses `sftp:` and `SFTP_PASSWORD` is present in the environment,
Backuper automatically enables its internal restic SFTP proxy, so no `sftp.command`
needs to be exposed in `config.json`.
At startup, Backuper also loads a local `.env` file from the current working directory
if it exists. Existing environment variables are not overridden.

### `jobs`

Defines all executable jobs. The first version supports two main job types:

- `database_dump`
- `restic_backup`

For `restic_backup` jobs, retention can be defined per job with an optional `retention` block.
Supported fields are `keep_hourly`, `keep_daily`, `keep_weekly`, `keep_monthly`, and `keep_yearly`.
For `database_dump` jobs, connection details live directly in the job:

- `database_name`
- `host`
- `port`
- `user`
- `password`

`password` is the name of the environment variable that stores the database password.
Local dump file retention is controlled by `retention_days`.
Dump files are written per job into `./backups/<job_name>` by default, or into
`job.output_dir` if you want a custom local path.

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
- keep backup logic separated by data type

## Running

The default production-style start is simply:

```bash
go run ./cmd/backuper
```

This mode:

- loads `.env` from the current working directory
- loads config from `./configs/config.json`
- checks at startup whether required binaries exist on the host:
  `restic` and/or the configured database dump tool
- starts the scheduler
- starts the HTTP API on `127.0.0.1:8080`

There is also an explicit equivalent command:

```bash
go run ./cmd/backuper serve -config ./configs/config.json -listen 127.0.0.1:8080
```

## HTTP API

Backuper exposes a small read-only HTTP API for restic snapshots in `backuper`
default mode and in `backuper serve`. You can still run the API separately if needed.

Start it with:

```bash
go run ./cmd/backuper api -config ./configs/config.json -listen 127.0.0.1:8080
```

Available endpoint:

- `GET /api/restic/snapshots`

Optional filters:

- `tag=a`
- `tag=a,b`
- `tag=a&tag=b`

Example:

```bash
curl 'http://127.0.0.1:8080/api/restic/snapshots?tag=a&tag=b'
```

The endpoint runs `restic snapshots --json` against the configured repository and
returns the currently available snapshots in JSON form.

## Restore

To restore the latest snapshot for a tag:

```bash
go run ./cmd/backuper restore -tag a -target /tmp/restore_a
```

This command:

- loads `.env`
- loads `./configs/config.json` by default
- finds the newest snapshot matching the provided tag
- runs `restic restore` into the target directory

You can also restore an explicit snapshot ID:

```bash
go run ./cmd/backuper restore -snapshot 862bd04a -target /tmp/restore_a
```

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
4. retention runner
5. lock handling and structured logs

## Summary

Backuper is a thin orchestration layer around stable backup tools.

That makes it suitable for production use without turning the project into a full backup engine.

## Development

Current bootstrap:

- `go.mod` with module `github.com/bartek5186/backuper`
- `cmd/backuper` with scheduler, API, restore, validation and runtime dependency checks
- `configs/example.json` with a starter config using shared database defaults and per-job DB connections

Quick check:

```bash
go run ./cmd/backuper validate -config ./configs/example.json
```

Template runner:

```bash
go run ./cmd/backuper run -config ./configs/example.json -jobs restic_db,restic_uploads
```
