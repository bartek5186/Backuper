package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLoadConfigSupportsDatabaseDefaultsAndJobSpecificConnections(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")

	content := `{
  "app": {
    "name": "backuper",
    "timezone": "Europe/Warsaw"
  },
  "database": {
    "type": "mariadb",
    "gzip": true
  },
  "restic": {
    "binary": "/usr/bin/restic",
    "repository": "sftp:backup@100.88.10.24:/volume1/backups/restic/myapp"
  },
  "jobs": [
    {
      "name": "db_app_main_dump",
      "type": "database_dump",
      "schedule": "0 2 * * *",
      "timeout_minutes": 90,
      "database_name": "myapp",
      "host": "127.0.0.1",
      "port": 3306,
      "user": "backup_user",
      "password": "BACKUP_DB_PASSWORD",
      "tables": ["users", "orders"]
    },
    {
      "name": "restic_uploads",
      "type": "restic_backup",
      "schedule": "15 * * * *",
      "timeout_minutes": 180,
      "sources": ["/srv/myapp/uploads/public"]
    }
  ]
}`

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	if _, err := loadConfig(path); err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
}

func TestLoadConfigSupportsNamedDatabaseConfigs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")

	content := `{
  "app": {
    "name": "backuper",
    "timezone": "Europe/Warsaw"
  },
  "database": [
    {
      "name": "mysql",
      "type": "mariadb",
      "gzip": true
    },
    {
      "name": "postgres",
      "type": "postgres",
      "gzip": false
    }
  ],
  "jobs": [
    {
      "name": "db_app_main_dump",
      "type": "database_dump",
      "schedule": "0 2 * * *",
      "timeout_minutes": 90,
      "database_config": "postgres",
      "database_name": "myapp",
      "host": "127.0.0.1",
      "port": 5432,
      "user": "backup_user",
      "password": "BACKUP_DB_PASSWORD"
    }
  ]
}`

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}

	if len(cfg.Database) != 2 {
		t.Fatalf("expected 2 database configs, got %d", len(cfg.Database))
	}
	if cfg.Database[1].Name != "postgres" {
		t.Fatalf("unexpected second database config: %#v", cfg.Database[1])
	}
	if cfg.Jobs[0].DatabaseConfig != "postgres" {
		t.Fatalf("unexpected job database_config: %q", cfg.Jobs[0].DatabaseConfig)
	}
}

func TestLoadDotEnvIfPresentSetsMissingVariables(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")

	content := "RESTIC_PASSWORD=secret\nSFTP_PASSWORD='another-secret'\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	if err := loadDotEnvIfPresent(path); err != nil {
		t.Fatalf("loadDotEnvIfPresent() error = %v", err)
	}

	if got := os.Getenv("RESTIC_PASSWORD"); got != "secret" {
		t.Fatalf("unexpected RESTIC_PASSWORD: %q", got)
	}
	if got := os.Getenv("SFTP_PASSWORD"); got != "another-secret" {
		t.Fatalf("unexpected SFTP_PASSWORD: %q", got)
	}
}

func TestLoadDotEnvIfPresentDoesNotOverrideExistingVariables(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")

	if err := os.WriteFile(path, []byte("RESTIC_PASSWORD=from-file\n"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	t.Setenv("RESTIC_PASSWORD", "from-env")

	if err := loadDotEnvIfPresent(path); err != nil {
		t.Fatalf("loadDotEnvIfPresent() error = %v", err)
	}

	if got := os.Getenv("RESTIC_PASSWORD"); got != "from-env" {
		t.Fatalf("unexpected RESTIC_PASSWORD: %q", got)
	}
}

func TestValidateRejectsMissingDatabaseFieldsInDumpJob(t *testing.T) {
	cfg := config{
		App: appConfig{
			Name:     "backuper",
			Timezone: "Europe/Warsaw",
		},
		Database: databaseConfigList{{Type: "mariadb"}},
		Jobs: []jobConfig{
			{
				Name:           "db_missing",
				Type:           "database_dump",
				Schedule:       "0 2 * * *",
				TimeoutMinutes: 90,
			},
		},
	}

	err := cfg.validate()
	if err == nil {
		t.Fatal("validate() returned nil error for incomplete database_dump job")
	}

	for _, fragment := range []string{
		"database_name is required",
		"host is required",
		"port must be greater than 0",
		"user is required",
		"password is required",
	} {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("expected validation error containing %q, got %v", fragment, err)
		}
	}
}

func TestValidateAllowsDatabaseDumpWithGlobalDefaults(t *testing.T) {
	cfg := config{
		App: appConfig{
			Name:     "backuper",
			Timezone: "Europe/Warsaw",
		},
		Database: databaseConfigList{{Type: "mariadb"}},
		Jobs: []jobConfig{
			{
				Name:           "db_dump",
				Type:           "database_dump",
				Schedule:       "0 2 * * *",
				TimeoutMinutes: 90,
				DatabaseName:   "myapp",
				Host:           "127.0.0.1",
				Port:           3306,
				User:           "backup_user",
				Password:       "BACKUP_DB_PASSWORD",
				OutputDir:      "/var/backups/myapp/db",
			},
		},
	}

	if err := cfg.validate(); err != nil {
		t.Fatalf("validate() error = %v", err)
	}
}

func TestValidateRequiresDatabaseConfigWhenMultipleDatabaseConfigsExist(t *testing.T) {
	cfg := config{
		App: appConfig{
			Name:     "backuper",
			Timezone: "Europe/Warsaw",
		},
		Database: databaseConfigList{
			{Name: "mysql", Type: "mysql"},
			{Name: "postgres", Type: "postgres"},
		},
		Jobs: []jobConfig{
			{
				Name:           "db_dump",
				Type:           "database_dump",
				Schedule:       "0 2 * * *",
				TimeoutMinutes: 90,
				DatabaseName:   "myapp",
				Host:           "127.0.0.1",
				Port:           3306,
				User:           "backup_user",
				Password:       "BACKUP_DB_PASSWORD",
			},
		},
	}

	err := cfg.validate()
	if err == nil {
		t.Fatal("validate() returned nil error for missing database_config")
	}

	if !strings.Contains(err.Error(), "database_config is required when multiple database configs are defined") {
		t.Fatalf("expected database_config validation error, got %v", err)
	}
}

func TestValidateRejectsUnsupportedDatabaseType(t *testing.T) {
	cfg := config{
		App: appConfig{
			Name:     "backuper",
			Timezone: "Europe/Warsaw",
		},
		Database: databaseConfigList{
			{Type: "oracle"},
		},
		Jobs: []jobConfig{
			{
				Name:           "db_dump",
				Type:           "database_dump",
				Schedule:       "0 2 * * *",
				TimeoutMinutes: 90,
				DatabaseName:   "myapp",
				Host:           "127.0.0.1",
				Port:           3306,
				User:           "backup_user",
				Password:       "BACKUP_DB_PASSWORD",
			},
		},
	}

	err := cfg.validate()
	if err == nil {
		t.Fatal("validate() returned nil error for unsupported database type")
	}

	if !strings.Contains(err.Error(), `database.type must be one of "mysql", "mariadb", or "postgres"`) {
		t.Fatalf("expected unsupported database type error, got %v", err)
	}
}

func TestValidateAllowsJobLevelRetentionForResticBackup(t *testing.T) {
	cfg := config{
		App: appConfig{
			Name:     "backuper",
			Timezone: "Europe/Warsaw",
		},
		Restic: resticConfig{
			Binary:     "/usr/bin/restic",
			Repository: "sftp://backup@example//repo",
		},
		Jobs: []jobConfig{
			{
				Name:           "restic_uploads",
				Type:           "restic_backup",
				Schedule:       "15 * * * *",
				TimeoutMinutes: 180,
				Sources:        []string{"/srv/myapp/uploads"},
				Tags:           []string{"uploads"},
				Retention: &jobRetentionConfig{
					KeepHourly: 48,
					KeepDaily:  14,
				},
			},
		},
	}

	if err := cfg.validate(); err != nil {
		t.Fatalf("validate() error = %v", err)
	}
}

func TestValidateAllowsRetentionForDatabaseDump(t *testing.T) {
	cfg := config{
		App: appConfig{
			Name:     "backuper",
			Timezone: "Europe/Warsaw",
		},
		Database: databaseConfigList{{Type: "mariadb"}},
		Jobs: []jobConfig{
			{
				Name:           "db_app_main_dump",
				Type:           "database_dump",
				Schedule:       "0 2 * * *",
				TimeoutMinutes: 90,
				DatabaseName:   "myapp",
				Host:           "127.0.0.1",
				Port:           3306,
				User:           "backup_user",
				Password:       "BACKUP_DB_PASSWORD",
				Retention: &jobRetentionConfig{
					KeepHourly: 3,
					KeepDaily:  14,
				},
			},
		},
	}

	if err := cfg.validate(); err != nil {
		t.Fatalf("validate() error = %v", err)
	}
}

func TestValidateRejectsRetentionDays(t *testing.T) {
	cfg := config{
		App: appConfig{
			Name:     "backuper",
			Timezone: "Europe/Warsaw",
		},
		Database: databaseConfigList{{Type: "mariadb"}},
		Jobs: []jobConfig{
			{
				Name:           "db_app_main_dump",
				Type:           "database_dump",
				Schedule:       "0 2 * * *",
				TimeoutMinutes: 90,
				DatabaseName:   "myapp",
				Host:           "127.0.0.1",
				Port:           3306,
				User:           "backup_user",
				Password:       "BACKUP_DB_PASSWORD",
				RetentionDays:  7,
			},
		},
	}

	err := cfg.validate()
	if err == nil {
		t.Fatal("validate() returned nil error for retention_days")
	}

	if !strings.Contains(err.Error(), "retention_days is no longer supported") {
		t.Fatalf("expected deprecated retention_days error, got %v", err)
	}
}

func TestCheckRuntimeDependenciesRejectsMissingBinaries(t *testing.T) {
	cfg := config{
		Database: databaseConfigList{{DumpTool: "/tmp/definitely-missing-dump-tool"}},
		Restic: resticConfig{
			Binary: "/tmp/definitely-missing-restic",
		},
	}

	jobs := []jobConfig{
		{
			Name: "db_job",
			Type: "database_dump",
		},
		{
			Name: "restic_job",
			Type: "restic_backup",
		},
	}

	err := checkRuntimeDependencies(cfg, jobs, true)
	if err == nil {
		t.Fatal("checkRuntimeDependencies() returned nil error for missing binaries")
	}

	if !strings.Contains(err.Error(), "restic.binary") {
		t.Fatalf("expected restic binary error, got %v", err)
	}
	if !strings.Contains(err.Error(), "database.binary") {
		t.Fatalf("expected dump binary error, got %v", err)
	}
}

func TestCheckRuntimeDependenciesRequiresResticForDatabaseDumpWhenEnabled(t *testing.T) {
	enabled := true
	cfg := config{
		Database: databaseConfigList{{
			Type:   "mariadb",
			Restic: &enabled,
		}},
		Restic: resticConfig{
			Binary: "/tmp/definitely-missing-restic",
		},
	}

	jobs := []jobConfig{
		{
			Name: "db_job",
			Type: "database_dump",
		},
	}

	err := checkRuntimeDependencies(cfg, jobs, requireResticForJobs(cfg, jobs))
	if err == nil {
		t.Fatal("checkRuntimeDependencies() returned nil error for missing restic on database_dump")
	}

	if !strings.Contains(err.Error(), "restic.binary") {
		t.Fatalf("expected restic binary error, got %v", err)
	}
}

func TestFilterJobsSelectsRequestedNames(t *testing.T) {
	jobs := []jobConfig{
		{Name: "restic_a"},
		{Name: "restic_b"},
		{Name: "restic_c"},
	}

	filtered, err := filterJobs(jobs, []string{"restic_b", "restic_c"})
	if err != nil {
		t.Fatalf("filterJobs() error = %v", err)
	}

	if len(filtered) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(filtered))
	}

	if filtered[0].Name != "restic_b" || filtered[1].Name != "restic_c" {
		t.Fatalf("unexpected filtered jobs: %#v", filtered)
	}
}

func TestFilterJobsRejectsUnknownName(t *testing.T) {
	_, err := filterJobs([]jobConfig{{Name: "restic_a"}}, []string{"missing"})
	if err == nil {
		t.Fatal("filterJobs() returned nil error for unknown job")
	}

	if !strings.Contains(err.Error(), `job "missing" does not exist`) {
		t.Fatalf("expected unknown job error, got %v", err)
	}
}

func TestJobMatchesSchedule(t *testing.T) {
	now := time.Date(2026, time.April, 10, 2, 10, 0, 0, time.UTC)

	tests := []struct {
		schedule string
		want     bool
	}{
		{schedule: "10 2 * * *", want: true},
		{schedule: "*/10 2 * * *", want: true},
		{schedule: "0,10,20 2 * * *", want: true},
		{schedule: "11 2 * * *", want: false},
		{schedule: "10 3 * * *", want: false},
	}

	for _, tc := range tests {
		got, err := jobMatchesSchedule(tc.schedule, now)
		if err != nil {
			t.Fatalf("jobMatchesSchedule(%q) error = %v", tc.schedule, err)
		}
		if got != tc.want {
			t.Fatalf("jobMatchesSchedule(%q) = %v, want %v", tc.schedule, got, tc.want)
		}
	}
}

func TestPlanResticWorkUnitsPairsSourcesWithTags(t *testing.T) {
	job := jobConfig{
		Name:    "restic_test",
		Sources: []string{"/data/A", "/data/B", "/data/C"},
		Tags:    []string{"a", "b", "c"},
	}

	units, err := planResticWorkUnits(job)
	if err != nil {
		t.Fatalf("planResticWorkUnits() error = %v", err)
	}

	if len(units) != 3 {
		t.Fatalf("expected 3 units, got %d", len(units))
	}

	if len(units[0].sources) != 1 || units[0].sources[0] != "/data/A" || units[0].tag != "a" {
		t.Fatalf("unexpected first unit: %#v", units[0])
	}
	if len(units[1].sources) != 1 || units[1].sources[0] != "/data/B" || units[1].tag != "b" {
		t.Fatalf("unexpected second unit: %#v", units[1])
	}
	if len(units[2].sources) != 1 || units[2].sources[0] != "/data/C" || units[2].tag != "c" {
		t.Fatalf("unexpected third unit: %#v", units[2])
	}
}

func TestValidateRejectsMismatchedResticTags(t *testing.T) {
	cfg := config{
		App: appConfig{
			Name:     "backuper",
			Timezone: "Europe/Warsaw",
		},
		Restic: resticConfig{
			Binary:     "/usr/bin/restic",
			Repository: "sftp://backup@example//repo",
		},
		Jobs: []jobConfig{
			{
				Name:           "restic_uploads",
				Type:           "restic_backup",
				Schedule:       "15 * * * *",
				TimeoutMinutes: 180,
				Sources:        []string{"/data/A", "/data/B", "/data/C"},
				Tags:           []string{"a", "b"},
				Retention: &jobRetentionConfig{
					KeepDaily: 14,
				},
			},
		},
	}

	err := cfg.validate()
	if err == nil {
		t.Fatal("validate() returned nil error for mismatched tags")
	}

	if !strings.Contains(err.Error(), "tags must contain exactly one tag or match the number of sources") {
		t.Fatalf("expected mismatched tags error, got %v", err)
	}
}

func TestValidateRejectsEmptyResticExcludeEntries(t *testing.T) {
	cfg := config{
		App: appConfig{
			Name:     "backuper",
			Timezone: "Europe/Warsaw",
		},
		Restic: resticConfig{
			Binary:     "/usr/bin/restic",
			Repository: "sftp://backup@example//repo",
		},
		Jobs: []jobConfig{
			{
				Name:           "restic_uploads",
				Type:           "restic_backup",
				Schedule:       "15 * * * *",
				TimeoutMinutes: 180,
				Sources:        []string{"/data/A"},
				Exclude:        []string{"cache", ""},
				Tags:           []string{"uploads"},
			},
		},
	}

	err := cfg.validate()
	if err == nil {
		t.Fatal("validate() returned nil error for empty exclude entry")
	}

	if !strings.Contains(err.Error(), "exclude[1] must not be empty") {
		t.Fatalf("expected exclude validation error, got %v", err)
	}
}

func TestParseResticSFTPConnectionSupportsURLForm(t *testing.T) {
	connection, err := parseResticSFTPConnection("sftp://footballmat@warehouse:9990//vault/backups/footballmat")
	if err != nil {
		t.Fatalf("parseResticSFTPConnection() error = %v", err)
	}

	if connection.User != "footballmat" {
		t.Fatalf("unexpected user: %q", connection.User)
	}
	if connection.Host != "warehouse" {
		t.Fatalf("unexpected host: %q", connection.Host)
	}
	if connection.Port != "9990" {
		t.Fatalf("unexpected port: %q", connection.Port)
	}
}

func TestResticBaseArgsEnablesInternalSFTPProxyWhenPasswordIsSet(t *testing.T) {
	t.Setenv(sftpPasswordEnv, "secret")

	runner := newTemplateRunner(config{
		Restic: resticConfig{
			Repository: "sftp://footballmat@warehouse:9990//vault/backups/footballmat",
		},
	}, os.Stdout, os.Stderr)

	args, envAdditions, err := runner.resticBaseArgs()
	if err != nil {
		t.Fatalf("resticBaseArgs() error = %v", err)
	}

	if len(args) != 4 {
		t.Fatalf("expected 4 restic base args, got %d: %#v", len(args), args)
	}
	if args[0] != "--option" {
		t.Fatalf("expected first arg to be --option, got %q", args[0])
	}
	if !strings.HasPrefix(args[1], "sftp.command=") {
		t.Fatalf("expected sftp.command option, got %q", args[1])
	}
	if !strings.HasSuffix(args[1], " "+resticSFTPProxySubcommand) {
		t.Fatalf("expected proxy subcommand suffix, got %q", args[1])
	}
	if args[2] != "-r" || args[3] != "sftp://footballmat@warehouse:9990//vault/backups/footballmat" {
		t.Fatalf("unexpected repository args: %#v", args)
	}

	if len(envAdditions) != 1 || envAdditions[0] != backuperResticRepoEnv+"=sftp://footballmat@warehouse:9990//vault/backups/footballmat" {
		t.Fatalf("unexpected restic env additions: %#v", envAdditions)
	}
}

func TestHandleResticSnapshotsReturnsActiveSnapshots(t *testing.T) {
	t.Setenv(resticPasswordEnv, "secret")

	dir := t.TempDir()
	argsPath := filepath.Join(dir, "args.txt")
	binaryPath := filepath.Join(dir, "fake-restic.sh")
	t.Setenv("TEST_ARGS_FILE", argsPath)

	script := `#!/usr/bin/env bash
set -eu
printf '%s
' "$@" > "$TEST_ARGS_FILE"
printf '%s
' '[{"id":"abc123","tags":["a"],"paths":["/data/A"]}]'
`

	if err := os.WriteFile(binaryPath, []byte(script), 0o755); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	server := newAPIServer(config{
		Restic: resticConfig{
			Binary:     binaryPath,
			Repository: "/tmp/restic-repo",
		},
	}, os.Stdout, os.Stderr)

	request := httptest.NewRequest(http.MethodGet, "/api/restic/snapshots?tag=a&tag=b,c", nil)
	recorder := httptest.NewRecorder()

	server.handleResticSnapshots(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected status code: got %d, want %d; body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var response resticSnapshotsResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if response.Repository != "/tmp/restic-repo" {
		t.Fatalf("unexpected repository: %q", response.Repository)
	}
	if response.Count != 1 {
		t.Fatalf("unexpected snapshot count: %d", response.Count)
	}
	if len(response.Tags) != 3 || response.Tags[0] != "a" || response.Tags[1] != "b" || response.Tags[2] != "c" {
		t.Fatalf("unexpected tags: %#v", response.Tags)
	}
	if len(response.Snapshots) != 1 {
		t.Fatalf("unexpected snapshots payload: %#v", response.Snapshots)
	}

	argsBytes, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	args := strings.Fields(string(argsBytes))
	expectedArgs := []string{"-r", "/tmp/restic-repo", "snapshots", "--json", "--tag", "a", "--tag", "b", "--tag", "c"}
	if len(args) != len(expectedArgs) {
		t.Fatalf("unexpected restic args: got %#v, want %#v", args, expectedArgs)
	}
	for i := range expectedArgs {
		if args[i] != expectedArgs[i] {
			t.Fatalf("unexpected restic args: got %#v, want %#v", args, expectedArgs)
		}
	}
}

func TestHandleResticSnapshotsRejectsNonGET(t *testing.T) {
	server := newAPIServer(config{}, os.Stdout, os.Stderr)

	request := httptest.NewRequest(http.MethodPost, "/api/restic/snapshots", nil)
	recorder := httptest.NewRecorder()

	server.handleResticSnapshots(recorder, request)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("unexpected status code: got %d, want %d", recorder.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandleHealthReturnsJobsWithBackupStatus(t *testing.T) {
	t.Setenv(resticPasswordEnv, "secret")
	now := time.Now().UTC().Truncate(time.Second)

	dir := t.TempDir()
	previousWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd() error = %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("Chdir() error = %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(previousWD)
	})

	dumpDir := filepath.Join(dir, "backups", "db_job")
	if err := os.MkdirAll(dumpDir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	dumpPath := filepath.Join(dumpDir, "db_job_2026-04-10_12-00-00.sql.gz")
	dumpTime := now.Add(-2 * time.Hour)
	if err := os.WriteFile(dumpPath, []byte("dump"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := os.Chtimes(dumpPath, dumpTime, dumpTime); err != nil {
		t.Fatalf("Chtimes() error = %v", err)
	}

	argsPath := filepath.Join(dir, "args.txt")
	binaryPath := filepath.Join(dir, "fake-restic.sh")
	t.Setenv("TEST_ARGS_FILE", argsPath)
	script := `#!/usr/bin/env bash
set -eu
printf '%s ' "$@" >> "$TEST_ARGS_FILE"
printf '\n' >> "$TEST_ARGS_FILE"
printf '%s\n' '[{"id":"dbsnap","time":"` + now.Add(-90*time.Minute).Format(time.RFC3339) + `","tags":["db_job"],"paths":["/tmp/ignored"]},{"id":"snap1","time":"` + now.Add(-time.Hour).Format(time.RFC3339) + `","tags":["uploads"],"paths":["/tmp/ignored"]}]'
`
	if err := os.WriteFile(binaryPath, []byte(script), 0o755); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	enabled := true
	runner := newTemplateRunner(config{
		Database: databaseConfigList{{Type: "mariadb", Restic: &enabled}},
		Restic: resticConfig{
			Binary:     binaryPath,
			Repository: "/tmp/restic-repo",
		},
	}, os.Stdout, os.Stderr)
	jobs := []jobConfig{
		{Name: "db_job", Type: "database_dump", DatabaseName: "myapp", Host: "127.0.0.1", Port: 3306, User: "backup_user", Password: "BACKUP_DB_PASSWORD"},
		{
			Name:           "restic_job",
			Type:           "restic_backup",
			Sources:        []string{"/data/uploads"},
			Tags:           []string{"uploads"},
			Schedule:       "0 * * * *",
			TimeoutMinutes: 60,
		},
	}

	server := newAPIServerWithRunner(runner, jobs, os.Stdout, os.Stderr)
	request := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	recorder := httptest.NewRecorder()

	server.handleHealth(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected status code: got %d, want %d; body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var response healthResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	if len(response.Jobs) != 2 {
		t.Fatalf("unexpected job count: %#v", response.Jobs)
	}
	if response.Jobs[0].Name != "db_job" || !response.Jobs[0].Healthy || response.Jobs[0].LastSuccessAt == nil {
		t.Fatalf("unexpected first job status: %#v", response.Jobs[0])
	}
	if !response.Jobs[0].LastSuccessAt.UTC().Equal(now.Add(-90 * time.Minute)) {
		t.Fatalf("unexpected db last success: %#v", response.Jobs[0].LastSuccessAt)
	}
	if response.Jobs[1].Name != "restic_job" || !response.Jobs[1].Healthy || response.Jobs[1].LastSuccessAt == nil {
		t.Fatalf("unexpected second job status: %#v", response.Jobs[1])
	}
	if !response.Jobs[1].LastSuccessAt.UTC().Equal(now.Add(-time.Hour)) {
		t.Fatalf("unexpected restic last success: %#v", response.Jobs[1].LastSuccessAt)
	}
	if response.Jobs[0].Error != "" || response.Jobs[1].Error != "" {
		t.Fatalf("unexpected errors in health response: %#v", response.Jobs)
	}

	argsBytes, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	args := strings.Fields(string(argsBytes))
	expectedArgs := []string{"-r", "/tmp/restic-repo", "snapshots", "--json"}
	if len(args) != len(expectedArgs) {
		t.Fatalf("unexpected restic args: got %#v, want %#v", args, expectedArgs)
	}
	for i := range expectedArgs {
		if args[i] != expectedArgs[i] {
			t.Fatalf("unexpected restic args: got %#v, want %#v", args, expectedArgs)
		}
	}
}

func TestHandleHealthReturnsFalseForStaleSnapshots(t *testing.T) {
	t.Setenv(resticPasswordEnv, "secret")
	now := time.Now().UTC().Truncate(time.Second)

	dir := t.TempDir()
	argsPath := filepath.Join(dir, "args.txt")
	binaryPath := filepath.Join(dir, "fake-restic.sh")
	t.Setenv("TEST_ARGS_FILE", argsPath)
	script := `#!/usr/bin/env bash
set -eu
printf '%s ' "$@" >> "$TEST_ARGS_FILE"
printf '\n' >> "$TEST_ARGS_FILE"
printf '%s\n' '[{"id":"snap1","time":"` + now.Add(-25*time.Hour).Format(time.RFC3339) + `","tags":["uploads"],"paths":["/data/uploads"]}]'
`
	if err := os.WriteFile(binaryPath, []byte(script), 0o755); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	runner := newTemplateRunner(config{
		Restic: resticConfig{
			Binary:     binaryPath,
			Repository: "/tmp/restic-repo",
		},
	}, os.Stdout, os.Stderr)
	jobs := []jobConfig{
		{
			Name:           "restic_job",
			Type:           "restic_backup",
			Sources:        []string{"/data/uploads"},
			Tags:           []string{"uploads"},
			Schedule:       "0 * * * *",
			TimeoutMinutes: 60,
		},
	}

	server := newAPIServerWithRunner(runner, jobs, os.Stdout, os.Stderr)
	request := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	recorder := httptest.NewRecorder()

	server.handleHealth(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected status code: got %d, want %d; body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var response healthResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	if len(response.Jobs) != 1 {
		t.Fatalf("unexpected job count: %#v", response.Jobs)
	}
	if response.Jobs[0].Healthy {
		t.Fatalf("expected stale snapshot to be unhealthy: %#v", response.Jobs[0])
	}
	if response.Jobs[0].LastSuccessAt == nil {
		t.Fatalf("expected stale snapshot timestamp to be present: %#v", response.Jobs[0])
	}
}

func TestHandleHealthRejectsNonGET(t *testing.T) {
	server := newAPIServer(config{}, os.Stdout, os.Stderr)

	request := httptest.NewRequest(http.MethodPost, "/api/health", nil)
	recorder := httptest.NewRecorder()

	server.handleHealth(recorder, request)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("unexpected status code: got %d, want %d", recorder.Code, http.StatusMethodNotAllowed)
	}
}

func TestSelectLatestResticSnapshotChoosesNewestByTime(t *testing.T) {
	snapshots := []resticSnapshotSummary{
		{ID: "old", Time: time.Date(2026, time.April, 10, 12, 0, 0, 0, time.UTC)},
		{ID: "new", Time: time.Date(2026, time.April, 10, 14, 0, 0, 0, time.UTC)},
		{ID: "mid", Time: time.Date(2026, time.April, 10, 13, 0, 0, 0, time.UTC)},
	}

	latest, err := selectLatestResticSnapshot(snapshots)
	if err != nil {
		t.Fatalf("selectLatestResticSnapshot() error = %v", err)
	}

	if latest.ID != "new" {
		t.Fatalf("unexpected latest snapshot: %#v", latest)
	}
}

func TestRestoreLatestResticSnapshotUsesNewestMatchingTag(t *testing.T) {
	t.Setenv(resticPasswordEnv, "secret")

	dir := t.TempDir()
	argsPath := filepath.Join(dir, "args.txt")
	binaryPath := filepath.Join(dir, "fake-restic.sh")
	t.Setenv("TEST_ARGS_FILE", argsPath)

	script := `#!/usr/bin/env bash
set -eu
printf '%s ' "$@" >> "$TEST_ARGS_FILE"
printf '\n' >> "$TEST_ARGS_FILE"
if [ "${3:-}" = "snapshots" ]; then
  printf '%s\n' '[{"id":"old","time":"2026-04-10T12:00:00Z","tags":["a"]},{"id":"new","time":"2026-04-10T14:00:00Z","tags":["a"]}]'
  exit 0
fi
exit 0
`

	if err := os.WriteFile(binaryPath, []byte(script), 0o755); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	runner := newTemplateRunner(config{
		Restic: resticConfig{
			Binary:     binaryPath,
			Repository: "/tmp/restic-repo",
		},
	}, os.Stdout, os.Stderr)

	snapshot, err := runner.findLatestResticSnapshot(context.Background(), []string{"a"})
	if err != nil {
		t.Fatalf("findLatestResticSnapshot() error = %v", err)
	}
	if snapshot.ID != "new" {
		t.Fatalf("unexpected selected snapshot: %#v", snapshot)
	}

	if err := runner.restoreResticSnapshot(context.Background(), snapshot.ID, "/tmp/restore_a"); err != nil {
		t.Fatalf("restoreResticSnapshot() error = %v", err)
	}

	logBytes, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(logBytes)), "\n")
	if len(lines) != 2 {
		t.Fatalf("unexpected command log: %q", string(logBytes))
	}

	firstCall := strings.Fields(lines[0])
	wantSnapshotsCall := []string{"-r", "/tmp/restic-repo", "snapshots", "--json", "--tag", "a"}
	if len(firstCall) != len(wantSnapshotsCall) {
		t.Fatalf("unexpected snapshots call: got %#v, want %#v", firstCall, wantSnapshotsCall)
	}
	for i := range wantSnapshotsCall {
		if firstCall[i] != wantSnapshotsCall[i] {
			t.Fatalf("unexpected snapshots call: got %#v, want %#v", firstCall, wantSnapshotsCall)
		}
	}

	secondCall := strings.Fields(lines[1])
	wantRestoreCall := []string{"-r", "/tmp/restic-repo", "restore", "new", "--target", "/tmp/restore_a"}
	if len(secondCall) != len(wantRestoreCall) {
		t.Fatalf("unexpected restore call: got %#v, want %#v", secondCall, wantRestoreCall)
	}
	for i := range wantRestoreCall {
		if secondCall[i] != wantRestoreCall[i] {
			t.Fatalf("unexpected restore call: got %#v, want %#v", secondCall, wantRestoreCall)
		}
	}
}

func TestRunResticBackupJobRecoversFromStaleLock(t *testing.T) {
	t.Setenv(resticPasswordEnv, "restic-secret")

	dir := t.TempDir()
	argsPath := filepath.Join(dir, "restic-args.txt")
	statePath := filepath.Join(dir, "restic-state.txt")
	resticBinaryPath := filepath.Join(dir, "fake-restic.sh")
	t.Setenv("TEST_ARGS_FILE", argsPath)
	t.Setenv("TEST_STATE_FILE", statePath)

	resticScript := `#!/usr/bin/env bash
set -eu
printf '%s ' "$@" >> "$TEST_ARGS_FILE"
printf '\n' >> "$TEST_ARGS_FILE"

command_name="${3:-}"
state_file="${TEST_STATE_FILE}"

attempt=0
if [ -f "$state_file" ]; then
  attempt=$(cat "$state_file")
fi

if [ "$command_name" = "backup" ] && [ "$attempt" -eq 0 ]; then
  printf '%s' '1' > "$state_file"
  printf '%s\n' 'unable to create lock in backend: repository is already locked exclusively by PID 1234'
  exit 1
fi

if [ "$command_name" = "unlock" ]; then
  printf '%s\n' 'successfully removed locks'
fi
`
	if err := os.WriteFile(resticBinaryPath, []byte(resticScript), 0o755); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	runner := newTemplateRunner(config{
		Restic: resticConfig{
			Binary:     resticBinaryPath,
			Repository: "/tmp/restic-repo",
		},
	}, os.Stdout, os.Stderr)

	job := jobConfig{
		Name:           "restic_uploads",
		Type:           "restic_backup",
		Schedule:       "15 * * * *",
		TimeoutMinutes: 180,
		Sources:        []string{"/data/uploads"},
		Tags:           []string{"uploads"},
	}

	if err := runner.runResticBackupJob(context.Background(), job); err != nil {
		t.Fatalf("runResticBackupJob() error = %v", err)
	}

	logBytes, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(logBytes)), "\n")
	if len(lines) != 3 {
		t.Fatalf("unexpected restic command log: %q", string(logBytes))
	}

	wantCalls := [][]string{
		{"-r", "/tmp/restic-repo", "backup", "--tag", "uploads", "/data/uploads"},
		{"-r", "/tmp/restic-repo", "unlock"},
		{"-r", "/tmp/restic-repo", "backup", "--tag", "uploads", "/data/uploads"},
	}

	for i, wantCall := range wantCalls {
		gotCall := strings.Fields(lines[i])
		if len(gotCall) != len(wantCall) {
			t.Fatalf("unexpected call %d: got %#v, want %#v", i, gotCall, wantCall)
		}
		for j := range wantCall {
			if gotCall[j] != wantCall[j] {
				t.Fatalf("unexpected call %d: got %#v, want %#v", i, gotCall, wantCall)
			}
		}
	}
}

func TestListResticSnapshotSummariesRecoversFromStaleLock(t *testing.T) {
	t.Setenv(resticPasswordEnv, "restic-secret")

	dir := t.TempDir()
	argsPath := filepath.Join(dir, "restic-args.txt")
	statePath := filepath.Join(dir, "restic-state.txt")
	resticBinaryPath := filepath.Join(dir, "fake-restic.sh")
	t.Setenv("TEST_ARGS_FILE", argsPath)
	t.Setenv("TEST_STATE_FILE", statePath)

	resticScript := `#!/usr/bin/env bash
set -eu
printf '%s ' "$@" >> "$TEST_ARGS_FILE"
printf '\n' >> "$TEST_ARGS_FILE"

command_name="${3:-}"
state_file="${TEST_STATE_FILE}"

attempt=0
if [ -f "$state_file" ]; then
  attempt=$(cat "$state_file")
fi

if [ "$command_name" = "snapshots" ] && [ "$attempt" -eq 0 ]; then
  printf '%s' '1' > "$state_file"
  printf '%s\n' 'unable to create lock in backend: repository is already locked exclusively by PID 1234'
  exit 1
fi

if [ "$command_name" = "unlock" ]; then
  printf '%s\n' 'successfully removed locks'
  exit 0
fi

printf '%s\n' '[{"id":"snapshot-1","time":"2026-04-10T14:00:00Z","tags":["uploads"]}]'
`
	if err := os.WriteFile(resticBinaryPath, []byte(resticScript), 0o755); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	runner := newTemplateRunner(config{
		Restic: resticConfig{
			Binary:     resticBinaryPath,
			Repository: "/tmp/restic-repo",
		},
	}, os.Stdout, os.Stderr)

	snapshots, err := runner.listResticSnapshotSummaries(context.Background(), []string{"uploads"})
	if err != nil {
		t.Fatalf("listResticSnapshotSummaries() error = %v", err)
	}
	if len(snapshots) != 1 || snapshots[0].ID != "snapshot-1" {
		t.Fatalf("unexpected snapshots: %#v", snapshots)
	}

	logBytes, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(logBytes)), "\n")
	if len(lines) != 3 {
		t.Fatalf("unexpected restic command log: %q", string(logBytes))
	}

	wantCalls := [][]string{
		{"-r", "/tmp/restic-repo", "snapshots", "--json", "--tag", "uploads"},
		{"-r", "/tmp/restic-repo", "unlock"},
		{"-r", "/tmp/restic-repo", "snapshots", "--json", "--tag", "uploads"},
	}

	for i, wantCall := range wantCalls {
		gotCall := strings.Fields(lines[i])
		if len(gotCall) != len(wantCall) {
			t.Fatalf("unexpected call %d: got %#v, want %#v", i, gotCall, wantCall)
		}
		for j := range wantCall {
			if gotCall[j] != wantCall[j] {
				t.Fatalf("unexpected call %d: got %#v, want %#v", i, gotCall, wantCall)
			}
		}
	}
}

func TestResticOperationsAreSerialized(t *testing.T) {
	t.Setenv(resticPasswordEnv, "secret")

	dir := t.TempDir()
	binaryPath := filepath.Join(dir, "fake-restic.sh")
	lockPath := filepath.Join(dir, "restic.lock")
	maxPath := filepath.Join(dir, "max.txt")
	t.Setenv("TEST_RESTIC_LOCK_FILE", lockPath)
	t.Setenv("TEST_RESTIC_MAX_FILE", maxPath)

	script := `#!/usr/bin/env bash
set -eu
lock_file="${TEST_RESTIC_LOCK_FILE}"
max_file="${TEST_RESTIC_MAX_FILE}"

count=0
if [ -f "$lock_file" ]; then
  count=$(cat "$lock_file")
fi
count=$((count + 1))
printf '%s' "$count" > "$lock_file"

max_seen=0
if [ -f "$max_file" ]; then
  max_seen=$(cat "$max_file")
fi
if [ "$count" -gt "$max_seen" ]; then
  printf '%s' "$count" > "$max_file"
fi

sleep 1

count=$(cat "$lock_file")
count=$((count - 1))
printf '%s' "$count" > "$lock_file"

printf '%s\n' '[]'
`
	if err := os.WriteFile(binaryPath, []byte(script), 0o755); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	runner := newTemplateRunner(config{
		Restic: resticConfig{
			Binary:     binaryPath,
			Repository: "/tmp/restic-repo",
		},
	}, os.Stdout, os.Stderr)

	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := runner.listResticSnapshotSummaries(context.Background(), nil)
			errCh <- err
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("listResticSnapshotSummaries() error = %v", err)
		}
	}

	maxBytes, err := os.ReadFile(maxPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	maxSeen, err := strconv.Atoi(strings.TrimSpace(string(maxBytes)))
	if err != nil {
		t.Fatalf("Atoi() error = %v", err)
	}
	if maxSeen != 1 {
		t.Fatalf("expected serialized restic access, max concurrency = %d", maxSeen)
	}
}

func TestRunDatabaseDumpJobCreatesDumpAndAppliesRetention(t *testing.T) {
	dir := t.TempDir()
	previousWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd() error = %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("Chdir() error = %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(previousWD)
	})

	binaryPath := filepath.Join(dir, "mariadb-dump")
	script := `#!/usr/bin/env bash
set -eu
printf '%s\n' 'dump-content'
`
	if err := os.WriteFile(binaryPath, []byte(script), 0o755); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	t.Setenv("BACKUP_DB_PASSWORD", "secret")

	oldDir := filepath.Join(dir, "backups", "db_job")
	if err := os.MkdirAll(oldDir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	oldFile := filepath.Join(oldDir, "old.sql.gz")
	if err := os.WriteFile(oldFile, []byte("old"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	oldTime := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(oldFile, oldTime, oldTime); err != nil {
		t.Fatalf("Chtimes() error = %v", err)
	}

	runner := newTemplateRunner(config{
		Database: databaseConfigList{{
			Type: "mariadb",
			Gzip: true,
		}},
	}, os.Stdout, os.Stderr)

	job := jobConfig{
		Name:           "db_job",
		Type:           "database_dump",
		Schedule:       "0 2 * * *",
		TimeoutMinutes: 90,
		DatabaseName:   "myapp",
		Host:           "127.0.0.1",
		Port:           3306,
		User:           "backup_user",
		Password:       "BACKUP_DB_PASSWORD",
		Retention: &jobRetentionConfig{
			KeepHourly: 1,
		},
	}

	if err := runner.runDatabaseDumpJob(context.Background(), job); err != nil {
		t.Fatalf("runDatabaseDumpJob() error = %v", err)
	}

	entries, err := os.ReadDir(oldDir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 dump file after pruning, got %d", len(entries))
	}
	if entries[0].Name() == "old.sql.gz" {
		t.Fatalf("expected old dump file to be pruned")
	}

	dumpPath := filepath.Join(oldDir, entries[0].Name())
	file, err := os.Open(dumpPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer file.Close()

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatalf("gzip.NewReader() error = %v", err)
	}
	defer gzipReader.Close()

	content, err := io.ReadAll(gzipReader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if strings.TrimSpace(string(content)) != "dump-content" {
		t.Fatalf("unexpected dump content: %q", string(content))
	}
}

func TestRunDatabaseDumpJobRetriesTemporaryFailures(t *testing.T) {
	dir := t.TempDir()
	previousWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd() error = %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("Chdir() error = %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(previousWD)
	})

	attemptsPath := filepath.Join(dir, "attempts.txt")
	t.Setenv("TEST_DUMP_ATTEMPTS_FILE", attemptsPath)

	binaryPath := filepath.Join(dir, "mariadb-dump")
	script := `#!/usr/bin/env bash
set -eu
attempt_file="${TEST_DUMP_ATTEMPTS_FILE}"
attempt=0
if [ -f "$attempt_file" ]; then
  attempt=$(cat "$attempt_file")
fi
attempt=$((attempt + 1))
printf '%s' "$attempt" > "$attempt_file"

if [ "$attempt" -lt 3 ]; then
  printf '%s\n' 'temporary database failure' >&2
  exit 1
fi

printf '%s\n' 'dump-content'
`
	if err := os.WriteFile(binaryPath, []byte(script), 0o755); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	t.Setenv("BACKUP_DB_PASSWORD", "secret")

	runner := newTemplateRunner(config{
		Database: databaseConfigList{{
			Type: "mariadb",
			Gzip: false,
		}},
	}, os.Stdout, os.Stderr)

	var gotSleeps []time.Duration
	runner.sleep = func(ctx context.Context, delay time.Duration) error {
		gotSleeps = append(gotSleeps, delay)
		return nil
	}

	job := jobConfig{
		Name:           "db_job",
		Type:           "database_dump",
		Schedule:       "0 2 * * *",
		TimeoutMinutes: 90,
		DatabaseName:   "myapp",
		Host:           "127.0.0.1",
		Port:           3306,
		User:           "backup_user",
		Password:       "BACKUP_DB_PASSWORD",
	}

	if err := runner.runDatabaseDumpJob(context.Background(), job); err != nil {
		t.Fatalf("runDatabaseDumpJob() error = %v", err)
	}

	attemptBytes, err := os.ReadFile(attemptsPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if strings.TrimSpace(string(attemptBytes)) != "3" {
		t.Fatalf("expected 3 dump attempts, got %q", string(attemptBytes))
	}

	wantSleeps := []time.Duration{15 * time.Second, 30 * time.Second}
	if len(gotSleeps) != len(wantSleeps) {
		t.Fatalf("unexpected retry sleeps: got %#v, want %#v", gotSleeps, wantSleeps)
	}
	for i := range wantSleeps {
		if gotSleeps[i] != wantSleeps[i] {
			t.Fatalf("unexpected retry sleeps: got %#v, want %#v", gotSleeps, wantSleeps)
		}
	}

	entries, err := os.ReadDir(filepath.Join(dir, "backups", "db_job"))
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 dump file, got %d", len(entries))
	}

	dumpPath := filepath.Join(dir, "backups", "db_job", entries[0].Name())
	content, err := os.ReadFile(dumpPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if strings.TrimSpace(string(content)) != "dump-content" {
		t.Fatalf("unexpected dump content: %q", string(content))
	}
}

func TestRunDatabaseDumpJobUsesSelectedPostgresConfig(t *testing.T) {
	dir := t.TempDir()
	previousWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd() error = %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("Chdir() error = %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(previousWD)
	})

	argsPath := filepath.Join(dir, "args.txt")
	passwordPath := filepath.Join(dir, "pgpassword.txt")
	t.Setenv("TEST_ARGS_FILE", argsPath)
	t.Setenv("TEST_PGPASSWORD_FILE", passwordPath)

	binaryPath := filepath.Join(dir, "pg_dump")
	script := `#!/usr/bin/env bash
set -eu
printf '%s ' "$@" > "$TEST_ARGS_FILE"
printf '\n' >> "$TEST_ARGS_FILE"
printf '%s' "${PGPASSWORD:-}" > "$TEST_PGPASSWORD_FILE"
printf '%s\n' 'dump-content'
`
	if err := os.WriteFile(binaryPath, []byte(script), 0o755); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	t.Setenv("BACKUP_DB_PASSWORD", "secret")

	runner := newTemplateRunner(config{
		Database: databaseConfigList{
			{Name: "mysql", Type: "mariadb", Gzip: true},
			{Name: "postgres", Type: "postgres", Gzip: false},
		},
	}, os.Stdout, os.Stderr)

	job := jobConfig{
		Name:           "db_job",
		Type:           "database_dump",
		Schedule:       "0 2 * * *",
		TimeoutMinutes: 90,
		DatabaseConfig: "postgres",
		DatabaseName:   "myapp",
		Host:           "127.0.0.1",
		Port:           5432,
		User:           "backup_user",
		Password:       "BACKUP_DB_PASSWORD",
		Tables:         []string{"users"},
		ExcludeTables:  []string{"sessions"},
	}

	if err := runner.runDatabaseDumpJob(context.Background(), job); err != nil {
		t.Fatalf("runDatabaseDumpJob() error = %v", err)
	}

	argsBytes, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	gotArgs := strings.Fields(strings.TrimSpace(string(argsBytes)))
	wantArgs := []string{"--clean", "--if-exists", "-h", "127.0.0.1", "-p", "5432", "-U", "backup_user", "-d", "myapp", "-t", "users", "-T", "sessions"}
	if len(gotArgs) != len(wantArgs) {
		t.Fatalf("unexpected pg_dump args: got %#v, want %#v", gotArgs, wantArgs)
	}
	for i := range wantArgs {
		if gotArgs[i] != wantArgs[i] {
			t.Fatalf("unexpected pg_dump args: got %#v, want %#v", gotArgs, wantArgs)
		}
	}

	passwordBytes, err := os.ReadFile(passwordPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if strings.TrimSpace(string(passwordBytes)) != "secret" {
		t.Fatalf("unexpected PGPASSWORD: %q", string(passwordBytes))
	}

	entries, err := os.ReadDir(filepath.Join(dir, "backups", "db_job"))
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 dump file, got %d", len(entries))
	}

	dumpPath := filepath.Join(dir, "backups", "db_job", entries[0].Name())
	content, err := os.ReadFile(dumpPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if strings.TrimSpace(string(content)) != "dump-content" {
		t.Fatalf("unexpected dump content: %q", string(content))
	}
}

func TestRunDatabaseDumpJobAlsoCreatesResticBackupWhenEnabled(t *testing.T) {
	dir := t.TempDir()
	previousWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd() error = %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("Chdir() error = %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(previousWD)
	})

	dumpBinaryPath := filepath.Join(dir, "mariadb-dump")
	dumpScript := `#!/usr/bin/env bash
set -eu
printf '%s\n' 'dump-content'
`
	if err := os.WriteFile(dumpBinaryPath, []byte(dumpScript), 0o755); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	argsPath := filepath.Join(dir, "restic-args.txt")
	resticBinaryPath := filepath.Join(dir, "fake-restic.sh")
	t.Setenv("TEST_ARGS_FILE", argsPath)
	resticScript := `#!/usr/bin/env bash
set -eu
printf '%s ' "$@" >> "$TEST_ARGS_FILE"
printf '\n' >> "$TEST_ARGS_FILE"
`
	if err := os.WriteFile(resticBinaryPath, []byte(resticScript), 0o755); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	t.Setenv("BACKUP_DB_PASSWORD", "secret")
	t.Setenv(resticPasswordEnv, "restic-secret")

	runner := newTemplateRunner(config{
		Database: databaseConfigList{{
			Type: "mariadb",
			Gzip: true,
		}},
		Restic: resticConfig{
			Binary:     resticBinaryPath,
			Repository: "/tmp/restic-repo",
		},
	}, os.Stdout, os.Stderr)

	job := jobConfig{
		Name:           "db_job",
		Type:           "database_dump",
		Schedule:       "0 2 * * *",
		TimeoutMinutes: 90,
		DatabaseName:   "myapp",
		Host:           "127.0.0.1",
		Port:           3306,
		User:           "backup_user",
		Password:       "BACKUP_DB_PASSWORD",
		Retention: &jobRetentionConfig{
			KeepHourly: 1,
		},
	}

	if err := runner.runDatabaseDumpJob(context.Background(), job); err != nil {
		t.Fatalf("runDatabaseDumpJob() error = %v", err)
	}

	logBytes, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(logBytes)), "\n")
	if len(lines) != 2 {
		t.Fatalf("unexpected restic command log: %q", string(logBytes))
	}

	firstCall := strings.Fields(lines[0])
	wantBackupCall := []string{"-r", "/tmp/restic-repo", "backup", "--tag", "db_job", filepath.Join("backups", "db_job")}
	if len(firstCall) != len(wantBackupCall) {
		t.Fatalf("unexpected backup call: got %#v, want %#v", firstCall, wantBackupCall)
	}
	for i := range wantBackupCall {
		if firstCall[i] != wantBackupCall[i] {
			t.Fatalf("unexpected backup call: got %#v, want %#v", firstCall, wantBackupCall)
		}
	}

	secondCall := strings.Fields(lines[1])
	wantForgetCall := []string{"-r", "/tmp/restic-repo", "forget", "--tag", "db_job", "--keep-hourly", "1"}
	if len(secondCall) != len(wantForgetCall) {
		t.Fatalf("unexpected forget call: got %#v, want %#v", secondCall, wantForgetCall)
	}
	for i := range wantForgetCall {
		if secondCall[i] != wantForgetCall[i] {
			t.Fatalf("unexpected forget call: got %#v, want %#v", secondCall, wantForgetCall)
		}
	}
}

func TestRunResticBackupJobPassesExcludePatterns(t *testing.T) {
	t.Setenv(resticPasswordEnv, "restic-secret")

	dir := t.TempDir()
	argsPath := filepath.Join(dir, "restic-args.txt")
	resticBinaryPath := filepath.Join(dir, "fake-restic.sh")
	t.Setenv("TEST_ARGS_FILE", argsPath)
	resticScript := `#!/usr/bin/env bash
set -eu
printf '%s ' "$@" >> "$TEST_ARGS_FILE"
printf '\n' >> "$TEST_ARGS_FILE"
`
	if err := os.WriteFile(resticBinaryPath, []byte(resticScript), 0o755); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	runner := newTemplateRunner(config{
		Restic: resticConfig{
			Binary:     resticBinaryPath,
			Repository: "/tmp/restic-repo",
		},
	}, os.Stdout, os.Stderr)

	job := jobConfig{
		Name:           "restic_uploads",
		Type:           "restic_backup",
		Schedule:       "15 * * * *",
		TimeoutMinutes: 180,
		Sources:        []string{"/data/uploads"},
		Exclude:        []string{"cache", "*.tmp"},
		Tags:           []string{"uploads"},
		Retention: &jobRetentionConfig{
			KeepDaily: 14,
		},
	}

	if err := runner.runResticBackupJob(context.Background(), job); err != nil {
		t.Fatalf("runResticBackupJob() error = %v", err)
	}

	logBytes, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(logBytes)), "\n")
	if len(lines) != 2 {
		t.Fatalf("unexpected restic command log: %q", string(logBytes))
	}

	firstCall := strings.Fields(lines[0])
	wantBackupCall := []string{"-r", "/tmp/restic-repo", "backup", "--tag", "uploads", "--exclude", "cache", "--exclude", "*.tmp", "/data/uploads"}
	if len(firstCall) != len(wantBackupCall) {
		t.Fatalf("unexpected backup call: got %#v, want %#v", firstCall, wantBackupCall)
	}
	for i := range wantBackupCall {
		if firstCall[i] != wantBackupCall[i] {
			t.Fatalf("unexpected backup call: got %#v, want %#v", firstCall, wantBackupCall)
		}
	}

	secondCall := strings.Fields(lines[1])
	wantForgetCall := []string{"-r", "/tmp/restic-repo", "forget", "--tag", "uploads", "--keep-daily", "14"}
	if len(secondCall) != len(wantForgetCall) {
		t.Fatalf("unexpected forget call: got %#v, want %#v", secondCall, wantForgetCall)
	}
	for i := range wantForgetCall {
		if secondCall[i] != wantForgetCall[i] {
			t.Fatalf("unexpected forget call: got %#v, want %#v", secondCall, wantForgetCall)
		}
	}
}

func TestRunResticBackupJobRunsExplicitPruneOnceAfterForget(t *testing.T) {
	t.Setenv(resticPasswordEnv, "restic-secret")

	dir := t.TempDir()
	argsPath := filepath.Join(dir, "restic-args.txt")
	resticBinaryPath := filepath.Join(dir, "fake-restic.sh")
	t.Setenv("TEST_ARGS_FILE", argsPath)
	resticScript := `#!/usr/bin/env bash
set -eu
printf '%s ' "$@" >> "$TEST_ARGS_FILE"
printf '\n' >> "$TEST_ARGS_FILE"
`
	if err := os.WriteFile(resticBinaryPath, []byte(resticScript), 0o755); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	runner := newTemplateRunner(config{
		Restic: resticConfig{
			Binary:     resticBinaryPath,
			Repository: "/tmp/restic-repo",
		},
	}, os.Stdout, os.Stderr)

	job := jobConfig{
		Name:           "restic_uploads",
		Type:           "restic_backup",
		Schedule:       "15 * * * *",
		TimeoutMinutes: 180,
		Sources:        []string{"/data/uploads"},
		Tags:           []string{"uploads"},
		Retention: &jobRetentionConfig{
			KeepDaily: 14,
		},
		ResticPrune: true,
	}

	if err := runner.runResticBackupJob(context.Background(), job); err != nil {
		t.Fatalf("runResticBackupJob() error = %v", err)
	}

	logBytes, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(logBytes)), "\n")
	if len(lines) != 3 {
		t.Fatalf("unexpected restic command log: %q", string(logBytes))
	}

	thirdCall := strings.Fields(lines[2])
	wantPruneCall := []string{"-r", "/tmp/restic-repo", "prune"}
	if len(thirdCall) != len(wantPruneCall) {
		t.Fatalf("unexpected prune call: got %#v, want %#v", thirdCall, wantPruneCall)
	}
	for i := range wantPruneCall {
		if thirdCall[i] != wantPruneCall[i] {
			t.Fatalf("unexpected prune call: got %#v, want %#v", thirdCall, wantPruneCall)
		}
	}
}

func TestPruneDatabaseDumpFilesKeepsHourlyAndDailyBuckets(t *testing.T) {
	dir := t.TempDir()
	location := time.UTC
	now := time.Date(2026, 4, 10, 12, 30, 0, 0, location)

	files := map[string]time.Time{
		"hour-11-old.sql.gz":      time.Date(2026, 4, 10, 11, 5, 0, 0, location),
		"hour-11-latest.sql.gz":   time.Date(2026, 4, 10, 11, 45, 0, 0, location),
		"hour-10.sql.gz":          time.Date(2026, 4, 10, 10, 15, 0, 0, location),
		"hour-09.sql.gz":          time.Date(2026, 4, 10, 9, 30, 0, 0, location),
		"day-09-latest.sql.gz":    time.Date(2026, 4, 9, 22, 0, 0, 0, location),
		"day-08-latest.sql.gz":    time.Date(2026, 4, 8, 21, 0, 0, 0, location),
		"day-07-should-prune.sql": time.Date(2026, 4, 7, 20, 0, 0, 0, location),
	}

	for name, modTime := range files {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(name), 0o644); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", name, err)
		}
		if err := os.Chtimes(path, modTime, modTime); err != nil {
			t.Fatalf("Chtimes(%s) error = %v", name, err)
		}
	}

	retention := &jobRetentionConfig{
		KeepHourly: 3,
		KeepDaily:  3,
	}
	if err := pruneDatabaseDumpFiles(dir, retention, now); err != nil {
		t.Fatalf("pruneDatabaseDumpFiles() error = %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}

	got := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		got[entry.Name()] = struct{}{}
	}

	for _, name := range []string{
		"hour-11-latest.sql.gz",
		"hour-10.sql.gz",
		"hour-09.sql.gz",
		"day-09-latest.sql.gz",
		"day-08-latest.sql.gz",
	} {
		if _, ok := got[name]; !ok {
			t.Fatalf("expected %s to be kept, remaining files: %#v", name, got)
		}
	}

	for _, name := range []string{
		"hour-11-old.sql.gz",
		"day-07-should-prune.sql",
	} {
		if _, ok := got[name]; ok {
			t.Fatalf("expected %s to be pruned, remaining files: %#v", name, got)
		}
	}
}
