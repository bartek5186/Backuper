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
	"strings"
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
    "dump_tool": "mariadb-dump",
    "dump_flags": ["--single-transaction", "--quick"],
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
		Database: databaseDefaultsConfig{
			DumpTool: "mariadb-dump",
		},
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
		Database: databaseDefaultsConfig{
			DumpTool: "mariadb-dump",
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
				OutputDir:      "/var/backups/myapp/db",
			},
		},
	}

	if err := cfg.validate(); err != nil {
		t.Fatalf("validate() error = %v", err)
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

func TestValidateRejectsRetentionForDatabaseDump(t *testing.T) {
	cfg := config{
		App: appConfig{
			Name:     "backuper",
			Timezone: "Europe/Warsaw",
		},
		Database: databaseDefaultsConfig{
			DumpTool: "mariadb-dump",
		},
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
					KeepDaily: 14,
				},
			},
		},
	}

	err := cfg.validate()
	if err == nil {
		t.Fatal("validate() returned nil error for database_dump retention")
	}

	if !strings.Contains(err.Error(), "retention is only supported for restic_backup jobs") {
		t.Fatalf("expected retention error, got %v", err)
	}
}

func TestValidateRejectsRetentionDaysForResticBackup(t *testing.T) {
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
				RetentionDays:  7,
			},
		},
	}

	err := cfg.validate()
	if err == nil {
		t.Fatal("validate() returned nil error for restic retention_days")
	}

	if !strings.Contains(err.Error(), "retention_days is only supported for database_dump jobs") {
		t.Fatalf("expected retention_days error, got %v", err)
	}
}

func TestCheckRuntimeDependenciesRejectsMissingBinaries(t *testing.T) {
	cfg := config{
		Database: databaseDefaultsConfig{
			DumpTool: "/tmp/definitely-missing-dump-tool",
		},
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
	if !strings.Contains(err.Error(), "database.dump_tool") {
		t.Fatalf("expected dump tool error, got %v", err)
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

func TestRunDatabaseDumpJobCreatesDumpAndPrunesOldFiles(t *testing.T) {
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

	binaryPath := filepath.Join(dir, "fake-dump.sh")
	script := `#!/usr/bin/env bash
set -eu
printf '%s\n' 'dump-content'
`
	if err := os.WriteFile(binaryPath, []byte(script), 0o755); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	t.Setenv("BACKUP_DB_PASSWORD", "secret")

	oldDir := filepath.Join(dir, "backups", "db_job")
	if err := os.MkdirAll(oldDir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	oldFile := filepath.Join(oldDir, "old.sql.gz")
	if err := os.WriteFile(oldFile, []byte("old"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	oldTime := time.Now().AddDate(0, 0, -10)
	if err := os.Chtimes(oldFile, oldTime, oldTime); err != nil {
		t.Fatalf("Chtimes() error = %v", err)
	}

	runner := newTemplateRunner(config{
		Database: databaseDefaultsConfig{
			DumpTool: binaryPath,
			Gzip:     true,
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
		RetentionDays:  7,
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
