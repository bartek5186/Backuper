package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfigSupportsMultipleDatabasesAndDirectories(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")

	content := `{
  "app": {
    "name": "backuper",
    "timezone": "Europe/Warsaw"
  },
  "databases": [
    {
      "name": "app_main",
      "database_name": "myapp",
      "dump_tool": "mariadb-dump",
      "host": "127.0.0.1",
      "port": 3306,
      "username": "backup_user",
      "password_env": "BACKUP_DB_PASSWORD",
      "tables": ["users", "orders"],
      "gzip": true,
      "output_dir": "/var/backups/backuper/db/app_main"
    }
  ],
  "directories": [
    {
      "name": "uploads_public",
      "path": "/srv/myapp/uploads/public",
      "excludes": ["*.tmp"]
    }
  ],
  "restic": {
    "binary": "/usr/bin/restic",
    "repository": "sftp:backup@100.88.10.24:/volume1/backups/restic/myapp",
    "password_env": "RESTIC_PASSWORD"
  },
  "jobs": [
    {
      "name": "db_app_main_dump",
      "type": "database_dump",
      "schedule": "0 2 * * *",
      "timeout_minutes": 90,
      "database_ref": "app_main"
    },
    {
      "name": "restic_uploads",
      "type": "restic_backup",
      "schedule": "15 * * * *",
      "timeout_minutes": 180,
      "directory_refs": ["uploads_public"]
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

func TestValidateRejectsUnknownDatabaseRef(t *testing.T) {
	cfg := config{
		App: appConfig{
			Name:     "backuper",
			Timezone: "Europe/Warsaw",
		},
		Databases: []databaseConfig{
			{
				Name:        "app_main",
				DumpTool:    "mariadb-dump",
				Host:        "127.0.0.1",
				Port:        3306,
				Username:    "backup_user",
				PasswordEnv: "BACKUP_DB_PASSWORD",
				OutputDir:   "/var/backups/backuper/db/app_main",
			},
		},
		Jobs: []jobConfig{
			{
				Name:           "db_missing",
				Type:           "database_dump",
				Schedule:       "0 2 * * *",
				TimeoutMinutes: 90,
				DatabaseRef:    "missing",
			},
		},
	}

	err := cfg.validate()
	if err == nil {
		t.Fatal("validate() returned nil error for unknown database ref")
	}

	if !strings.Contains(err.Error(), `database_ref "missing"`) {
		t.Fatalf("expected unknown database_ref error, got %v", err)
	}
}

func TestValidateAllowsLegacySingleDatabaseConfig(t *testing.T) {
	cfg := config{
		App: appConfig{
			Name:     "backuper",
			Timezone: "Europe/Warsaw",
		},
		Database: legacyDatabaseConfig{
			DumpTool:    "mariadb-dump",
			Host:        "127.0.0.1",
			Port:        3306,
			Name:        "myapp",
			Username:    "backup_user",
			PasswordEnv: "BACKUP_DB_PASSWORD",
		},
		Jobs: []jobConfig{
			{
				Name:           "db_dump",
				Type:           "database_dump",
				Schedule:       "0 2 * * *",
				TimeoutMinutes: 90,
				OutputDir:      "/var/backups/myapp/db",
			},
		},
	}

	if err := cfg.validate(); err != nil {
		t.Fatalf("validate() error = %v", err)
	}
}
