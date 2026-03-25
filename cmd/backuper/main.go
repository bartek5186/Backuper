package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

const version = "0.1.0"

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		printUsage(os.Stderr)
		return 2
	}

	switch args[0] {
	case "validate":
		return runValidate(args[1:])
	case "version":
		fmt.Fprintln(os.Stdout, version)
		return 0
	case "help", "-h", "--help":
		printUsage(os.Stdout)
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", args[0])
		printUsage(os.Stderr)
		return 2
	}
}

func runValidate(args []string) int {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	configPath := fs.String("config", "", "path to JSON config file")

	if err := fs.Parse(args); err != nil {
		return 2
	}

	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "missing required -config flag")
		return 2
	}

	_, err := loadConfig(*configPath)
	if err != nil {
		var validationErr *validationError
		if errors.As(err, &validationErr) {
			fmt.Fprintf(os.Stderr, "config validation failed:\n%s\n", validationErr.pretty())
			return 1
		}

		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		return 1
	}

	fmt.Fprintf(os.Stdout, "config %s is valid\n", *configPath)
	return 0
}

func printUsage(out *os.File) {
	fmt.Fprintln(out, "Backuper")
	fmt.Fprintln(out)
	fmt.Fprintln(out, "Usage:")
	fmt.Fprintln(out, "  backuper validate -config /path/to/config.json")
	fmt.Fprintln(out, "  backuper version")
}

type config struct {
	App          appConfig            `json:"app"`
	Paths        pathsConfig          `json:"paths"`
	Database     legacyDatabaseConfig `json:"database"`
	Databases    []databaseConfig     `json:"databases"`
	Directories  []directoryConfig    `json:"directories"`
	Restic       resticConfig         `json:"restic"`
	Healthchecks healthchecksConfig   `json:"healthchecks"`
	Jobs         []jobConfig          `json:"jobs"`
	Retention    retentionConfig      `json:"retention"`
}

type appConfig struct {
	Name     string `json:"name"`
	Timezone string `json:"timezone"`
}

type pathsConfig struct {
	DBDumpDir  string `json:"db_dump_dir"`
	UploadsDir string `json:"uploads_dir"`
	VideosDir  string `json:"videos_dir"`
}

type legacyDatabaseConfig struct {
	DumpTool    string   `json:"dump_tool"`
	Host        string   `json:"host"`
	Port        int      `json:"port"`
	Name        string   `json:"name"`
	Username    string   `json:"username"`
	PasswordEnv string   `json:"password_env"`
	DumpFlags   []string `json:"dump_flags"`
	Gzip        bool     `json:"gzip"`
}

type databaseConfig struct {
	Name          string   `json:"name"`
	DatabaseName  string   `json:"database_name,omitempty"`
	DumpTool      string   `json:"dump_tool"`
	Host          string   `json:"host"`
	Port          int      `json:"port"`
	Username      string   `json:"username"`
	PasswordEnv   string   `json:"password_env"`
	Tables        []string `json:"tables,omitempty"`
	ExcludeTables []string `json:"exclude_tables,omitempty"`
	DumpFlags     []string `json:"dump_flags,omitempty"`
	Gzip          bool     `json:"gzip"`
	OutputDir     string   `json:"output_dir"`
}

type resticConfig struct {
	Binary      string `json:"binary"`
	Repository  string `json:"repository"`
	PasswordEnv string `json:"password_env"`
}

type healthchecksConfig struct {
	BaseURL string            `json:"base_url"`
	Jobs    map[string]string `json:"jobs"`
}

type directoryConfig struct {
	Name     string   `json:"name"`
	Path     string   `json:"path"`
	Excludes []string `json:"excludes"`
}

type jobConfig struct {
	Name           string   `json:"name"`
	Type           string   `json:"type"`
	Schedule       string   `json:"schedule"`
	TimeoutMinutes int      `json:"timeout_minutes"`
	OutputDir      string   `json:"output_dir,omitempty"`
	DatabaseRef    string   `json:"database_ref,omitempty"`
	Sources        []string `json:"sources,omitempty"`
	DirectoryRefs  []string `json:"directory_refs,omitempty"`
	Tags           []string `json:"tags,omitempty"`
	HealthcheckID  string   `json:"healthcheck_id,omitempty"`
}

type retentionConfig struct {
	DBKeepDaily       int `json:"db_keep_daily"`
	UploadsKeepHourly int `json:"uploads_keep_hourly"`
	UploadsKeepDaily  int `json:"uploads_keep_daily"`
	VideosKeepDaily   int `json:"videos_keep_daily"`
}

type validationError struct {
	problems []string
}

func (e *validationError) Error() string {
	return strings.Join(e.problems, "; ")
}

func (e *validationError) pretty() string {
	var b strings.Builder
	for _, problem := range e.problems {
		b.WriteString(" - ")
		b.WriteString(problem)
		b.WriteByte('\n')
	}

	return strings.TrimRight(b.String(), "\n")
}

func loadConfig(path string) (config, error) {
	file, err := os.Open(path)
	if err != nil {
		return config{}, fmt.Errorf("open config: %w", err)
	}
	defer file.Close()

	var cfg config
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&cfg); err != nil {
		return config{}, fmt.Errorf("decode config: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return config{}, err
	}

	return cfg, nil
}

func (c config) validate() error {
	var problems []string

	if strings.TrimSpace(c.App.Name) == "" {
		problems = append(problems, "app.name is required")
	}

	if strings.TrimSpace(c.App.Timezone) == "" {
		problems = append(problems, "app.timezone is required")
	} else if _, err := time.LoadLocation(c.App.Timezone); err != nil {
		problems = append(problems, fmt.Sprintf("app.timezone is invalid: %v", err))
	}

	if len(c.Jobs) == 0 {
		problems = append(problems, "at least one job must be configured")
	}

	databaseRefs := make(map[string]struct{}, len(c.Databases))
	for i, db := range c.Databases {
		prefix := fmt.Sprintf("databases[%d]", i)
		ref := strings.TrimSpace(db.Name)

		if ref == "" {
			problems = append(problems, prefix+".name is required")
		} else {
			if _, exists := databaseRefs[ref]; exists {
				problems = append(problems, fmt.Sprintf("database name %q must be unique", ref))
			}
			databaseRefs[ref] = struct{}{}
		}

		problems = appendDatabaseProblems(problems, prefix, db.DumpTool, db.Host, db.Port, db.Username, db.PasswordEnv)

		if strings.TrimSpace(db.OutputDir) == "" {
			problems = append(problems, prefix+".output_dir is required")
		}

		problems = appendNonEmptyListProblems(problems, prefix+".tables", db.Tables)
		problems = appendNonEmptyListProblems(problems, prefix+".exclude_tables", db.ExcludeTables)
	}

	directoryRefs := make(map[string]struct{}, len(c.Directories))
	for i, directory := range c.Directories {
		prefix := fmt.Sprintf("directories[%d]", i)
		ref := strings.TrimSpace(directory.Name)

		if ref == "" {
			problems = append(problems, prefix+".name is required")
		} else {
			if _, exists := directoryRefs[ref]; exists {
				problems = append(problems, fmt.Sprintf("directory name %q must be unique", ref))
			}
			directoryRefs[ref] = struct{}{}
		}

		if strings.TrimSpace(directory.Path) == "" {
			problems = append(problems, prefix+".path is required")
		}

		problems = appendNonEmptyListProblems(problems, prefix+".excludes", directory.Excludes)
	}

	jobNames := make(map[string]struct{}, len(c.Jobs))
	needsDatabase := false
	needsRestic := false
	needsLegacyDatabase := false

	for i, job := range c.Jobs {
		prefix := fmt.Sprintf("jobs[%d]", i)

		if strings.TrimSpace(job.Name) == "" {
			problems = append(problems, prefix+".name is required")
		} else {
			if _, exists := jobNames[job.Name]; exists {
				problems = append(problems, fmt.Sprintf("job name %q must be unique", job.Name))
			}
			jobNames[job.Name] = struct{}{}
		}

		if strings.TrimSpace(job.Schedule) == "" {
			problems = append(problems, prefix+".schedule is required")
		}

		if job.TimeoutMinutes <= 0 {
			problems = append(problems, prefix+".timeout_minutes must be greater than 0")
		}

		switch job.Type {
		case "database_dump":
			needsDatabase = true

			if ref := strings.TrimSpace(job.DatabaseRef); ref != "" {
				if _, exists := databaseRefs[ref]; !exists {
					problems = append(problems, fmt.Sprintf("%s.database_ref %q does not exist in top-level databases", prefix, ref))
				}
			} else {
				needsLegacyDatabase = true
				if strings.TrimSpace(job.OutputDir) == "" {
					problems = append(problems, prefix+".output_dir is required when database_ref is not set")
				}
			}
		case "restic_backup":
			needsRestic = true
			if len(job.Sources) == 0 && len(job.DirectoryRefs) == 0 {
				problems = append(problems, prefix+" must define at least one source or directory_ref for restic_backup jobs")
			}

			for j, source := range job.Sources {
				if strings.TrimSpace(source) == "" {
					problems = append(problems, fmt.Sprintf("%s.sources[%d] must not be empty", prefix, j))
				}
			}

			for j, ref := range job.DirectoryRefs {
				ref = strings.TrimSpace(ref)
				if ref == "" {
					problems = append(problems, fmt.Sprintf("%s.directory_refs[%d] must not be empty", prefix, j))
					continue
				}
				if _, exists := directoryRefs[ref]; !exists {
					problems = append(problems, fmt.Sprintf("%s.directory_refs[%d] references unknown directory %q", prefix, j, ref))
				}
			}
		default:
			problems = append(problems, fmt.Sprintf("%s.type must be one of %q or %q", prefix, "database_dump", "restic_backup"))
		}

		if job.HealthcheckID != "" && c.Healthchecks.Jobs != nil {
			if mappedID, ok := c.Healthchecks.Jobs[job.Name]; ok && mappedID != job.HealthcheckID {
				problems = append(problems, fmt.Sprintf("healthchecks.jobs[%q] does not match jobs[%d].healthcheck_id", job.Name, i))
			}
		}
	}

	if needsDatabase && len(c.Databases) == 0 && !needsLegacyDatabase {
		problems = append(problems, "database_dump jobs require either top-level databases definitions or legacy database settings")
	}

	if needsLegacyDatabase {
		problems = appendDatabaseProblems(problems, "database", c.Database.DumpTool, c.Database.Host, c.Database.Port, c.Database.Username, c.Database.PasswordEnv)
		if strings.TrimSpace(c.Database.Name) == "" {
			problems = append(problems, "database.name is required when legacy database_dump jobs are configured")
		}
	}

	if needsRestic {
		if strings.TrimSpace(c.Restic.Binary) == "" {
			problems = append(problems, "restic.binary is required when restic_backup jobs are configured")
		}
		if strings.TrimSpace(c.Restic.Repository) == "" {
			problems = append(problems, "restic.repository is required when restic_backup jobs are configured")
		}
		if strings.TrimSpace(c.Restic.PasswordEnv) == "" {
			problems = append(problems, "restic.password_env is required when restic_backup jobs are configured")
		}
	}

	if len(problems) > 0 {
		return &validationError{problems: problems}
	}

	return nil
}

func appendDatabaseProblems(problems []string, prefix, dumpTool, host string, port int, username, passwordEnv string) []string {
	if strings.TrimSpace(dumpTool) == "" {
		problems = append(problems, prefix+".dump_tool is required")
	}
	if strings.TrimSpace(host) == "" {
		problems = append(problems, prefix+".host is required")
	}
	if port <= 0 {
		problems = append(problems, prefix+".port must be greater than 0")
	}
	if strings.TrimSpace(username) == "" {
		problems = append(problems, prefix+".username is required")
	}
	if strings.TrimSpace(passwordEnv) == "" {
		problems = append(problems, prefix+".password_env is required")
	}

	return problems
}

func appendNonEmptyListProblems(problems []string, prefix string, values []string) []string {
	for i, value := range values {
		if strings.TrimSpace(value) == "" {
			problems = append(problems, fmt.Sprintf("%s[%d] must not be empty", prefix, i))
		}
	}

	return problems
}
