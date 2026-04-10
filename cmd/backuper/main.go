package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const version = "0.1.0"

const (
	resticPasswordEnv         = "RESTIC_PASSWORD"
	sftpPasswordEnv           = "SFTP_PASSWORD"
	backuperResticRepoEnv     = "BACKUPER_RESTIC_REPOSITORY"
	defaultConfigPath         = "./configs/config.json"
	defaultSFTPKnownHosts     = "/tmp/backuper_restic_known_hosts"
	resticSFTPProxySubcommand = "restic-sftp-proxy"
	defaultAPIListenAddress   = "127.0.0.1:8080"
	defaultAPIRequestTimeout  = 30 * time.Second
	defaultHealthMaxAge       = 24 * time.Hour
)

var databaseDumpRetryBackoff = []time.Duration{
	15 * time.Second,
	30 * time.Second,
	60 * time.Second,
}

type resticSFTPConnection struct {
	User string
	Host string
	Port string
}

type resticSnapshotsResponse struct {
	Repository string            `json:"repository"`
	Count      int               `json:"count"`
	Tags       []string          `json:"tags,omitempty"`
	Snapshots  []json.RawMessage `json:"snapshots"`
}

type jobHealthStatus struct {
	Name          string     `json:"name"`
	Healthy       bool       `json:"healthy"`
	LastSuccessAt *time.Time `json:"last_success_at,omitempty"`
	Error         string     `json:"error,omitempty"`
}

type healthResponse struct {
	Jobs []jobHealthStatus `json:"jobs"`
}

type resticSnapshotSummary struct {
	ID      string    `json:"id"`
	ShortID string    `json:"short_id,omitempty"`
	Time    time.Time `json:"time"`
	Tags    []string  `json:"tags,omitempty"`
	Paths   []string  `json:"paths,omitempty"`
}

type errorResponse struct {
	Error string `json:"error"`
}

type apiServer struct {
	runner *templateRunner
	jobs   []jobConfig
	out    *os.File
	errOut *os.File
}

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if err := loadDotEnvIfPresent(".env"); err != nil {
		fmt.Fprintf(os.Stderr, "failed to load .env: %v\n", err)
		return 1
	}

	if len(args) == 0 {
		return runServe(nil)
	}

	switch args[0] {
	case "serve":
		return runServe(args[1:])
	case "restore":
		return runRestore(args[1:])
	case "run":
		return runScheduler(args[1:])
	case "api":
		return runAPI(args[1:])
	case "validate":
		return runValidate(args[1:])
	case "version":
		fmt.Fprintln(os.Stdout, version)
		return 0
	case resticSFTPProxySubcommand:
		return runResticSFTPProxy()
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
	fmt.Fprintln(out, "  backuper")
	fmt.Fprintln(out, "  backuper serve [-config ./configs/config.json] [-listen 127.0.0.1:8080] [-jobs job_a,job_b]")
	fmt.Fprintln(out, "  backuper restore [-config ./configs/config.json] -tag a[,b] -target /tmp/restore_a")
	fmt.Fprintln(out, "  backuper run -config /path/to/config.json [-jobs job_a,job_b] [-once] [-force]")
	fmt.Fprintln(out, "  backuper api -config /path/to/config.json [-listen 127.0.0.1:8080]")
	fmt.Fprintln(out, "  backuper validate -config /path/to/config.json")
	fmt.Fprintln(out, "  backuper version")
}

func runServe(args []string) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	configPath := fs.String("config", defaultConfigPath, "path to JSON config file")
	listenAddress := fs.String("listen", defaultAPIListenAddress, "HTTP listen address")
	selectedJobsFlag := fs.String("jobs", "", "comma-separated list of job names to run in scheduler; default is all jobs")

	if err := fs.Parse(args); err != nil {
		return 2
	}

	cfg, err := loadConfig(*configPath)
	if err != nil {
		var validationErr *validationError
		if errors.As(err, &validationErr) {
			fmt.Fprintf(os.Stderr, "config validation failed:\n%s\n", validationErr.pretty())
			return 1
		}

		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		return 1
	}

	location, err := time.LoadLocation(cfg.App.Timezone)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load timezone %q: %v\n", cfg.App.Timezone, err)
		return 1
	}

	selectedJobs, err := filterJobs(cfg.Jobs, parseJobNames(*selectedJobsFlag))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to select jobs: %v\n", err)
		return 1
	}

	if err := checkRuntimeDependencies(cfg, selectedJobs, requireResticForJobs(cfg, selectedJobs)); err != nil {
		var runtimeErr *runtimeCheckError
		if errors.As(err, &runtimeErr) {
			fmt.Fprintf(os.Stderr, "startup checks failed:\n%s\n", runtimeErr.pretty())
			return 1
		}

		fmt.Fprintf(os.Stderr, "startup checks failed: %v\n", err)
		return 1
	}

	runner := newTemplateRunner(cfg, os.Stdout, os.Stderr)
	api := newAPIServerWithRunner(runner, selectedJobs, os.Stdout, os.Stderr)
	httpServer := &http.Server{
		Addr:    *listenAddress,
		Handler: api.routes(),
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	schedulerDone := make(chan struct{})
	go func() {
		defer close(schedulerDone)
		runContinuousScheduler(ctx, runner, selectedJobs, location)
	}()

	go func() {
		<-ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := httpServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(api.errOut, "api shutdown error: %v\n", err)
		}
	}()

	fmt.Fprintf(os.Stdout, "backuper started with config %s\n", *configPath)
	fmt.Fprintf(os.Stdout, "api listening on http://%s\n", *listenAddress)

	err = httpServer.ListenAndServe()
	stop()
	<-schedulerDone

	if errors.Is(err, http.ErrServerClosed) {
		return 0
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "api server failed: %v\n", err)
		return 1
	}

	return 0
}

func runRestore(args []string) int {
	fs := flag.NewFlagSet("restore", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	configPath := fs.String("config", defaultConfigPath, "path to JSON config file")
	tagValue := fs.String("tag", "", "tag or comma-separated tags used to select the latest snapshot")
	target := fs.String("target", "", "directory where the snapshot will be restored")
	snapshotID := fs.String("snapshot", "", "explicit snapshot ID to restore; skips latest-by-tag lookup")

	if err := fs.Parse(args); err != nil {
		return 2
	}

	if strings.TrimSpace(*target) == "" {
		fmt.Fprintln(os.Stderr, "missing required -target flag")
		return 2
	}

	tags := parseMultiValueQuery([]string{*tagValue})
	if strings.TrimSpace(*snapshotID) == "" && len(tags) == 0 {
		fmt.Fprintln(os.Stderr, "missing required -tag flag or -snapshot flag")
		return 2
	}

	cfg, err := loadConfig(*configPath)
	if err != nil {
		var validationErr *validationError
		if errors.As(err, &validationErr) {
			fmt.Fprintf(os.Stderr, "config validation failed:\n%s\n", validationErr.pretty())
			return 1
		}

		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		return 1
	}

	if err := checkRuntimeDependencies(cfg, nil, true); err != nil {
		var runtimeErr *runtimeCheckError
		if errors.As(err, &runtimeErr) {
			fmt.Fprintf(os.Stderr, "startup checks failed:\n%s\n", runtimeErr.pretty())
			return 1
		}

		fmt.Fprintf(os.Stderr, "startup checks failed: %v\n", err)
		return 1
	}

	runner := newTemplateRunner(cfg, os.Stdout, os.Stderr)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	selectedSnapshotID := strings.TrimSpace(*snapshotID)
	if selectedSnapshotID == "" {
		snapshot, err := runner.findLatestResticSnapshot(ctx, tags)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to resolve snapshot: %v\n", err)
			return 1
		}

		selectedSnapshotID = snapshot.ID
		fmt.Fprintf(os.Stdout, "selected snapshot %s from %s\n", snapshot.ID, snapshot.Time.Format(time.RFC3339))
	}

	if err := runner.restoreResticSnapshot(ctx, selectedSnapshotID, *target); err != nil {
		fmt.Fprintf(os.Stderr, "restore failed: %v\n", err)
		return 1
	}

	fmt.Fprintf(os.Stdout, "restored snapshot %s to %s\n", selectedSnapshotID, *target)
	return 0
}

func runAPI(args []string) int {
	fs := flag.NewFlagSet("api", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	configPath := fs.String("config", "", "path to JSON config file")
	listenAddress := fs.String("listen", defaultAPIListenAddress, "HTTP listen address")

	if err := fs.Parse(args); err != nil {
		return 2
	}

	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "missing required -config flag")
		return 2
	}

	cfg, err := loadConfig(*configPath)
	if err != nil {
		var validationErr *validationError
		if errors.As(err, &validationErr) {
			fmt.Fprintf(os.Stderr, "config validation failed:\n%s\n", validationErr.pretty())
			return 1
		}

		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		return 1
	}

	if err := checkRuntimeDependencies(cfg, nil, true); err != nil {
		var runtimeErr *runtimeCheckError
		if errors.As(err, &runtimeErr) {
			fmt.Fprintf(os.Stderr, "startup checks failed:\n%s\n", runtimeErr.pretty())
			return 1
		}

		fmt.Fprintf(os.Stderr, "startup checks failed: %v\n", err)
		return 1
	}

	server := newAPIServer(cfg, os.Stdout, os.Stderr)
	httpServer := &http.Server{
		Addr:    *listenAddress,
		Handler: server.routes(),
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	go func() {
		<-ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := httpServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(server.errOut, "api shutdown error: %v\n", err)
		}
	}()

	fmt.Fprintf(server.out, "api listening on http://%s\n", *listenAddress)
	err = httpServer.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return 0
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "api server failed: %v\n", err)
		return 1
	}

	return 0
}

func runScheduler(args []string) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	configPath := fs.String("config", "", "path to JSON config file")
	selectedJobsFlag := fs.String("jobs", "", "comma-separated list of job names to run; default is all jobs")
	runOnce := fs.Bool("once", false, "evaluate due jobs once and exit")
	forceRun := fs.Bool("force", false, "run selected jobs immediately once and exit, ignoring schedule")

	if err := fs.Parse(args); err != nil {
		return 2
	}

	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "missing required -config flag")
		return 2
	}

	cfg, err := loadConfig(*configPath)
	if err != nil {
		var validationErr *validationError
		if errors.As(err, &validationErr) {
			fmt.Fprintf(os.Stderr, "config validation failed:\n%s\n", validationErr.pretty())
			return 1
		}

		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		return 1
	}

	location, err := time.LoadLocation(cfg.App.Timezone)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load timezone %q: %v\n", cfg.App.Timezone, err)
		return 1
	}

	selectedJobs, err := filterJobs(cfg.Jobs, parseJobNames(*selectedJobsFlag))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to select jobs: %v\n", err)
		return 1
	}

	if err := checkRuntimeDependencies(cfg, selectedJobs, requireResticForJobs(cfg, selectedJobs)); err != nil {
		var runtimeErr *runtimeCheckError
		if errors.As(err, &runtimeErr) {
			fmt.Fprintf(os.Stderr, "startup checks failed:\n%s\n", runtimeErr.pretty())
			return 1
		}

		fmt.Fprintf(os.Stderr, "startup checks failed: %v\n", err)
		return 1
	}

	runner := newTemplateRunner(cfg, os.Stdout, os.Stderr)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if *forceRun {
		now := time.Now().In(location).Truncate(time.Minute)
		runner.dispatchSelectedJobs(ctx, selectedJobs, now)
		runner.Wait()
		return 0
	}

	if *runOnce {
		now := time.Now().In(location).Truncate(time.Minute)
		runner.dispatchDueJobs(ctx, selectedJobs, now)
		runner.Wait()
		return 0
	}

	runContinuousScheduler(ctx, runner, selectedJobs, location)
	return 0
}

type config struct {
	App      appConfig              `json:"app"`
	Paths    pathsConfig            `json:"paths"`
	Database databaseDefaultsConfig `json:"database"`
	Restic   resticConfig           `json:"restic"`
	Jobs     []jobConfig            `json:"jobs"`
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

type databaseDefaultsConfig struct {
	DumpTool  string   `json:"dump_tool"`
	DumpFlags []string `json:"dump_flags"`
	Gzip      bool     `json:"gzip"`
	Restic    *bool    `json:"restic,omitempty"`
}

type resticConfig struct {
	Binary     string `json:"binary"`
	Repository string `json:"repository"`
}

type jobConfig struct {
	Name           string              `json:"name"`
	Type           string              `json:"type"`
	Schedule       string              `json:"schedule"`
	TimeoutMinutes int                 `json:"timeout_minutes"`
	OutputDir      string              `json:"output_dir,omitempty"`
	DatabaseName   string              `json:"database_name,omitempty"`
	Host           string              `json:"host,omitempty"`
	Port           int                 `json:"port,omitempty"`
	User           string              `json:"user,omitempty"`
	Password       string              `json:"password,omitempty"`
	Tables         []string            `json:"tables,omitempty"`
	ExcludeTables  []string            `json:"exclude_tables,omitempty"`
	Sources        []string            `json:"sources,omitempty"`
	Tags           []string            `json:"tags,omitempty"`
	Retention      *jobRetentionConfig `json:"retention,omitempty"`
	RetentionDays  int                 `json:"retention_days,omitempty"`
}

type jobRetentionConfig struct {
	KeepHourly  int `json:"keep_hourly,omitempty"`
	KeepDaily   int `json:"keep_daily,omitempty"`
	KeepWeekly  int `json:"keep_weekly,omitempty"`
	KeepMonthly int `json:"keep_monthly,omitempty"`
	KeepYearly  int `json:"keep_yearly,omitempty"`
}

type validationError struct {
	problems []string
}

type runtimeCheckError struct {
	problems []string
}

type templateRunner struct {
	cfg      config
	out      *os.File
	errOut   *os.File
	mu       sync.Mutex
	resticMu sync.Mutex
	running  map[string]struct{}
	wg       sync.WaitGroup
	sleep    func(context.Context, time.Duration) error
}

type resticWorkUnit struct {
	sources []string
	tag     string
}

type databaseDumpFile struct {
	path    string
	modTime time.Time
}

func (c config) databaseResticEnabled() bool {
	if c.Database.Restic != nil {
		return *c.Database.Restic
	}

	return strings.TrimSpace(c.Restic.Binary) != "" || strings.TrimSpace(c.Restic.Repository) != ""
}

type resolvedDatabaseDumpConfig struct {
	Name          string
	DumpTool      string
	Host          string
	Port          int
	User          string
	PasswordEnv   string
	Tables        []string
	ExcludeTables []string
	DumpFlags     []string
	Gzip          bool
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

func (e *runtimeCheckError) Error() string {
	return strings.Join(e.problems, "; ")
}

func (e *runtimeCheckError) pretty() string {
	var b strings.Builder
	for _, problem := range e.problems {
		b.WriteString(" - ")
		b.WriteString(problem)
		b.WriteByte('\n')
	}

	return strings.TrimRight(b.String(), "\n")
}

func newTemplateRunner(cfg config, out, errOut *os.File) *templateRunner {
	return &templateRunner{
		cfg:     cfg,
		out:     out,
		errOut:  errOut,
		running: make(map[string]struct{}),
		sleep:   sleepWithContext,
	}
}

func newAPIServer(cfg config, out, errOut *os.File) *apiServer {
	return newAPIServerWithRunner(newTemplateRunner(cfg, out, errOut), cfg.Jobs, out, errOut)
}

func newAPIServerWithRunner(runner *templateRunner, jobs []jobConfig, out, errOut *os.File) *apiServer {
	return &apiServer{
		runner: runner,
		jobs:   append([]jobConfig(nil), jobs...),
		out:    out,
		errOut: errOut,
	}
}

func (s *apiServer) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/health", s.handleHealth)
	mux.HandleFunc("/api/restic/snapshots", s.handleResticSnapshots)
	return mux
}

func (s *apiServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "method not allowed"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), defaultAPIRequestTimeout)
	defer cancel()

	statuses, err := s.collectJobHealthStatuses(ctx)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, errorResponse{Error: err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, healthResponse{Jobs: statuses})
}

func (s *apiServer) handleResticSnapshots(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "method not allowed"})
		return
	}

	tags := parseMultiValueQuery(r.URL.Query()["tag"])
	ctx, cancel := context.WithTimeout(r.Context(), defaultAPIRequestTimeout)
	defer cancel()

	snapshots, err := s.runner.listResticSnapshots(ctx, tags)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, errorResponse{Error: err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, resticSnapshotsResponse{
		Repository: s.runner.cfg.Restic.Repository,
		Count:      len(snapshots),
		Tags:       tags,
		Snapshots:  snapshots,
	})
}

func (s *apiServer) collectJobHealthStatuses(ctx context.Context) ([]jobHealthStatus, error) {
	statuses := make([]jobHealthStatus, 0, len(s.jobs))
	now := time.Now()

	var (
		snapshots   []resticSnapshotSummary
		resticError error
	)
	if hasJobType(s.jobs, "restic_backup") || (hasJobType(s.jobs, "database_dump") && s.runner.cfg.databaseResticEnabled()) {
		snapshots, resticError = s.runner.listResticSnapshotSummaries(ctx, nil)
	}

	for _, job := range s.jobs {
		status := jobHealthStatus{Name: job.Name}

		switch job.Type {
		case "database_dump":
			if s.runner.cfg.databaseResticEnabled() {
				if resticError != nil {
					status.Error = resticError.Error()
					break
				}

				lastSuccessAt, err := latestDatabaseDumpResticSuccess(job, snapshots)
				if err != nil {
					status.Error = err.Error()
					break
				}
				if lastSuccessAt != nil {
					status.LastSuccessAt = lastSuccessAt
					status.Healthy = successIsFresh(*lastSuccessAt, now, defaultHealthMaxAge)
				}
				break
			}

			lastSuccessAt, err := latestDatabaseDumpSuccess(job)
			if err != nil {
				status.Error = err.Error()
				break
			}
			if lastSuccessAt != nil {
				status.LastSuccessAt = lastSuccessAt
				status.Healthy = successIsFresh(*lastSuccessAt, now, defaultHealthMaxAge)
			}
		case "restic_backup":
			if resticError != nil {
				status.Error = resticError.Error()
				break
			}

			lastSuccessAt, err := latestResticJobSuccess(job, snapshots)
			if err != nil {
				status.Error = err.Error()
				break
			}
			if lastSuccessAt != nil {
				status.LastSuccessAt = lastSuccessAt
				status.Healthy = successIsFresh(*lastSuccessAt, now, defaultHealthMaxAge)
			}
		default:
			status.Error = fmt.Sprintf("unsupported job type %q", job.Type)
		}

		statuses = append(statuses, status)
	}

	return statuses, nil
}

func latestDatabaseDumpSuccess(job jobConfig) (*time.Time, error) {
	dir := databaseDumpOutputDir(job)
	if _, err := os.Stat(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat dump output dir %s: %w", dir, err)
	}

	files, err := listDatabaseDumpFiles(dir)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, nil
	}

	latest := files[0].modTime
	return &latest, nil
}

func successIsFresh(lastSuccessAt, now time.Time, maxAge time.Duration) bool {
	if lastSuccessAt.After(now) {
		return true
	}

	return now.Sub(lastSuccessAt) <= maxAge
}

func latestResticJobSuccess(job jobConfig, snapshots []resticSnapshotSummary) (*time.Time, error) {
	workUnits, err := planResticWorkUnits(job)
	if err != nil {
		return nil, err
	}

	var latest time.Time
	matchedAny := false
	for _, unit := range workUnits {
		unitLatest, ok := latestResticUnitSuccess(unit, snapshots)
		if !ok {
			return nil, nil
		}
		if !matchedAny || unitLatest.After(latest) {
			latest = unitLatest
			matchedAny = true
		}
	}

	if !matchedAny {
		return nil, nil
	}

	return &latest, nil
}

func latestDatabaseDumpResticSuccess(job jobConfig, snapshots []resticSnapshotSummary) (*time.Time, error) {
	unit := resticWorkUnit{
		sources: []string{databaseDumpOutputDir(job)},
		tag:     job.Name,
	}

	latest, ok := latestResticUnitSuccess(unit, snapshots)
	if !ok {
		return nil, nil
	}

	return &latest, nil
}

func latestResticUnitSuccess(unit resticWorkUnit, snapshots []resticSnapshotSummary) (time.Time, bool) {
	var latest time.Time
	found := false
	for _, snapshot := range snapshots {
		if !resticUnitMatchesSnapshot(unit, snapshot) {
			continue
		}

		if !found || snapshot.Time.After(latest) {
			latest = snapshot.Time
			found = true
		}
	}

	return latest, found
}

func resticUnitMatchesSnapshot(unit resticWorkUnit, snapshot resticSnapshotSummary) bool {
	if unit.tag != "" {
		return stringSliceContains(snapshot.Tags, unit.tag)
	}

	return stringSlicesEqual(normalizePaths(snapshot.Paths), normalizePaths(unit.sources))
}

func normalizePaths(paths []string) []string {
	normalized := make([]string, 0, len(paths))
	for _, value := range paths {
		normalized = append(normalized, filepath.Clean(strings.TrimSpace(value)))
	}
	sort.Strings(normalized)
	return normalized
}

func stringSliceContains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func stringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(payload); err != nil {
		fmt.Fprintf(os.Stderr, "failed to encode http response: %v\n", err)
	}
}

func parseMultiValueQuery(values []string) []string {
	if len(values) == 0 {
		return nil
	}

	result := make([]string, 0, len(values))
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			result = append(result, part)
		}
	}

	return result
}

func loadDotEnvIfPresent(path string) error {
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for lineNumber := 1; scanner.Scan(); lineNumber++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}

		key, value, found := strings.Cut(line, "=")
		if !found {
			return fmt.Errorf("%s:%d: expected KEY=VALUE", path, lineNumber)
		}

		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" {
			return fmt.Errorf("%s:%d: empty key", path, lineNumber)
		}

		if _, exists := os.LookupEnv(key); exists {
			continue
		}

		if len(value) >= 2 {
			if strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"") {
				value = strings.Trim(value, "\"")
			} else if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
				value = strings.Trim(value, "'")
			}
		}

		if err := os.Setenv(key, value); err != nil {
			return fmt.Errorf("%s:%d: set %s: %w", path, lineNumber, key, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
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

	jobNames := make(map[string]struct{}, len(c.Jobs))
	needsDatabase := false
	needsRestic := false

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

		if job.RetentionDays != 0 {
			problems = append(problems, prefix+".retention_days is no longer supported; use retention.keep_daily and/or retention.keep_hourly")
		}

		switch job.Type {
		case "database_dump":
			needsDatabase = true
			problems = appendRetentionProblems(problems, prefix+".retention", job.Retention)
			problems = appendDatabaseJobProblems(problems, prefix, job)
		case "restic_backup":
			needsRestic = true
			if len(job.Sources) == 0 {
				problems = append(problems, prefix+" must define at least one source for restic_backup jobs")
			}
			problems = appendRetentionProblems(problems, prefix+".retention", job.Retention)

			for j, source := range job.Sources {
				if strings.TrimSpace(source) == "" {
					problems = append(problems, fmt.Sprintf("%s.sources[%d] must not be empty", prefix, j))
				}
			}
			problems = appendNonEmptyListProblems(problems, prefix+".tags", job.Tags)
			problems = appendResticTagProblems(problems, prefix, job)
		default:
			problems = append(problems, fmt.Sprintf("%s.type must be one of %q or %q", prefix, "database_dump", "restic_backup"))
		}

	}

	if needsDatabase && c.databaseResticEnabled() {
		needsRestic = true
	}

	if needsDatabase && strings.TrimSpace(c.Database.DumpTool) == "" {
		problems = append(problems, "database.dump_tool is required when database_dump jobs are configured")
	}

	if needsRestic {
		if strings.TrimSpace(c.Restic.Binary) == "" {
			problems = append(problems, "restic.binary is required when restic_backup jobs are configured")
		}
		if strings.TrimSpace(c.Restic.Repository) == "" {
			problems = append(problems, "restic.repository is required when restic_backup jobs are configured")
		}
	}

	if len(problems) > 0 {
		return &validationError{problems: problems}
	}

	return nil
}

func appendDatabaseJobProblems(problems []string, prefix string, job jobConfig) []string {
	if strings.TrimSpace(job.DatabaseName) == "" {
		problems = append(problems, prefix+".database_name is required")
	}
	if strings.TrimSpace(job.Host) == "" {
		problems = append(problems, prefix+".host is required")
	}
	if job.Port <= 0 {
		problems = append(problems, prefix+".port must be greater than 0")
	}
	if strings.TrimSpace(job.User) == "" {
		problems = append(problems, prefix+".user is required")
	}
	if strings.TrimSpace(job.Password) == "" {
		problems = append(problems, prefix+".password is required")
	}
	problems = appendNonEmptyListProblems(problems, prefix+".tables", job.Tables)
	problems = appendNonEmptyListProblems(problems, prefix+".exclude_tables", job.ExcludeTables)

	return problems
}

func checkRuntimeDependencies(cfg config, jobs []jobConfig, requireRestic bool) error {
	var problems []string

	if requireRestic {
		problems = appendBinaryAvailabilityProblem(problems, "restic.binary", cfg.Restic.Binary)
	}

	if hasJobType(jobs, "database_dump") {
		problems = appendBinaryAvailabilityProblem(problems, "database.dump_tool", cfg.Database.DumpTool)
	}

	if len(problems) > 0 {
		return &runtimeCheckError{problems: problems}
	}

	return nil
}

func requireResticForJobs(cfg config, jobs []jobConfig) bool {
	if hasJobType(jobs, "restic_backup") {
		return true
	}

	return hasJobType(jobs, "database_dump") && cfg.databaseResticEnabled()
}

func appendBinaryAvailabilityProblem(problems []string, label, binary string) []string {
	binary = strings.TrimSpace(binary)
	if binary == "" {
		return append(problems, label+" is required for this command")
	}

	if _, err := exec.LookPath(binary); err != nil {
		return append(problems, fmt.Sprintf("%s %q is not available: %v", label, binary, err))
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

func appendRetentionProblems(problems []string, prefix string, retention *jobRetentionConfig) []string {
	if retention == nil {
		return problems
	}

	hasPositiveValue := false
	for field, value := range map[string]int{
		"keep_hourly":  retention.KeepHourly,
		"keep_daily":   retention.KeepDaily,
		"keep_weekly":  retention.KeepWeekly,
		"keep_monthly": retention.KeepMonthly,
		"keep_yearly":  retention.KeepYearly,
	} {
		if value < 0 {
			problems = append(problems, fmt.Sprintf("%s.%s must be greater than or equal to 0", prefix, field))
			continue
		}
		if value > 0 {
			hasPositiveValue = true
		}
	}

	if !hasPositiveValue {
		problems = append(problems, prefix+" must define at least one keep_* value greater than 0")
	}

	return problems
}

func appendResticTagProblems(problems []string, prefix string, job jobConfig) []string {
	switch len(job.Tags) {
	case 0:
		if job.Retention != nil {
			problems = append(problems, prefix+".tags must define at least one tag when retention is configured")
		}
	case 1:
		return problems
	default:
		if len(job.Tags) != len(job.Sources) {
			problems = append(problems, fmt.Sprintf("%s.tags must contain exactly one tag or match the number of sources", prefix))
		}
	}

	return problems
}

func parseJobNames(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}

	rawParts := strings.Split(value, ",")
	names := make([]string, 0, len(rawParts))
	for _, part := range rawParts {
		name := strings.TrimSpace(part)
		if name == "" {
			continue
		}
		names = append(names, name)
	}

	return names
}

func filterJobs(jobs []jobConfig, selectedNames []string) ([]jobConfig, error) {
	if len(selectedNames) == 0 {
		return append([]jobConfig(nil), jobs...), nil
	}

	selectedSet := make(map[string]struct{}, len(selectedNames))
	for _, name := range selectedNames {
		selectedSet[name] = struct{}{}
	}

	filtered := make([]jobConfig, 0, len(selectedNames))
	found := make(map[string]struct{}, len(selectedNames))
	for _, job := range jobs {
		if _, ok := selectedSet[job.Name]; !ok {
			continue
		}
		filtered = append(filtered, job)
		found[job.Name] = struct{}{}
	}

	for _, name := range selectedNames {
		if _, ok := found[name]; !ok {
			return nil, fmt.Errorf("job %q does not exist in config", name)
		}
	}

	return filtered, nil
}

func hasJobType(jobs []jobConfig, jobType string) bool {
	for _, job := range jobs {
		if job.Type == jobType {
			return true
		}
	}

	return false
}

func (r *templateRunner) dispatchDueJobs(ctx context.Context, jobs []jobConfig, now time.Time) {
	for _, job := range jobs {
		match, err := jobMatchesSchedule(job.Schedule, now)
		if err != nil {
			fmt.Fprintf(r.errOut, "[%s] skip job %s: invalid schedule %q: %v\n", now.Format(time.RFC3339), job.Name, job.Schedule, err)
			continue
		}
		if !match {
			continue
		}
		r.launchJob(ctx, job, now)
	}
}

func (r *templateRunner) dispatchSelectedJobs(ctx context.Context, jobs []jobConfig, now time.Time) {
	for _, job := range jobs {
		r.launchJob(ctx, job, now)
	}
}

func runContinuousScheduler(ctx context.Context, runner *templateRunner, jobs []jobConfig, location *time.Location) {
	fmt.Fprintf(runner.out, "runner started for %d jobs in timezone %s\n", len(jobs), location.String())

	for {
		now := time.Now().In(location).Truncate(time.Minute)
		runner.dispatchDueJobs(ctx, jobs, now)

		nextTick := now.Add(time.Minute)
		timer := time.NewTimer(time.Until(nextTick))
		select {
		case <-ctx.Done():
			timer.Stop()
			fmt.Fprintln(runner.out, "runner stopping")
			runner.Wait()
			return
		case <-timer.C:
		}
	}
}

func (r *templateRunner) launchJob(ctx context.Context, job jobConfig, triggerTime time.Time) {
	if !r.tryStart(job.Name) {
		fmt.Fprintf(r.out, "[%s] job %s is already running, skipping duplicate trigger\n", triggerTime.Format(time.RFC3339), job.Name)
		return
	}

	r.wg.Add(1)
	go func(job jobConfig, triggerTime time.Time) {
		defer r.finish(job.Name)
		defer r.wg.Done()

		jobCtx, cancel := context.WithTimeout(ctx, time.Duration(job.TimeoutMinutes)*time.Minute)
		defer cancel()

		fmt.Fprintf(r.out, "[%s] start job %s (%s)\n", triggerTime.Format(time.RFC3339), job.Name, job.Type)
		err := r.runJob(jobCtx, job)
		if err != nil {
			fmt.Fprintf(r.errOut, "[%s] fail job %s: %v\n", triggerTime.Format(time.RFC3339), job.Name, err)
			return
		}
		fmt.Fprintf(r.out, "[%s] finish job %s\n", triggerTime.Format(time.RFC3339), job.Name)
	}(job, triggerTime)
}

func (r *templateRunner) tryStart(jobName string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.running[jobName]; exists {
		return false
	}

	r.running[jobName] = struct{}{}
	return true
}

func (r *templateRunner) finish(jobName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.running, jobName)
}

func (r *templateRunner) Wait() {
	r.wg.Wait()
}

func (r *templateRunner) runJob(ctx context.Context, job jobConfig) error {
	switch job.Type {
	case "restic_backup":
		return r.runResticBackupJob(ctx, job)
	case "database_dump":
		return r.runDatabaseDumpJob(ctx, job)
	default:
		return fmt.Errorf("unsupported job type %q", job.Type)
	}
}

func (r *templateRunner) runDatabaseDumpJob(ctx context.Context, job jobConfig) error {
	database, err := r.resolveDatabaseDumpConfig(job)
	if err != nil {
		return err
	}

	password, ok := os.LookupEnv(database.PasswordEnv)
	if !ok {
		return fmt.Errorf("required environment variable %s is not set", database.PasswordEnv)
	}

	outputDir := databaseDumpOutputDir(job)
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("create dump output dir %s: %w", outputDir, err)
	}

	now := r.currentTime()
	fileName := databaseDumpFileName(job.Name, database.Gzip, now)
	outputPath := filepath.Join(outputDir, fileName)

	defaultsFilePath, err := createMySQLDefaultsFile(database, password)
	if err != nil {
		return err
	}
	defer os.Remove(defaultsFilePath)

	if err := r.runDatabaseDumpWithRetry(ctx, job.Name, database, defaultsFilePath, outputPath); err != nil {
		return err
	}

	fmt.Fprintf(r.out, "[%s] dump saved to %s\n", job.Name, outputPath)

	if job.Retention != nil {
		if err := pruneDatabaseDumpFiles(outputDir, job.Retention, now); err != nil {
			return err
		}
	}

	if r.cfg.databaseResticEnabled() {
		if err := r.runDatabaseDumpResticBackup(ctx, job, outputDir); err != nil {
			return err
		}
	}

	return nil
}

func (r *templateRunner) runDatabaseDumpWithRetry(ctx context.Context, jobName string, database resolvedDatabaseDumpConfig, defaultsFilePath, outputPath string) error {
	totalAttempts := len(databaseDumpRetryBackoff) + 1
	var lastErr error

	for attempt := 1; attempt <= totalAttempts; attempt++ {
		err := r.runDatabaseDumpCommand(ctx, database, defaultsFilePath, outputPath)
		if err == nil {
			return nil
		}

		lastErr = err
		if ctx.Err() != nil || attempt == totalAttempts {
			break
		}

		delay := databaseDumpRetryBackoff[attempt-1]
		fmt.Fprintf(r.errOut, "[%s] database dump attempt %d/%d failed: %v; retrying in %s\n", jobName, attempt, totalAttempts, err, delay)
		if err := r.sleep(ctx, delay); err != nil {
			return fmt.Errorf("database dump retry interrupted for job %s: %w", jobName, err)
		}
	}

	return lastErr
}

func (r *templateRunner) runDatabaseDumpCommand(ctx context.Context, database resolvedDatabaseDumpConfig, defaultsFilePath, outputPath string) error {
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create dump file %s: %w", outputPath, err)
	}

	var writer io.WriteCloser = outputFile
	if database.Gzip {
		gzipWriter := gzip.NewWriter(outputFile)
		writer = &stackedWriteCloser{
			Writer: gzipWriter,
			closers: []io.Closer{
				gzipWriter,
				outputFile,
			},
		}
	}

	cleanupOnError := func(runErr error) error {
		_ = writer.Close()
		_ = os.Remove(outputPath)
		return runErr
	}

	args := buildDatabaseDumpArgs(defaultsFilePath, database)
	cmd := exec.CommandContext(ctx, database.DumpTool, args...)
	cmd.Env = os.Environ()
	cmd.Stdout = writer

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		message := strings.TrimSpace(stderr.String())
		if message != "" {
			return cleanupOnError(fmt.Errorf("%s %s: %w: %s", database.DumpTool, strings.Join(args, " "), err, message))
		}

		return cleanupOnError(fmt.Errorf("%s %s: %w", database.DumpTool, strings.Join(args, " "), err))
	}

	if err := writer.Close(); err != nil {
		_ = os.Remove(outputPath)
		return fmt.Errorf("finalize dump file %s: %w", outputPath, err)
	}

	return nil
}

func (r *templateRunner) runResticBackupJob(ctx context.Context, job jobConfig) error {
	workUnits, err := planResticWorkUnits(job)
	if err != nil {
		return err
	}

	return r.runResticWorkUnits(ctx, job.Name, job.Retention, workUnits)
}

func (r *templateRunner) runDatabaseDumpResticBackup(ctx context.Context, job jobConfig, outputDir string) error {
	unit := resticWorkUnit{
		sources: []string{outputDir},
		tag:     job.Name,
	}

	return r.runResticWorkUnits(ctx, job.Name, job.Retention, []resticWorkUnit{unit})
}

func (r *templateRunner) runResticWorkUnits(ctx context.Context, jobName string, retention *jobRetentionConfig, workUnits []resticWorkUnit) error {
	if _, ok := os.LookupEnv(resticPasswordEnv); !ok {
		return fmt.Errorf("required environment variable %s is not set", resticPasswordEnv)
	}

	r.resticMu.Lock()
	defer r.resticMu.Unlock()

	baseArgs, envAdditions, err := r.resticBaseArgs()
	if err != nil {
		return err
	}

	for _, unit := range workUnits {
		args := append([]string(nil), baseArgs...)
		args = append(args, "backup")
		if unit.tag != "" {
			args = append(args, "--tag", unit.tag)
		}
		args = append(args, unit.sources...)

		if err := r.runCommandWithEnv(ctx, jobName, r.cfg.Restic.Binary, envAdditions, args...); err != nil {
			return fmt.Errorf("restic backup failed for job %s: %w", jobName, err)
		}

		if retention == nil {
			continue
		}

		forgetArgs := append([]string(nil), baseArgs...)
		forgetArgs = append(forgetArgs, "forget", "--prune")
		if unit.tag != "" {
			forgetArgs = append(forgetArgs, "--tag", unit.tag)
		}
		forgetArgs = append(forgetArgs, retentionArgs(retention)...)

		if err := r.runCommandWithEnv(ctx, jobName, r.cfg.Restic.Binary, envAdditions, forgetArgs...); err != nil {
			return fmt.Errorf("restic forget failed for job %s: %w", jobName, err)
		}
	}

	return nil
}

func (r *templateRunner) resticBaseArgs() ([]string, []string, error) {
	args := make([]string, 0, 4)
	var envAdditions []string

	if shouldUseResticSFTPProxy(r.cfg.Restic.Repository) {
		executable, err := os.Executable()
		if err != nil {
			return nil, nil, fmt.Errorf("resolve current executable for restic sftp proxy: %w", err)
		}

		args = append(args, "--option", "sftp.command="+executable+" "+resticSFTPProxySubcommand)
		envAdditions = append(envAdditions, backuperResticRepoEnv+"="+r.cfg.Restic.Repository)
	}
	args = append(args, "-r", r.cfg.Restic.Repository)
	return args, envAdditions, nil
}

func (r *templateRunner) listResticSnapshots(ctx context.Context, tags []string) ([]json.RawMessage, error) {
	output, err := r.resticSnapshotsJSON(ctx, tags)
	if err != nil {
		return nil, err
	}

	var snapshots []json.RawMessage
	if err := json.Unmarshal(output, &snapshots); err != nil {
		return nil, fmt.Errorf("decode restic snapshots JSON: %w", err)
	}

	return snapshots, nil
}

func (r *templateRunner) listResticSnapshotSummaries(ctx context.Context, tags []string) ([]resticSnapshotSummary, error) {
	output, err := r.resticSnapshotsJSON(ctx, tags)
	if err != nil {
		return nil, err
	}

	var snapshots []resticSnapshotSummary
	if err := json.Unmarshal(output, &snapshots); err != nil {
		return nil, fmt.Errorf("decode restic snapshots JSON: %w", err)
	}

	return snapshots, nil
}

func (r *templateRunner) resticSnapshotsJSON(ctx context.Context, tags []string) ([]byte, error) {
	if _, ok := os.LookupEnv(resticPasswordEnv); !ok {
		return nil, fmt.Errorf("required environment variable %s is not set", resticPasswordEnv)
	}

	r.resticMu.Lock()
	defer r.resticMu.Unlock()

	baseArgs, envAdditions, err := r.resticBaseArgs()
	if err != nil {
		return nil, err
	}

	args := append([]string(nil), baseArgs...)
	args = append(args, "snapshots", "--json")
	for _, tag := range tags {
		args = append(args, "--tag", tag)
	}

	output, err := r.runCommandOutputWithEnv(ctx, r.cfg.Restic.Binary, envAdditions, args...)
	if err != nil {
		return nil, err
	}

	return output, nil
}

func (r *templateRunner) findLatestResticSnapshot(ctx context.Context, tags []string) (resticSnapshotSummary, error) {
	snapshots, err := r.listResticSnapshotSummaries(ctx, tags)
	if err != nil {
		return resticSnapshotSummary{}, err
	}

	return selectLatestResticSnapshot(snapshots)
}

func selectLatestResticSnapshot(snapshots []resticSnapshotSummary) (resticSnapshotSummary, error) {
	if len(snapshots) == 0 {
		return resticSnapshotSummary{}, fmt.Errorf("no matching snapshots found")
	}

	latest := snapshots[0]
	for _, snapshot := range snapshots[1:] {
		if snapshot.Time.After(latest.Time) {
			latest = snapshot
		}
	}

	return latest, nil
}

func (r *templateRunner) restoreResticSnapshot(ctx context.Context, snapshotID, target string) error {
	if _, ok := os.LookupEnv(resticPasswordEnv); !ok {
		return fmt.Errorf("required environment variable %s is not set", resticPasswordEnv)
	}

	snapshotID = strings.TrimSpace(snapshotID)
	target = strings.TrimSpace(target)
	if snapshotID == "" {
		return fmt.Errorf("snapshot ID must not be empty")
	}
	if target == "" {
		return fmt.Errorf("target must not be empty")
	}

	baseArgs, envAdditions, err := r.resticBaseArgs()
	if err != nil {
		return err
	}

	r.resticMu.Lock()
	defer r.resticMu.Unlock()

	args := append([]string(nil), baseArgs...)
	args = append(args, "restore", snapshotID, "--target", target)

	if err := r.runCommandWithEnv(ctx, "restore", r.cfg.Restic.Binary, envAdditions, args...); err != nil {
		return fmt.Errorf("restic restore failed: %w", err)
	}

	return nil
}

func (r *templateRunner) runCommand(ctx context.Context, jobName, binary string, args ...string) error {
	return r.runCommandWithEnv(ctx, jobName, binary, nil, args...)
}

func (r *templateRunner) resolveDatabaseDumpConfig(job jobConfig) (resolvedDatabaseDumpConfig, error) {
	return resolvedDatabaseDumpConfig{
		Name:          strings.TrimSpace(job.DatabaseName),
		DumpTool:      r.cfg.Database.DumpTool,
		Host:          job.Host,
		Port:          job.Port,
		User:          job.User,
		PasswordEnv:   job.Password,
		Tables:        append([]string(nil), job.Tables...),
		ExcludeTables: append([]string(nil), job.ExcludeTables...),
		DumpFlags:     append([]string(nil), r.cfg.Database.DumpFlags...),
		Gzip:          r.cfg.Database.Gzip,
	}, nil
}

func databaseDumpOutputDir(job jobConfig) string {
	if strings.TrimSpace(job.OutputDir) != "" {
		return job.OutputDir
	}

	return filepath.Join(".", "backups", job.Name)
}

func databaseDumpFileName(jobName string, gzipEnabled bool, now time.Time) string {
	extension := ".sql"
	if gzipEnabled {
		extension += ".gz"
	}

	return fmt.Sprintf("%s_%s%s", jobName, now.Format("2006-01-02_15-04-05"), extension)
}

func (r *templateRunner) currentTime() time.Time {
	now := time.Now()
	if strings.TrimSpace(r.cfg.App.Timezone) == "" {
		return now
	}

	location, err := time.LoadLocation(r.cfg.App.Timezone)
	if err != nil {
		return now
	}

	return now.In(location)
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func createMySQLDefaultsFile(database resolvedDatabaseDumpConfig, password string) (string, error) {
	file, err := os.CreateTemp("", "backuper-mysql-*.cnf")
	if err != nil {
		return "", fmt.Errorf("create mysql defaults file: %w", err)
	}

	defer file.Close()

	if err := file.Chmod(0o600); err != nil {
		_ = os.Remove(file.Name())
		return "", fmt.Errorf("chmod mysql defaults file: %w", err)
	}

	content := fmt.Sprintf("[client]\nuser=%s\npassword=%s\nhost=%s\nport=%d\n", database.User, password, database.Host, database.Port)
	if _, err := file.WriteString(content); err != nil {
		_ = os.Remove(file.Name())
		return "", fmt.Errorf("write mysql defaults file: %w", err)
	}

	return file.Name(), nil
}

func buildDatabaseDumpArgs(defaultsFilePath string, database resolvedDatabaseDumpConfig) []string {
	args := []string{"--defaults-extra-file=" + defaultsFilePath}
	args = append(args, database.DumpFlags...)
	for _, table := range database.ExcludeTables {
		args = append(args, "--ignore-table="+database.Name+"."+table)
	}
	args = append(args, database.Name)
	args = append(args, database.Tables...)
	return args
}

func pruneDatabaseDumpFiles(dir string, retention *jobRetentionConfig, now time.Time) error {
	if retention == nil {
		return nil
	}

	files, err := listDatabaseDumpFiles(dir)
	if err != nil {
		return err
	}

	keep := selectDatabaseDumpFilesToKeep(files, retention, now.Location())
	for _, file := range files {
		if _, ok := keep[file.path]; ok {
			continue
		}
		if err := os.Remove(file.path); err != nil {
			return fmt.Errorf("remove expired dump file %s: %w", file.path, err)
		}
	}

	return nil
}

func listDatabaseDumpFiles(dir string) ([]databaseDumpFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read dump output dir %s: %w", dir, err)
	}

	files := make([]databaseDumpFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			return nil, fmt.Errorf("stat dump file %s: %w", path, err)
		}

		files = append(files, databaseDumpFile{
			path:    path,
			modTime: info.ModTime(),
		})
	}

	sort.Slice(files, func(i, j int) bool {
		if files[i].modTime.Equal(files[j].modTime) {
			return files[i].path > files[j].path
		}

		return files[i].modTime.After(files[j].modTime)
	})

	return files, nil
}

func selectDatabaseDumpFilesToKeep(files []databaseDumpFile, retention *jobRetentionConfig, location *time.Location) map[string]struct{} {
	keep := make(map[string]struct{})
	if retention == nil {
		return keep
	}
	if location == nil {
		location = time.Local
	}

	keepLatestDatabaseDumpBuckets(files, keep, retention.KeepHourly, func(t time.Time) string {
		return t.In(location).Format("2006-01-02 15")
	})
	keepLatestDatabaseDumpBuckets(files, keep, retention.KeepDaily, func(t time.Time) string {
		return t.In(location).Format("2006-01-02")
	})
	keepLatestDatabaseDumpBuckets(files, keep, retention.KeepWeekly, func(t time.Time) string {
		inLocation := t.In(location)
		year, week := inLocation.ISOWeek()
		return fmt.Sprintf("%04d-W%02d", year, week)
	})
	keepLatestDatabaseDumpBuckets(files, keep, retention.KeepMonthly, func(t time.Time) string {
		return t.In(location).Format("2006-01")
	})
	keepLatestDatabaseDumpBuckets(files, keep, retention.KeepYearly, func(t time.Time) string {
		return t.In(location).Format("2006")
	})

	return keep
}

func keepLatestDatabaseDumpBuckets(files []databaseDumpFile, keep map[string]struct{}, limit int, bucketKey func(time.Time) string) {
	if limit <= 0 {
		return
	}

	seenBuckets := make(map[string]struct{}, limit)
	for _, file := range files {
		key := bucketKey(file.modTime)
		if _, ok := seenBuckets[key]; ok {
			continue
		}

		seenBuckets[key] = struct{}{}
		keep[file.path] = struct{}{}

		if len(seenBuckets) == limit {
			return
		}
	}
}

type stackedWriteCloser struct {
	io.Writer
	closers []io.Closer
}

func (w *stackedWriteCloser) Close() error {
	var firstErr error
	for _, closer := range w.closers {
		if err := closer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func (r *templateRunner) runCommandWithEnv(ctx context.Context, jobName, binary string, envAdditions []string, args ...string) error {
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = mergeEnv(os.Environ(), envAdditions)

	output, err := cmd.CombinedOutput()
	if len(strings.TrimSpace(string(output))) > 0 {
		fmt.Fprintf(r.out, "[%s] %s\n", jobName, strings.TrimSpace(string(output)))
	}
	if err != nil {
		return fmt.Errorf("%s %s: %w", binary, strings.Join(args, " "), err)
	}

	return nil
}

func (r *templateRunner) runCommandOutputWithEnv(ctx context.Context, binary string, envAdditions []string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = mergeEnv(os.Environ(), envAdditions)

	output, err := cmd.CombinedOutput()
	if err != nil {
		commandLine := binary + " " + strings.Join(args, " ")
		trimmedOutput := strings.TrimSpace(string(output))
		if trimmedOutput == "" {
			return nil, fmt.Errorf("%s: %w", commandLine, err)
		}

		return nil, fmt.Errorf("%s: %w: %s", commandLine, err, trimmedOutput)
	}

	return output, nil
}

func shouldUseResticSFTPProxy(repository string) bool {
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(repository)), "sftp:") {
		return false
	}

	password, ok := os.LookupEnv(sftpPasswordEnv)
	return ok && strings.TrimSpace(password) != ""
}

func runResticSFTPProxy() int {
	password := strings.TrimSpace(os.Getenv(sftpPasswordEnv))
	if password == "" {
		fmt.Fprintf(os.Stderr, "%s is not set\n", sftpPasswordEnv)
		return 1
	}

	repository := strings.TrimSpace(os.Getenv(backuperResticRepoEnv))
	if repository == "" {
		fmt.Fprintf(os.Stderr, "%s is not set\n", backuperResticRepoEnv)
		return 1
	}

	connection, err := parseResticSFTPConnection(repository)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to parse restic repository %q: %v\n", repository, err)
		return 1
	}

	if value := strings.TrimSpace(os.Getenv("RESTIC_SFTP_HOST")); value != "" {
		connection.Host = value
	}
	if value := strings.TrimSpace(os.Getenv("RESTIC_SFTP_PORT")); value != "" {
		connection.Port = value
	}
	if value := strings.TrimSpace(os.Getenv("RESTIC_SFTP_USER")); value != "" {
		connection.User = value
	}

	if connection.Host == "" {
		fmt.Fprintln(os.Stderr, "restic sftp proxy requires a host")
		return 1
	}
	if connection.User == "" {
		fmt.Fprintln(os.Stderr, "restic sftp proxy requires a user")
		return 1
	}
	if connection.Port == "" {
		connection.Port = "22"
	}

	sshpassPath, err := exec.LookPath("sshpass")
	if err != nil {
		fmt.Fprintln(os.Stderr, "sshpass binary not found in PATH")
		return 1
	}

	sshPath, err := exec.LookPath("ssh")
	if err != nil {
		fmt.Fprintln(os.Stderr, "ssh binary not found in PATH")
		return 1
	}

	knownHostsPath := strings.TrimSpace(os.Getenv("RESTIC_SFTP_KNOWN_HOSTS"))
	if knownHostsPath == "" {
		knownHostsPath = defaultSFTPKnownHosts
	}

	cmd := exec.Command(
		sshpassPath,
		"-e",
		sshPath,
		"-o", "PreferredAuthentications=password",
		"-o", "PubkeyAuthentication=no",
		"-o", "StrictHostKeyChecking=accept-new",
		"-o", "UserKnownHostsFile="+knownHostsPath,
		"-p", connection.Port,
		"-l", connection.User,
		connection.Host,
		"-s", "sftp",
	)
	cmd.Env = mergeEnv(os.Environ(), []string{"SSHPASS=" + password})
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return exitErr.ExitCode()
		}

		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	return 0
}

func parseResticSFTPConnection(repository string) (resticSFTPConnection, error) {
	repository = strings.TrimSpace(repository)

	switch {
	case strings.HasPrefix(repository, "sftp://"):
		parsed, err := url.Parse(repository)
		if err != nil {
			return resticSFTPConnection{}, err
		}

		host := strings.TrimSpace(parsed.Hostname())
		if host == "" {
			return resticSFTPConnection{}, fmt.Errorf("missing host")
		}

		connection := resticSFTPConnection{
			User: parsed.User.Username(),
			Host: host,
			Port: parsed.Port(),
		}
		if connection.Port == "" {
			connection.Port = "22"
		}

		return connection, nil
	case strings.HasPrefix(repository, "sftp:"):
		target := strings.TrimPrefix(repository, "sftp:")
		target, _, found := strings.Cut(target, ":")
		if !found || strings.TrimSpace(target) == "" {
			return resticSFTPConnection{}, fmt.Errorf("expected sftp:user@host:/path or sftp:host:/path")
		}

		if strings.Contains(target, "@") {
			user, host, _ := strings.Cut(target, "@")
			return resticSFTPConnection{
				User: strings.TrimSpace(user),
				Host: strings.TrimSpace(host),
				Port: "22",
			}, nil
		}

		return resticSFTPConnection{
			Host: strings.TrimSpace(target),
			Port: "22",
		}, nil
	default:
		return resticSFTPConnection{}, fmt.Errorf("unsupported repository scheme")
	}
}

func mergeEnv(base []string, additions []string) []string {
	if len(additions) == 0 {
		return append([]string(nil), base...)
	}

	merged := append([]string(nil), base...)
	indexByKey := make(map[string]int, len(merged))
	for i, entry := range merged {
		key, _, _ := strings.Cut(entry, "=")
		indexByKey[key] = i
	}

	for _, entry := range additions {
		key, _, _ := strings.Cut(entry, "=")
		if index, ok := indexByKey[key]; ok {
			merged[index] = entry
			continue
		}

		indexByKey[key] = len(merged)
		merged = append(merged, entry)
	}

	return merged
}

func planResticWorkUnits(job jobConfig) ([]resticWorkUnit, error) {
	switch len(job.Tags) {
	case 0:
		return []resticWorkUnit{
			{sources: append([]string(nil), job.Sources...)},
		}, nil
	case 1:
		return []resticWorkUnit{
			{sources: append([]string(nil), job.Sources...), tag: job.Tags[0]},
		}, nil
	default:
		if len(job.Tags) != len(job.Sources) {
			return nil, fmt.Errorf("job %s: tags must contain exactly one tag or match the number of sources", job.Name)
		}

		units := make([]resticWorkUnit, 0, len(job.Sources))
		for i, source := range job.Sources {
			units = append(units, resticWorkUnit{
				sources: []string{source},
				tag:     job.Tags[i],
			})
		}

		return units, nil
	}
}

func retentionArgs(retention *jobRetentionConfig) []string {
	if retention == nil {
		return nil
	}

	args := make([]string, 0, 10)
	if retention.KeepHourly > 0 {
		args = append(args, "--keep-hourly", strconv.Itoa(retention.KeepHourly))
	}
	if retention.KeepDaily > 0 {
		args = append(args, "--keep-daily", strconv.Itoa(retention.KeepDaily))
	}
	if retention.KeepWeekly > 0 {
		args = append(args, "--keep-weekly", strconv.Itoa(retention.KeepWeekly))
	}
	if retention.KeepMonthly > 0 {
		args = append(args, "--keep-monthly", strconv.Itoa(retention.KeepMonthly))
	}
	if retention.KeepYearly > 0 {
		args = append(args, "--keep-yearly", strconv.Itoa(retention.KeepYearly))
	}

	return args
}

func jobMatchesSchedule(schedule string, now time.Time) (bool, error) {
	fields := strings.Fields(schedule)
	if len(fields) != 5 {
		return false, fmt.Errorf("expected 5 fields, got %d", len(fields))
	}

	matchers := []struct {
		expr     string
		value    int
		min, max int
	}{
		{expr: fields[0], value: now.Minute(), min: 0, max: 59},
		{expr: fields[1], value: now.Hour(), min: 0, max: 23},
		{expr: fields[2], value: now.Day(), min: 1, max: 31},
		{expr: fields[3], value: int(now.Month()), min: 1, max: 12},
		{expr: fields[4], value: int(now.Weekday()), min: 0, max: 6},
	}

	for _, matcher := range matchers {
		ok, err := cronFieldMatches(matcher.expr, matcher.value, matcher.min, matcher.max)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}

	return true, nil
}

func cronFieldMatches(expr string, value, min, max int) (bool, error) {
	if expr == "*" {
		return true, nil
	}

	for _, part := range strings.Split(expr, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return false, errors.New("empty cron field part")
		}

		switch {
		case strings.HasPrefix(part, "*/"):
			step, err := strconv.Atoi(strings.TrimPrefix(part, "*/"))
			if err != nil || step <= 0 {
				return false, fmt.Errorf("invalid step %q", part)
			}
			if (value-min)%step == 0 {
				return true, nil
			}
		case strings.Contains(part, "-"):
			rangeParts := strings.Split(part, "-")
			if len(rangeParts) != 2 {
				return false, fmt.Errorf("invalid range %q", part)
			}
			start, err := strconv.Atoi(rangeParts[0])
			if err != nil {
				return false, fmt.Errorf("invalid range start %q", part)
			}
			end, err := strconv.Atoi(rangeParts[1])
			if err != nil {
				return false, fmt.Errorf("invalid range end %q", part)
			}
			if start < min || end > max || start > end {
				return false, fmt.Errorf("range %q is outside allowed bounds", part)
			}
			if value >= start && value <= end {
				return true, nil
			}
		default:
			number, err := strconv.Atoi(part)
			if err != nil {
				return false, fmt.Errorf("unsupported cron field value %q", part)
			}
			if number < min || number > max {
				return false, fmt.Errorf("cron field value %q is outside allowed bounds", part)
			}
			if value == number {
				return true, nil
			}
		}
	}

	return false, nil
}
