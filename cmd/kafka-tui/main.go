package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/cli"
	"github.com/aleksey925/kafka-tui/internal/clipboard"
	"github.com/aleksey925/kafka-tui/internal/config"
	"github.com/aleksey925/kafka-tui/internal/logging"
	"github.com/aleksey925/kafka-tui/internal/state"
	"github.com/aleksey925/kafka-tui/internal/tui"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/clusters"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/produce"
	"github.com/aleksey925/kafka-tui/internal/version"
)

// these variables are populated at build time via -ldflags "-X main.version=... -X main.commit=...".
//
//nolint:gochecknoglobals // ldflags target.
var (
	versionString = "dev"
	commit        = ""
)

func main() {
	flags, ok := cli.MustParseOrExit()
	if !ok {
		// help shown — clean exit.
		return
	}

	// ResolveAll runs env+file then asserts no placeholders remain, so a
	// stranded ${vault:...} on a CLI flag (e.g. --sasl-password) fails at
	// startup instead of silently propagating the literal placeholder string.
	if err := config.ResolveAll(flags, nil); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	switch {
	case flags.ShowVersion:
		_, _ = fmt.Fprintln(os.Stdout, version.Format(versionString, commit))
		return
	case flags.ShowLogsDir:
		path, err := resolveLogPath(flags)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
		_, _ = fmt.Fprintln(os.Stdout, filepath.Dir(path))
		return
	case flags.ShowLogs:
		path, err := resolveLogPath(flags)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
		if err := logging.OpenInPager(context.Background(), path); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
		return
	}

	if err := run(flags); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

// run wires bootstrap (config, logging, state, kafka dialer) and starts the
// Bubble Tea program. Split out from main so deferred cleanup runs before
// any os.Exit.
func run(flags *cli.Flags) error {
	loaded, err := config.Load(config.LoaderOptions{
		ConfigPath:     flags.ConfigPath,
		CLIClusterName: flags.Inline.Name,
	})
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger, err := logging.Init(logging.Options{
		Level:     loaded.Config.Logging.Level,
		File:      loaded.Config.Logging.File,
		MaxSizeMB: loaded.Config.Logging.MaxSizeMB,
		MaxFiles:  loaded.Config.Logging.MaxFiles,
	})
	if err != nil {
		return fmt.Errorf("init logging: %w", err)
	}
	defer func() { _ = logger.Close() }()
	slog.SetDefault(logger.Logger)

	store, err := state.Open(context.Background(), "")
	if err != nil {
		// state is non-critical — log and proceed without history.
		slog.Warn("state: open failed, history disabled", "err", err)
	}
	defer func() {
		if store != nil {
			_ = store.Close()
		}
	}()

	clusterList, cliClu := buildClusterList(loaded.Clusters, flags.Inline)

	globalPath, projectPath := configPaths()

	dialer := tui.NewKafkaDialer("kafka-tui")
	clip := clipboard.New(clipboard.Options{})

	boot := &tui.Bootstrap{
		Loaded:          loaded,
		Clusters:        clusterList,
		CLIName:         cliClu,
		GlobalPath:      globalPath,
		ProjectPath:     projectPath,
		LogPath:         logger.ResolvedAt,
		Dialer:          dialer,
		Pinger:          tui.NewClusterPinger(dialer, 5*time.Second),
		Editor:          clusters.DefaultEditor(),
		History:         produceHistory(store, logger.Logger),
		Clipboard:       clip,
		Pager:           produce.DefaultPagerOpener(),
		StartupWarnings: loaded.Warnings,
		ReadOnly:        flags.Inline.ReadOnly,
	}

	model := tui.New(tui.Options{
		Cluster:      flags.ClusterName,
		ClusterColor: flags.Inline.Color,
		ReadOnly:     flags.Inline.ReadOnly,
		FromCLI:      flags.Inline.HasInlineCluster(),
		Initial:      tui.ScreenClusters,
		Version:      versionString,
		Commit:       commit,
		Bootstrap:    boot,
	})

	if _, err := tea.NewProgram(model).Run(); err != nil {
		return fmt.Errorf("tui: %w", err)
	}
	if c := model.ActiveClient(); c != nil {
		c.Close()
	}
	return nil
}

// buildClusterList prepends/replaces the CLI inline cluster onto the loaded
// list. Returns the merged list and the CLI cluster name (empty when no
// --brokers was given). The loader has already removed any same-named entry
// from clusters.yaml before we get here, so a simple append is safe.
func buildClusterList(loaded []config.Cluster, inline cli.CLICluster) ([]config.Cluster, string) {
	if !inline.HasInlineCluster() {
		return loaded, ""
	}
	c := cliInlineToCluster(inline)
	merged := make([]config.Cluster, 0, len(loaded)+1)
	merged = append(merged, c)
	merged = append(merged, loaded...)
	return merged, c.Name
}

// cliInlineToCluster converts the flat CLI inline-cluster shape into a
// [config.Cluster]. SASL / TLS sub-structs are only populated when at least
// one of their fields was set on the command line.
func cliInlineToCluster(inline cli.CLICluster) config.Cluster {
	c := config.Cluster{
		Name:     inline.Name,
		Brokers:  append([]string(nil), inline.Brokers...),
		Color:    inline.Color,
		ReadOnly: inline.ReadOnly,
	}
	if inline.SASLMechanism != "" || inline.SASLUsername != "" || inline.SASLPassword != "" {
		c.SASL = &config.SASLConfig{
			Mechanism: inline.SASLMechanism,
			Username:  inline.SASLUsername,
			Password:  inline.SASLPassword,
		}
	}
	if inline.TLSEnabled || inline.TLSCAFile != "" || inline.TLSCertFile != "" || inline.TLSKeyFile != "" || inline.TLSSkipVerify {
		c.TLS = &config.TLSConfig{
			CAFile:     inline.TLSCAFile,
			CertFile:   inline.TLSCertFile,
			KeyFile:    inline.TLSKeyFile,
			SkipVerify: inline.TLSSkipVerify,
		}
	}
	return c
}

// configPaths returns the absolute paths of the global and project config
// directories' clusters.yaml files (best-effort — empty when unavailable).
// Used by the clusters screen edit-target chooser.
func configPaths() (globalPath, projectPath string) {
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		globalPath = filepath.Join(home, config.DirName, config.ClustersFileName)
	}
	if cwd, err := os.Getwd(); err == nil {
		if pd, ok := findProjectDir(cwd); ok {
			projectPath = filepath.Join(pd, config.ClustersFileName)
		}
	}
	return globalPath, projectPath
}

// findProjectDir walks parents of startDir looking for a `.kafka-tui/` dir,
// matching the loader's project-detection logic. Returns the absolute path
// of that directory.
func findProjectDir(startDir string) (string, bool) {
	dir, err := filepath.Abs(startDir)
	if err != nil {
		return "", false
	}
	for {
		candidate := filepath.Join(dir, config.DirName)
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			return candidate, true
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", false
		}
		dir = parent
	}
}

// produceHistory builds a [tui.NewStateHistory] when the store opened
// successfully. Otherwise returns nil so produce.Options.History stays nil.
func produceHistory(store *state.Store, log *slog.Logger) produce.History {
	if store == nil {
		return nil
	}
	return tui.NewStateHistory(store, log)
}

// resolveLogPath loads config (without failing on missing files) and returns
// the resolved absolute path of the log file.
func resolveLogPath(flags *cli.Flags) (string, error) {
	loaded, err := config.Load(config.LoaderOptions{ConfigPath: flags.ConfigPath})
	logFile := config.Defaults().Logging.File
	if err == nil && loaded != nil {
		logFile = loaded.Config.Logging.File
	}
	resolved, resolveErr := logging.ResolveFilePath(logFile, "")
	if resolveErr != nil {
		return "", fmt.Errorf("resolve log path: %w", resolveErr)
	}
	return resolved, nil
}
