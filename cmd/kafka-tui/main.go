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
	"github.com/aleksey925/kafka-tui/internal/tui/screens/messages"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/produce"
	"github.com/aleksey925/kafka-tui/internal/version"
)

//nolint:gochecknoglobals // ldflags target.
var ver = "0.0.0"

func main() {
	flags, ok := cli.MustParseOrExit()
	if !ok {
		return
	}

	// --version doesn't read config and shouldn't depend on placeholder
	// resolution — handle it before ResolveAll so a stranded ${vault:...}
	// on an unrelated flag doesn't block debugging the binary itself.
	if flags.ShowVersion {
		_, _ = fmt.Fprintln(os.Stdout, version.NewBuildInfo(ver).Display())
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

// run is split out from main so deferred cleanup runs before any os.Exit.
func run(flags *cli.Flags) error {
	loaderOpts := config.LoaderOptions{
		ConfigPath:     flags.ConfigPath,
		CLIClusterName: flags.Inline.Name,
	}
	watcher, loaded, err := config.NewWatcher(loaderOpts, flags.Inline.Name, 0)
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
		_ = watcher.Close()
		return fmt.Errorf("init logging: %w", err)
	}
	// logger.Close must be deferred LAST so all other tear-down (watcher,
	// state store, etc.) can still log on shutdown — defers fire LIFO.
	defer func() { _ = logger.Close() }()
	defer func() { _ = watcher.Close() }()
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
	method, err := clipboard.ParseMethod(loaded.Config.Clipboard.Method)
	if err != nil {
		return fmt.Errorf("init clipboard: %w", err)
	}
	clip := clipboard.New(clipboard.Options{Method: method})

	boot := &tui.Bootstrap{
		Loaded:            loaded,
		Clusters:          clusterList,
		CLIName:           cliClu,
		GlobalPath:        globalPath,
		ProjectPath:       projectPath,
		LogPath:           logger.ResolvedAt,
		Dialer:            dialer,
		Pinger:            tui.NewClusterPinger(dialer, 5*time.Second),
		Editor:            clusters.DefaultEditor(),
		History:           produceHistory(store, loaded.Config.Produce.HistorySize, logger.Logger),
		MessagesViewState: messagesViewState(store, logger.Logger),
		Clipboard:         clip,
		Pager:             produce.DefaultPagerOpener(),
		StartupWarnings:   loaded.Warnings,
		ReadOnly:          flags.Inline.ReadOnly,
		ConfigReloader: func() (*config.Loaded, []config.Cluster, string, error) {
			fresh, err := config.Load(loaderOpts)
			if err != nil {
				return nil, nil, "", fmt.Errorf("reload config: %w", err)
			}
			list, cli := buildClusterList(fresh.Clusters, flags.Inline)
			return fresh, list, cli, nil
		},
		ConfigSnapshots: watcher.Snapshots(),
		BuildClusterList: func(c []config.Cluster) ([]config.Cluster, string) {
			return buildClusterList(c, flags.Inline)
		},
	}

	model := tui.New(tui.Options{
		Cluster:      flags.ClusterName,
		ClusterColor: flags.Inline.Color,
		ReadOnly:     flags.Inline.ReadOnly,
		FromCLI:      flags.Inline.HasInlineCluster(),
		Initial:      tui.ScreenClusters,
		Build:        version.NewBuildInfo(ver),
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

// buildClusterList prepends the CLI inline cluster onto the loaded list. The
// loader has already removed any same-named entry from clusters.yaml before
// we get here, so a simple append is safe.
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

// cliInlineToCluster only populates SASL / TLS sub-structs when at least one
// of their fields was set on the command line.
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
// matching the loader's project-detection logic.
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

func produceHistory(store *state.Store, histSize int, log *slog.Logger) produce.History {
	if store == nil {
		return nil
	}
	if histSize <= 0 {
		histSize = produce.DefaultHistorySize
	}
	return tui.NewStateHistory(store, histSize, log)
}

func messagesViewState(store *state.Store, log *slog.Logger) messages.ViewStateRepository {
	if store == nil {
		return nil
	}
	return tui.NewStateMessagesView(store, log)
}

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
