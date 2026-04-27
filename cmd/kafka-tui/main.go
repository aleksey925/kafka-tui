package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/aleksey925/kafka-tui/internal/cli"
	"github.com/aleksey925/kafka-tui/internal/config"
	"github.com/aleksey925/kafka-tui/internal/logging"
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

	// TUI start-up lands in Task 10.
	_, _ = fmt.Fprintln(os.Stdout, "kafka-tui: TUI is not implemented yet (arrives in Task 10).")
	if flags.ClusterName != "" {
		_, _ = fmt.Fprintf(os.Stdout, "  cluster: %s\n", flags.ClusterName)
	}
	if flags.Inline.HasInlineCluster() {
		_, _ = fmt.Fprintf(os.Stdout, "  inline brokers: %v\n", flags.Inline.Brokers)
	}
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
