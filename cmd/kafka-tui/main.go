package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/cli"
	"github.com/aleksey925/kafka-tui/internal/config"
	"github.com/aleksey925/kafka-tui/internal/logging"
	"github.com/aleksey925/kafka-tui/internal/tui"
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

	if err := config.EnvFileResolvers().ResolveStruct(flags); err != nil {
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

	model := tui.New(tui.Options{
		Cluster:      flags.ClusterName,
		ClusterColor: flags.Inline.Color,
		ReadOnly:     flags.Inline.ReadOnly,
		FromCLI:      flags.Inline.HasInlineCluster(),
		Initial:      tui.ScreenClusters,
		Version:      versionString,
		Commit:       commit,
	})

	if _, err := tea.NewProgram(model).Run(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
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
