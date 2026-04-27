package main

import (
	"fmt"
	"os"

	"github.com/aleksey925/kafka-tui/internal/cli"
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
		// real implementation lands in Task 3 (logging).
		_, _ = fmt.Fprintln(os.Stderr, "--logs-dir: not yet implemented (logging arrives in Task 3)")
		os.Exit(1)
	case flags.ShowLogs:
		_, _ = fmt.Fprintln(os.Stderr, "--logs: not yet implemented (logging arrives in Task 3)")
		os.Exit(1)
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
