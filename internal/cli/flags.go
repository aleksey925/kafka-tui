package cli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"slices"

	"github.com/spf13/pflag"
)

// CLICluster holds an inline cluster definition assembled from CLI flags.
// It is populated only when --brokers is provided.
type CLICluster struct {
	Name          string
	Brokers       []string
	Color         string
	ReadOnly      bool
	TLSEnabled    bool
	TLSCAFile     string
	TLSCertFile   string
	TLSKeyFile    string
	TLSSkipVerify bool
	SASLMechanism string
	SASLUsername  string
	SASLPassword  string
}

// HasInlineCluster reports whether enough flags were provided to build an inline cluster.
func (c *CLICluster) HasInlineCluster() bool {
	return c != nil && len(c.Brokers) > 0
}

// Flags is the parsed result of CLI arguments.
type Flags struct {
	// Mode flags — when set, exit before starting the TUI.
	ShowVersion bool
	ShowLogs    bool
	ShowLogsDir bool

	// Configuration overrides.
	ConfigPath string

	// Cluster selection / inline definition.
	ClusterName string
	Inline      CLICluster
}

// ParseError describes a CLI flag validation problem.
type ParseError struct {
	Msg string
}

func (e *ParseError) Error() string { return e.Msg }

// ErrHelpRequested is returned when --help / -h is encountered. The caller should
// treat this as a successful early exit.
var ErrHelpRequested = errors.New("cli: help requested")

// Parse parses the given args (without the program name) and returns Flags.
//
// stdout / stderr are wired into the underlying pflag.FlagSet so the caller can
// capture or redirect output (used in tests).
func Parse(args []string, stdout, stderr io.Writer) (*Flags, error) {
	fs := pflag.NewFlagSet("kafka-tui", pflag.ContinueOnError)
	fs.SetOutput(stderr)

	flags := &Flags{}

	fs.BoolVar(&flags.ShowVersion, "version", false, "print version and exit")
	fs.BoolVar(&flags.ShowLogs, "logs", false, "open the log file in $PAGER and exit")
	fs.BoolVar(&flags.ShowLogsDir, "logs-dir", false, "print the log directory and exit")

	fs.StringVar(&flags.ConfigPath, "config", "", "path to a config file or directory (disables config hierarchy lookup)")

	fs.StringVar(&flags.ClusterName, "cluster", "", "cluster name to connect to (matches name from clusters.yaml or --brokers)")

	fs.StringSliceVar(&flags.Inline.Brokers, "brokers", nil, "comma-separated list of broker addresses (defines an inline CLI cluster)")
	fs.StringVar(&flags.Inline.Color, "color", "", "cluster color (red|yellow|green|gray|white) for the inline CLI cluster")
	fs.BoolVar(&flags.Inline.ReadOnly, "read-only", false, "mark the inline CLI cluster as read-only")

	fs.BoolVar(&flags.Inline.TLSEnabled, "tls", false, "enable TLS for the inline CLI cluster")
	fs.StringVar(&flags.Inline.TLSCAFile, "tls-ca", "", "path to a TLS CA certificate (requires --tls)")
	fs.StringVar(&flags.Inline.TLSCertFile, "tls-cert", "", "path to a TLS client certificate (requires --tls)")
	fs.StringVar(&flags.Inline.TLSKeyFile, "tls-key", "", "path to a TLS client key (requires --tls)")
	fs.BoolVar(&flags.Inline.TLSSkipVerify, "tls-skip-verify", false, "skip TLS verification (requires --tls)")

	fs.StringVar(&flags.Inline.SASLMechanism, "sasl-mechanism", "", "SASL mechanism (PLAIN|SCRAM-SHA-256|SCRAM-SHA-512)")
	fs.StringVar(&flags.Inline.SASLUsername, "sasl-username", "", "SASL username")
	fs.StringVar(&flags.Inline.SASLPassword, "sasl-password", "", "SASL password")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, pflag.ErrHelp) {
			return nil, ErrHelpRequested
		}
		return nil, fmt.Errorf("cli: %w", err)
	}

	// resolve cluster name when only --brokers is given
	if flags.Inline.HasInlineCluster() && flags.Inline.Name == "" {
		flags.Inline.Name = flags.ClusterName
		if flags.Inline.Name == "" {
			flags.Inline.Name = "cli"
		}
	}

	if err := validate(flags); err != nil {
		_, _ = fmt.Fprintln(stderr, err.Error())
		return nil, err
	}

	_ = stdout
	return flags, nil
}

// validate enforces cross-flag invariants (e.g. TLS sub-flags require --tls).
func validate(f *Flags) error {
	if err := validateTLS(&f.Inline); err != nil {
		return err
	}
	if err := validateSASL(&f.Inline); err != nil {
		return err
	}
	if err := validateCluster(&f.Inline); err != nil {
		return err
	}
	return validateExitFlags(f)
}

func validateTLS(c *CLICluster) error {
	requiresTLS := []struct {
		name string
		set  bool
	}{
		{"--tls-ca", c.TLSCAFile != ""},
		{"--tls-cert", c.TLSCertFile != ""},
		{"--tls-key", c.TLSKeyFile != ""},
		{"--tls-skip-verify", c.TLSSkipVerify},
	}
	if !c.TLSEnabled {
		for _, r := range requiresTLS {
			if r.set {
				return &ParseError{Msg: fmt.Sprintf("flag %s requires --tls", r.name)}
			}
		}
	}
	if (c.TLSCertFile != "") != (c.TLSKeyFile != "") {
		return &ParseError{Msg: "flags --tls-cert and --tls-key must be specified together"}
	}
	if c.TLSEnabled && !c.HasInlineCluster() {
		return &ParseError{Msg: "--tls requires --brokers (inline cluster)"}
	}
	return nil
}

func validateSASL(c *CLICluster) error {
	hasUser := c.SASLUsername != ""
	hasPass := c.SASLPassword != ""
	hasMech := c.SASLMechanism != ""
	anySet := hasUser || hasPass || hasMech
	allSet := hasUser && hasPass && hasMech
	if anySet && !allSet {
		return &ParseError{Msg: "flags --sasl-mechanism, --sasl-username, --sasl-password must be specified together"}
	}
	if anySet && !c.HasInlineCluster() {
		return &ParseError{Msg: "SASL flags require --brokers (inline cluster)"}
	}
	return nil
}

func validateCluster(c *CLICluster) error {
	if c.HasInlineCluster() && slices.Contains(c.Brokers, "") {
		return &ParseError{Msg: "--brokers contains an empty entry"}
	}
	if c.Color != "" {
		switch c.Color {
		case "red", "yellow", "green", "gray", "white":
		default:
			return &ParseError{Msg: fmt.Sprintf("invalid --color %q (allowed: red, yellow, green, gray, white)", c.Color)}
		}
	}
	return nil
}

func validateExitFlags(f *Flags) error {
	exitOnly := boolToInt(f.ShowVersion) + boolToInt(f.ShowLogs) + boolToInt(f.ShowLogsDir)
	if exitOnly > 1 {
		return &ParseError{Msg: "--version, --logs and --logs-dir are mutually exclusive"}
	}
	return nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// MustParseOrExit parses os.Args and prints any error to stderr, exiting with code 2 on error.
// Returns false when the process should exit cleanly (help shown).
func MustParseOrExit() (*Flags, bool) {
	f, err := Parse(os.Args[1:], os.Stdout, os.Stderr)
	switch {
	case errors.Is(err, ErrHelpRequested):
		return nil, false
	case err != nil:
		os.Exit(2)
	}
	return f, true
}
