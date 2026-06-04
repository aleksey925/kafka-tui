package cli

import (
	"crypto/rand"
	"encoding/base32"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"github.com/aleksey925/kafka-tui/internal/config"
)

const cliInlineSuffix = "-cli"

// generateInlineName produces "<random>-cli" via crypto/rand → base32
// (8 chars), with a nanosecond-timestamp fallback for the rare case of
// a broken entropy source. See CLAUDE.md § "CLI inline cluster".
func generateInlineName() string {
	var b [5]byte
	if _, err := rand.Read(b[:]); err != nil {
		nano := time.Now().UnixNano()
		for i := range b {
			b[i] = byte(nano >> (8 * i))
		}
	}
	encoded := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b[:])
	return strings.ToLower(encoded) + cliInlineSuffix
}

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

func (c *CLICluster) HasInlineCluster() bool {
	return c != nil && len(c.Brokers) > 0
}

// Flags is the parsed result of CLI arguments.
type Flags struct {
	ShowVersion bool
	ShowLogs    bool
	ShowLogsDir bool

	ConfigPath string

	ClusterName string
	// Inline is resolved through the loader's per-cluster pipeline, not the
	// global placeholder phase, so a bad ${vault:...} in --sasl-password
	// quarantines the inline cluster instead of aborting startup.
	Inline CLICluster `placeholder:"-"`

	// LogLevel, when non-empty, overrides cfg.Logging.Level from YAML.
	// Accepts the same values as logging.ParseLevel (debug|info|warn|error).
	LogLevel string

	// VaultAddr and VaultToken override the corresponding fields of
	// cfg.Vault loaded from YAML. They accept the same placeholder syntax
	// (${env:...}, ${file:...}) as YAML values, resolved by the loader
	// during the env+file phase before the vault client is constructed.
	VaultAddr  string
	VaultToken string
}

type ParseError struct {
	Msg string
}

func (e *ParseError) Error() string { return e.Msg }

// ErrHelpRequested is returned when --help / -h is encountered. The caller should
// treat this as a successful early exit.
var ErrHelpRequested = errors.New("cli: help requested")

// Parse parses the given args (without the program name) and returns Flags.
func Parse(args []string, stdout, stderr io.Writer) (*Flags, error) {
	fs := pflag.NewFlagSet("kafka-tui", pflag.ContinueOnError)
	fs.SetOutput(stderr)

	flags := &Flags{}

	fs.BoolVarP(&flags.ShowVersion, "version", "v", false, "print version and exit")
	fs.BoolVar(&flags.ShowLogs, "logs", false, "open the log file in $PAGER and exit")
	fs.BoolVar(&flags.ShowLogsDir, "logs-dir", false, "print the log directory and exit")

	fs.StringVar(&flags.ConfigPath, "config", "", "path to a config file or directory (disables config hierarchy lookup)")

	fs.StringVar(&flags.ClusterName, "cluster", "", "name of a cluster from clusters.yaml to auto-connect to at startup")

	fs.StringSliceVar(&flags.Inline.Brokers, "brokers", nil, "comma-separated broker addresses; creates an inline cluster named <random>-cli for this session")
	fs.StringVar(&flags.Inline.Color, "color", "", "cluster color (red|yellow|green|gray|white) for the inline CLI cluster")
	fs.BoolVar(&flags.Inline.ReadOnly, "read-only", false, "mark the inline CLI cluster as read-only")

	fs.BoolVar(&flags.Inline.TLSEnabled, "tls", false, "enable TLS for the inline CLI cluster")
	fs.StringVar(&flags.Inline.TLSCAFile, "tls-ca", "", "path to a TLS CA certificate (requires --tls)")
	fs.StringVar(&flags.Inline.TLSCertFile, "tls-cert", "", "path to a TLS client certificate (requires --tls)")
	fs.StringVar(&flags.Inline.TLSKeyFile, "tls-key", "", "path to a TLS client key (requires --tls)")
	fs.BoolVar(&flags.Inline.TLSSkipVerify, "tls-skip-verify", false, "skip TLS verification (requires --tls)")

	fs.StringVar(&flags.Inline.SASLMechanism, "sasl-mechanism", "", "SASL mechanism (PLAIN|SCRAM-SHA-256|SCRAM-SHA-512)")
	fs.StringVar(&flags.Inline.SASLUsername, "sasl-username", "", "SASL username")
	fs.StringVar(&flags.Inline.SASLPassword, "sasl-password", "", "SASL password — a literal value is visible in `ps`/`/proc/<pid>/cmdline`; prefer ${env:VAR}, ${file:/path}, or ${vault:path#key}")

	fs.StringVar(&flags.LogLevel, "log-level", "", "log level (debug|info|warn|error); overrides logging.level from config")

	fs.StringVar(&flags.VaultAddr, "vault-addr", "", "Vault address; overrides vault.address from config")
	fs.StringVar(&flags.VaultToken, "vault-token", "", "Vault token — a literal value is visible in `ps`/`/proc/<pid>/cmdline`; prefer ${env:VAR} or ${file:/path}; overrides vault.token from config")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, pflag.ErrHelp) {
			return nil, ErrHelpRequested
		}
		return nil, fmt.Errorf("cli: %w", err)
	}

	if fs.NArg() > 0 {
		return nil, &ParseError{Msg: fmt.Sprintf("unexpected argument: %q", fs.Arg(0))}
	}

	if flags.Inline.HasInlineCluster() {
		// --cluster is a separate selector concern, handled by the
		// clusters screen; the inline name is always auto-generated.
		flags.Inline.Name = generateInlineName()
	}

	if err := validate(flags); err != nil {
		_, _ = fmt.Fprintln(stderr, err.Error())
		return nil, err
	}

	_ = stdout
	return flags, nil
}

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
	if err := validateLogLevel(f); err != nil {
		return err
	}
	return validateExitFlags(f)
}

func validateLogLevel(f *Flags) error {
	if f.LogLevel == "" {
		return nil
	}
	norm, ok := config.NormalizeEnum(f.LogLevel, config.AllowedLogLevels)
	if !ok {
		return &ParseError{Msg: fmt.Sprintf("invalid --log-level %q (allowed: debug, info, warn, error)", f.LogLevel)}
	}
	f.LogLevel = norm
	return nil
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
	if hasMech {
		norm, ok := config.NormalizeEnum(c.SASLMechanism, config.AllowedSASLMechanisms)
		if !ok {
			return &ParseError{Msg: fmt.Sprintf("invalid --sasl-mechanism %q (allowed: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)", c.SASLMechanism)}
		}
		c.SASLMechanism = norm
	}
	return nil
}

func validateCluster(c *CLICluster) error {
	if c.HasInlineCluster() && slices.Contains(c.Brokers, "") {
		return &ParseError{Msg: "--brokers contains an empty entry"}
	}
	if c.Color != "" && !c.HasInlineCluster() {
		return &ParseError{Msg: "--color requires --brokers (inline cluster)"}
	}
	if c.ReadOnly && !c.HasInlineCluster() {
		return &ParseError{Msg: "--read-only requires --brokers (inline cluster)"}
	}
	if c.Color == "" {
		return nil
	}
	norm, ok := config.NormalizeEnum(c.Color, config.AllowedClusterColors)
	if !ok {
		return &ParseError{Msg: fmt.Sprintf("invalid --color %q (allowed: red, yellow, green, gray, white)", c.Color)}
	}
	c.Color = norm
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

// CredentialExposureWarnings returns one warning per credential-bearing
// flag whose value is a literal. See CLAUDE.md § Credentials: storage
// and exposure warnings. Must be called BEFORE the env+file phase
// resolves placeholders.
func CredentialExposureWarnings(f *Flags) []string {
	if f == nil {
		return nil
	}
	var warns []string
	if w := plainCredentialWarning("--sasl-password", f.Inline.SASLPassword); w != "" {
		warns = append(warns, w)
	}
	if w := plainCredentialWarning("--vault-token", f.VaultToken); w != "" {
		warns = append(warns, w)
	}
	return warns
}

func plainCredentialWarning(flagName, value string) string {
	if !config.IsLiteralCredential(value) {
		return ""
	}
	return fmt.Sprintf(
		"%s: literal value passed on command line is visible to other "+
			"processes via ps / /proc; prefer ${env:VAR}, ${file:/path}, "+
			"or ${vault:path#key}",
		flagName,
	)
}

// MustParseOrExit parses os.Args, exits with code 2 on error, and returns
// false when the process should exit cleanly (help shown).
func MustParseOrExit() (*Flags, bool) {
	f, err := Parse(os.Args[1:], os.Stdout, os.Stderr)
	switch {
	case errors.Is(err, ErrHelpRequested):
		return nil, false
	case err != nil:
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(2)
	}
	return f, true
}
