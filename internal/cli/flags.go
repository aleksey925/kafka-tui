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

	"github.com/spf13/cobra"

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

// CLICluster holds an inline cluster definition assembled from the
// `connect --brokers` flags. It is populated only when --brokers is provided.
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

	// ClusterName is the `connect <name>` positional: a clusters.yaml cluster
	// to auto-connect to. Empty when launching the picker or an inline cluster.
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

// ErrExitEarly is returned when a built-in command already produced its own
// output (--help / -h, the help subcommand, or shell completion) and the
// process must exit cleanly instead of launching the TUI.
var ErrExitEarly = errors.New("cli: handled, exit early")

// Parse parses the given args (without the program name) and returns Flags.
func Parse(args []string, stdout, stderr io.Writer) (*Flags, error) {
	flags := &Flags{}
	helpRequested := false

	root := newRootCmd(flags, &helpRequested)
	// cobra falls back to os.Args[1:] when SetArgs receives nil; force an
	// empty slice so a nil/empty invocation parses to "no flags".
	if args == nil {
		args = []string{}
	}
	root.SetArgs(args)
	root.SetOut(stdout)
	root.SetErr(stderr)

	executed, err := root.ExecuteC()
	if err != nil {
		// surfaced verbatim by MustParseOrExit; it is either our ParseError
		// or cobra's flag-parse error, and wrapping would corrupt the
		// "unknown flag" / *ParseError messages the caller relies on.
		//nolint:wrapcheck // intentional passthrough of the parse error.
		return nil, err
	}
	// help and shell-completion commands print their own output; they must
	// not fall through to launching the TUI.
	if helpRequested || isCompletionCmd(executed) {
		return nil, ErrExitEarly
	}
	return flags, nil
}

// isCompletionCmd reports whether the executed command is cobra's user-facing
// `completion` command (or one of its shell subcommands) or the hidden
// `__complete*` command the shell calls at runtime — all of which emit their
// own output and self-terminate.
func isCompletionCmd(cmd *cobra.Command) bool {
	for c := cmd; c != nil; c = c.Parent() {
		if c.Name() == "completion" || strings.HasPrefix(c.Name(), cobra.ShellCompRequestCmd) {
			return true
		}
	}
	return false
}

func newRootCmd(flags *Flags, helpRequested *bool) *cobra.Command {
	root := &cobra.Command{
		Use:   "kafka-tui",
		Short: "A terminal client for Apache Kafka in the spirit of k9s",
		Args:  cobra.NoArgs,
		// errors and usage are surfaced by the caller (MustParseOrExit); cobra
		// must not also print them or every parse error renders twice.
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
			return validateLogLevel(flags)
		},
		RunE: func(_ *cobra.Command, _ []string) error {
			return validateExitFlags(flags)
		},
	}

	root.PersistentFlags().StringVar(&flags.ConfigPath, "config", "", "path to a config file or directory (disables config hierarchy lookup)")
	root.PersistentFlags().StringVar(&flags.LogLevel, "log-level", "", "log level (debug|info|warn|error); overrides logging.level from config")
	root.PersistentFlags().StringVar(&flags.VaultAddr, "vault-addr", "", "Vault address; overrides vault.address from config")
	root.PersistentFlags().StringVar(&flags.VaultToken, "vault-token", "", "Vault token — a literal value is visible via ps / /proc/<pid>/cmdline; prefer ${env:VAR} or ${file:/path}; overrides vault.token from config")

	root.Flags().BoolVarP(&flags.ShowVersion, "version", "v", false, "print version and exit")
	root.Flags().BoolVar(&flags.ShowLogs, "logs", false, "open the log file in $PAGER and exit")
	root.Flags().BoolVar(&flags.ShowLogsDir, "logs-dir", false, "print the log directory and exit")

	root.AddCommand(newConnectCmd(flags))

	// cobra runs its help func instead of RunE on --help/-h; record it so
	// Parse can signal a clean exit rather than launching the TUI.
	defaultHelp := root.HelpFunc()
	root.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		*helpRequested = true
		defaultHelp(cmd, args)
	})

	return root
}

func newConnectCmd(flags *Flags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "connect [name]",
		Short: "Connect to a cluster: a clusters.yaml entry by name, or an ad-hoc cluster via --brokers",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			name := ""
			if len(args) == 1 {
				name = args[0]
			}
			return applyConnect(flags, name)
		},
	}

	cmd.Flags().StringSliceVar(&flags.Inline.Brokers, "brokers", nil, "comma-separated broker addresses; creates an inline cluster named <random>-cli for this session")
	cmd.Flags().StringVar(&flags.Inline.Color, "color", "", "cluster color (red|yellow|green|gray|white) for the inline cluster")
	cmd.Flags().BoolVar(&flags.Inline.ReadOnly, "read-only", false, "mark the inline cluster as read-only")

	cmd.Flags().BoolVar(&flags.Inline.TLSEnabled, "tls", false, "enable TLS for the inline cluster")
	cmd.Flags().StringVar(&flags.Inline.TLSCAFile, "tls-ca", "", "path to a TLS CA certificate (requires --tls)")
	cmd.Flags().StringVar(&flags.Inline.TLSCertFile, "tls-cert", "", "path to a TLS client certificate (requires --tls)")
	cmd.Flags().StringVar(&flags.Inline.TLSKeyFile, "tls-key", "", "path to a TLS client key (requires --tls)")
	cmd.Flags().BoolVar(&flags.Inline.TLSSkipVerify, "tls-skip-verify", false, "skip TLS verification (requires --tls)")

	cmd.Flags().StringVar(&flags.Inline.SASLMechanism, "sasl-mechanism", "", "SASL mechanism (PLAIN|SCRAM-SHA-256|SCRAM-SHA-512)")
	cmd.Flags().StringVar(&flags.Inline.SASLUsername, "sasl-username", "", "SASL username")
	cmd.Flags().StringVar(&flags.Inline.SASLPassword, "sasl-password", "", "SASL password — a literal value is visible via ps / /proc/<pid>/cmdline; prefer ${env:VAR}, ${file:/path}, or ${vault:path#key}")

	return cmd
}

// applyConnect resolves the connect target: either a named clusters.yaml
// cluster or an ad-hoc inline cluster (--brokers). The two are mutually
// exclusive, and the inline-attribute flags (--tls/--sasl/--color/--read-only)
// are validated to require --brokers.
func applyConnect(flags *Flags, name string) error {
	hasName := name != ""
	hasBrokers := flags.Inline.HasInlineCluster()

	if hasName && hasBrokers {
		return &ParseError{Msg: "connect takes either a cluster name or --brokers, not both"}
	}

	// attribute checks run before the missing-target check so `connect --tls`
	// (forgot --brokers) and `connect prod --tls` (attrs invalid for a named
	// cluster) both report the specific flag rather than a generic message.
	if err := validateTLS(&flags.Inline); err != nil {
		return err
	}
	if err := validateSASL(&flags.Inline); err != nil {
		return err
	}
	if err := validateCluster(&flags.Inline); err != nil {
		return err
	}

	if !hasName && !hasBrokers {
		return &ParseError{Msg: "connect requires a cluster name or --brokers"}
	}

	if hasBrokers {
		flags.Inline.Name = generateInlineName()
	} else {
		flags.ClusterName = name
	}
	return nil
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
	case errors.Is(err, ErrExitEarly):
		return nil, false
	case err != nil:
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(2)
	}
	return f, true
}
