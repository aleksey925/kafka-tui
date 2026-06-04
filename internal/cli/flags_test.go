package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse__noArgs__returnsEmptyFlags(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	f, err := Parse(nil, &out, &errOut)

	// assert
	require.NoError(t, err)
	assert.Equal(t, &Flags{}, f)
}

func TestParse__versionFlag__setsShowVersion(t *testing.T) {
	for _, flag := range []string{"--version", "-v"} {
		t.Run(flag, func(t *testing.T) {
			// arrange
			var out, errOut bytes.Buffer

			// act
			f, err := Parse([]string{flag}, &out, &errOut)

			// assert
			require.NoError(t, err)
			assert.True(t, f.ShowVersion)
		})
	}
}

func TestParse__logsAndLogsDir__areMutuallyExclusive(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	_, err := Parse([]string{"--logs", "--logs-dir"}, &out, &errOut)

	// assert
	var pe *ParseError
	require.ErrorAs(t, err, &pe)
	assert.Contains(t, pe.Msg, "mutually exclusive")
}

func TestParse__connectName__setsClusterName(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	f, err := Parse([]string{"connect", "prod"}, &out, &errOut)

	// assert
	require.NoError(t, err)
	assert.Equal(t, "prod", f.ClusterName)
	assert.False(t, f.Inline.HasInlineCluster())
}

func TestParse__connectNameAndBrokers__fails(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	_, err := Parse([]string{"connect", "prod", "--brokers", "x:9092"}, &out, &errOut)

	// assert
	var pe *ParseError
	require.ErrorAs(t, err, &pe)
	assert.Contains(t, pe.Msg, "either a cluster name or --brokers, not both")
}

func TestParse__connectWithoutTarget__fails(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	_, err := Parse([]string{"connect"}, &out, &errOut)

	// assert
	var pe *ParseError
	require.ErrorAs(t, err, &pe)
	assert.Contains(t, pe.Msg, "requires a cluster name or --brokers")
}

func TestParse__globalFlagWorksBeforeAndAfterConnect(t *testing.T) {
	// cobra lets a persistent flag sit on either side of the subcommand; this
	// is the ergonomics we adopted cobra for.
	for _, args := range [][]string{
		{"--log-level", "debug", "connect", "prod"},
		{"connect", "prod", "--log-level", "debug"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			// arrange
			var out, errOut bytes.Buffer

			// act
			f, err := Parse(args, &out, &errOut)

			// assert
			require.NoError(t, err)
			assert.Equal(t, "prod", f.ClusterName)
			assert.Equal(t, "debug", f.LogLevel)
		})
	}
}

func TestParse__tlsCAWithoutTLS__fails(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	_, err := Parse([]string{"connect", "--tls-ca", "/etc/ca.pem", "--brokers", "localhost:9092"}, &out, &errOut)

	// assert
	var pe *ParseError
	require.ErrorAs(t, err, &pe)
	assert.Contains(t, pe.Msg, "--tls-ca requires --tls")
}

func TestParse__tlsSkipVerifyWithoutTLS__fails(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	_, err := Parse([]string{"connect", "--tls-skip-verify", "--brokers", "localhost:9092"}, &out, &errOut)

	// assert
	var pe *ParseError
	require.ErrorAs(t, err, &pe)
	assert.Contains(t, pe.Msg, "--tls-skip-verify requires --tls")
}

func TestParse__tlsCertWithoutKey__fails(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	_, err := Parse([]string{"connect", "--tls", "--tls-cert", "/c.pem", "--brokers", "localhost:9092"}, &out, &errOut)

	// assert
	var pe *ParseError
	require.ErrorAs(t, err, &pe)
	assert.Contains(t, pe.Msg, "--tls-cert and --tls-key must be specified together")
}

func TestParse__tlsRequiresBrokers__fails(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	_, err := Parse([]string{"connect", "--tls"}, &out, &errOut)

	// assert
	var pe *ParseError
	require.ErrorAs(t, err, &pe)
	assert.Contains(t, pe.Msg, "--tls requires --brokers")
}

func TestParse__colorRequiresBrokers__fails(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	_, err := Parse([]string{"connect", "--color", "red"}, &out, &errOut)

	// assert
	var pe *ParseError
	require.ErrorAs(t, err, &pe)
	assert.Contains(t, pe.Msg, "--color requires --brokers")
}

func TestParse__readOnlyRequiresBrokers__fails(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	_, err := Parse([]string{"connect", "--read-only"}, &out, &errOut)

	// assert
	var pe *ParseError
	require.ErrorAs(t, err, &pe)
	assert.Contains(t, pe.Msg, "--read-only requires --brokers")
}

func TestParse__partialSASL__fails(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	_, err := Parse([]string{"connect", "--brokers", "localhost:9092", "--sasl-username", "u"}, &out, &errOut)

	// assert
	var pe *ParseError
	require.ErrorAs(t, err, &pe)
	assert.Contains(t, pe.Msg, "must be specified together")
}

func TestParse__validInlineCluster__populatesAllFields(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer
	args := []string{
		"connect",
		"--brokers", "broker-1:9092,broker-2:9092",
		"--color", "red",
		"--read-only",
		"--tls",
		"--tls-ca", "/etc/ca.pem",
		"--tls-cert", "/etc/c.pem",
		"--tls-key", "/etc/k.pem",
		"--sasl-mechanism", "PLAIN",
		"--sasl-username", "u",
		"--sasl-password", "p",
	}

	// act
	f, err := Parse(args, &out, &errOut)

	// assert — the inline name is auto-generated with a "-cli" suffix; no
	// cluster name is set when --brokers defines an ad-hoc cluster.
	require.NoError(t, err)
	assert.Empty(t, f.ClusterName)
	assert.True(t, strings.HasSuffix(f.Inline.Name, "-cli"), "inline name must end with -cli, got %q", f.Inline.Name)
	assert.Equal(t, []string{"broker-1:9092", "broker-2:9092"}, f.Inline.Brokers)
	assert.Equal(t, "red", f.Inline.Color)
	assert.True(t, f.Inline.ReadOnly)
	assert.True(t, f.Inline.TLSEnabled)
	assert.Equal(t, "PLAIN", f.Inline.SASLMechanism)
}

func TestParse__inlineClusterGetsAutoName(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	f, err := Parse([]string{"connect", "--brokers", "localhost:9092"}, &out, &errOut)

	// assert
	require.NoError(t, err)
	assert.True(t, strings.HasSuffix(f.Inline.Name, "-cli"), "inline name must end with -cli, got %q", f.Inline.Name)
	assert.Greater(t, len(f.Inline.Name), len("-cli"), "inline name must have a random prefix before -cli")
}

func TestParse__inlineClusterNameIsRandomPerCall(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act — two parses must produce different inline names so collisions
	// across processes (or with YAML clusters) stay astronomically rare.
	f1, err := Parse([]string{"connect", "--brokers", "x:9092"}, &out, &errOut)
	require.NoError(t, err)
	f2, err := Parse([]string{"connect", "--brokers", "x:9092"}, &out, &errOut)
	require.NoError(t, err)

	// assert
	assert.NotEqual(t, f1.Inline.Name, f2.Inline.Name)
}

func TestParse__configFlag__setsConfigPath(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	f, err := Parse([]string{"--config", "/etc/kafka-tui.yaml"}, &out, &errOut)

	// assert
	require.NoError(t, err)
	assert.Equal(t, "/etc/kafka-tui.yaml", f.ConfigPath)
}

func TestParse__invalidColor__fails(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	_, err := Parse([]string{"connect", "--brokers", "localhost:9092", "--color", "magenta"}, &out, &errOut)

	// assert
	var pe *ParseError
	require.ErrorAs(t, err, &pe)
	assert.Contains(t, pe.Msg, "invalid --color")
}

func TestParse__validColor__accepted(t *testing.T) {
	// arrange
	colors := []string{"red", "yellow", "green", "gray", "white"}

	for _, color := range colors {
		t.Run(color, func(t *testing.T) {
			// arrange
			var out, errOut bytes.Buffer

			// act
			f, err := Parse([]string{"connect", "--brokers", "localhost:9092", "--color", color}, &out, &errOut)

			// assert
			require.NoError(t, err)
			assert.Equal(t, color, f.Inline.Color)
		})
	}
}

func TestParse__colorIsNormalized(t *testing.T) {
	cases := []struct{ input, want string }{
		{"RED", "red"},
		{" Red ", "red"},
		{"YELLOW", "yellow"},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			// arrange
			var out, errOut bytes.Buffer

			// act
			f, err := Parse([]string{"connect", "--brokers", "localhost:9092", "--color", tc.input}, &out, &errOut)

			// assert
			require.NoError(t, err)
			assert.Equal(t, tc.want, f.Inline.Color)
		})
	}
}

func TestParse__emptyBroker__fails(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	_, err := Parse([]string{"connect", "--brokers", "localhost:9092,"}, &out, &errOut)

	// assert
	var pe *ParseError
	require.ErrorAs(t, err, &pe)
	assert.Contains(t, pe.Msg, "empty entry")
}

func TestParse__helpFlag__returnsErrExitEarly(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	_, err := Parse([]string{"--help"}, &out, &errOut)

	// assert
	assert.ErrorIs(t, err, ErrExitEarly)
}

func TestParse__unknownCommand__fails(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	_, err := Parse([]string{"something"}, &out, &errOut)

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown command")
	assert.Contains(t, err.Error(), "something")
}

func TestParse__completionCommand__exitsEarlyWithoutLaunching(t *testing.T) {
	// the shell-completion command prints its script and must signal a clean
	// exit, not fall through to launching the TUI.
	var out, errOut bytes.Buffer

	// act
	_, err := Parse([]string{"completion", "bash"}, &out, &errOut)

	// assert
	require.ErrorIs(t, err, ErrExitEarly)
	assert.Contains(t, out.String(), "kafka-tui")
}

func TestParse__unknownFlag__fails(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	_, err := Parse([]string{"--nonsense"}, &out, &errOut)

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown flag")
	assert.Contains(t, err.Error(), "--nonsense")
}

func TestCLICluster_HasInlineCluster(t *testing.T) {
	// arrange
	tests := []struct {
		name string
		c    CLICluster
		want bool
	}{
		{"empty", CLICluster{}, false},
		{"with brokers", CLICluster{Brokers: []string{"a:9092"}}, true},
		{"only-name", CLICluster{Name: "x"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// act
			got := tt.c.HasInlineCluster()

			// assert
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParse__vaultFlags__capturedOnFlags(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	f, err := Parse([]string{
		"--vault-addr", "https://vault.example.com",
		"--vault-token", "hvs.xxx",
	}, &out, &errOut)

	// assert
	require.NoError(t, err)
	assert.Equal(t, "https://vault.example.com", f.VaultAddr)
	assert.Equal(t, "hvs.xxx", f.VaultToken)
}

func TestParse__logLevel__capturedOnFlags(t *testing.T) {
	for _, lvl := range []string{"debug", "info", "warn", "error"} {
		t.Run(lvl, func(t *testing.T) {
			// arrange
			var out, errOut bytes.Buffer

			// act
			f, err := Parse([]string{"--log-level", lvl}, &out, &errOut)

			// assert
			require.NoError(t, err)
			assert.Equal(t, lvl, f.LogLevel)
		})
	}
}

func TestParse__logLevel__invalidValue__fails(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	_, err := Parse([]string{"--log-level", "trace"}, &out, &errOut)

	// assert
	var pe *ParseError
	require.ErrorAs(t, err, &pe)
	assert.Contains(t, pe.Msg, "invalid --log-level")
}

func TestParse__logLevel__isNormalized(t *testing.T) {
	cases := []struct{ input, want string }{
		{"DEBUG", "debug"},
		{" Info ", "info"},
		{"WARNING", ""},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			// arrange
			var out, errOut bytes.Buffer

			// act
			f, err := Parse([]string{"--log-level", tc.input}, &out, &errOut)

			// assert
			if tc.want == "" {
				var pe *ParseError
				require.ErrorAs(t, err, &pe)
				assert.Contains(t, pe.Msg, "invalid --log-level")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, f.LogLevel)
		})
	}
}

func TestParse__saslMechanism__isNormalized(t *testing.T) {
	// The inline cluster runs through the loader's per-cluster pipeline, where
	// a bad enum would only quarantine it. CLI input must instead hard-fail at
	// parse (CLAUDE.md § Config-value normalization), so the validator
	// case-folds the mechanism here and rejects unknown values immediately.
	cases := []struct{ input, want string }{
		{"plain", "PLAIN"},
		{"PLAIN", "PLAIN"},
		{" scram-sha-256 ", "SCRAM-SHA-256"},
		{"Scram-Sha-512", "SCRAM-SHA-512"},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			// arrange
			var out, errOut bytes.Buffer
			args := []string{
				"connect",
				"--brokers", "x:9092",
				"--sasl-mechanism", tc.input,
				"--sasl-username", "u",
				"--sasl-password", "p",
			}

			// act
			f, err := Parse(args, &out, &errOut)

			// assert
			require.NoError(t, err)
			assert.Equal(t, tc.want, f.Inline.SASLMechanism)
		})
	}
}

func TestParse__saslMechanism__invalidValue__fails(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer
	args := []string{
		"connect",
		"--brokers", "x:9092",
		"--sasl-mechanism", "kerberos",
		"--sasl-username", "u",
		"--sasl-password", "p",
	}

	// act
	_, err := Parse(args, &out, &errOut)

	// assert
	var pe *ParseError
	require.ErrorAs(t, err, &pe)
	assert.Contains(t, pe.Msg, "invalid --sasl-mechanism")
	assert.Contains(t, pe.Msg, "kerberos")
}

func TestCredentialExposureWarnings(t *testing.T) {
	cases := []struct {
		name     string
		flags    *Flags
		wantHits []string // substrings every returned warning collectively must cover
		wantLen  int
	}{
		{
			name:    "nil flags",
			flags:   nil,
			wantLen: 0,
		},
		{
			name:    "no credentials set",
			flags:   &Flags{},
			wantLen: 0,
		},
		{
			name: "literal sasl-password warns",
			flags: &Flags{
				Inline: CLICluster{SASLPassword: "hunter2"},
			},
			wantHits: []string{"--sasl-password"},
			wantLen:  1,
		},
		{
			name: "env placeholder is silent",
			flags: &Flags{
				Inline: CLICluster{SASLPassword: "${env:KAFKA_PASS}"},
			},
			wantLen: 0,
		},
		{
			name: "file placeholder is silent",
			flags: &Flags{
				Inline: CLICluster{SASLPassword: "${file:/run/secrets/p}"},
			},
			wantLen: 0,
		},
		{
			name: "vault placeholder is silent",
			flags: &Flags{
				Inline: CLICluster{SASLPassword: "${vault:secret/kafka#pass}"},
			},
			wantLen: 0,
		},
		{
			name: "whitespace-only is silent",
			flags: &Flags{
				Inline:     CLICluster{SASLPassword: "   "},
				VaultToken: "\t",
			},
			wantLen: 0,
		},
		{
			name: "literal vault-token warns",
			flags: &Flags{
				VaultToken: "s.deadbeef",
			},
			wantHits: []string{"--vault-token"},
			wantLen:  1,
		},
		{
			name: "both literals warn independently",
			flags: &Flags{
				Inline:     CLICluster{SASLPassword: "hunter2"},
				VaultToken: "s.deadbeef",
			},
			wantHits: []string{"--sasl-password", "--vault-token"},
			wantLen:  2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// act
			warns := CredentialExposureWarnings(tc.flags)

			// assert
			assert.Len(t, warns, tc.wantLen)
			joined := strings.Join(warns, "\n")
			for _, sub := range tc.wantHits {
				assert.Contains(t, joined, sub)
			}
		})
	}
}

func TestParse__connectHelp__mentionsCredentialPlaceholders(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act — the credential flags live on the connect subcommand.
	_, err := Parse([]string{"connect", "--help"}, &out, &errOut)

	// assert: --help is a clean early exit; the usage mentions placeholders.
	require.ErrorIs(t, err, ErrExitEarly)
	usage := out.String() + errOut.String()
	assert.Contains(t, usage, "--sasl-password")
	assert.Contains(t, usage, "${env:")
}
