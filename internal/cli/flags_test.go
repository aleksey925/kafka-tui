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

func TestParse__tlsCAWithoutTLS__fails(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	_, err := Parse([]string{"--tls-ca", "/etc/ca.pem", "--brokers", "localhost:9092"}, &out, &errOut)

	// assert
	var pe *ParseError
	require.ErrorAs(t, err, &pe)
	assert.Contains(t, pe.Msg, "--tls-ca requires --tls")
}

func TestParse__tlsSkipVerifyWithoutTLS__fails(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	_, err := Parse([]string{"--tls-skip-verify", "--brokers", "localhost:9092"}, &out, &errOut)

	// assert
	var pe *ParseError
	require.ErrorAs(t, err, &pe)
	assert.Contains(t, pe.Msg, "--tls-skip-verify requires --tls")
}

func TestParse__tlsCertWithoutKey__fails(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	_, err := Parse([]string{"--tls", "--tls-cert", "/c.pem", "--brokers", "localhost:9092"}, &out, &errOut)

	// assert
	var pe *ParseError
	require.ErrorAs(t, err, &pe)
	assert.Contains(t, pe.Msg, "--tls-cert and --tls-key must be specified together")
}

func TestParse__tlsRequiresBrokers__fails(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	_, err := Parse([]string{"--tls"}, &out, &errOut)

	// assert
	var pe *ParseError
	require.ErrorAs(t, err, &pe)
	assert.Contains(t, pe.Msg, "--tls requires --brokers")
}

func TestParse__partialSASL__fails(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	_, err := Parse([]string{"--brokers", "localhost:9092", "--sasl-username", "u"}, &out, &errOut)

	// assert
	var pe *ParseError
	require.ErrorAs(t, err, &pe)
	assert.Contains(t, pe.Msg, "must be specified together")
}

func TestParse__validInlineCluster__populatesAllFields(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer
	args := []string{
		"--brokers", "broker-1:9092,broker-2:9092",
		"--cluster", "prod",
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

	// assert — --cluster is a selector and is NOT used to name the inline
	// cluster; the inline name is auto-generated with a "-cli" suffix.
	require.NoError(t, err)
	assert.Equal(t, "prod", f.ClusterName)
	assert.True(t, strings.HasSuffix(f.Inline.Name, "-cli"), "inline name must end with -cli, got %q", f.Inline.Name)
	assert.NotEqual(t, "prod", f.Inline.Name, "--cluster value must not be used as the inline name")
	assert.Equal(t, []string{"broker-1:9092", "broker-2:9092"}, f.Inline.Brokers)
	assert.Equal(t, "red", f.Inline.Color)
	assert.True(t, f.Inline.ReadOnly)
	assert.True(t, f.Inline.TLSEnabled)
	assert.Equal(t, "PLAIN", f.Inline.SASLMechanism)
}

func TestParse__inlineClusterWithoutName__getsAutoName(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	f, err := Parse([]string{"--brokers", "localhost:9092"}, &out, &errOut)

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
	f1, err := Parse([]string{"--brokers", "x:9092"}, &out, &errOut)
	require.NoError(t, err)
	f2, err := Parse([]string{"--brokers", "x:9092"}, &out, &errOut)
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
	_, err := Parse([]string{"--brokers", "localhost:9092", "--color", "magenta"}, &out, &errOut)

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
			f, err := Parse([]string{"--brokers", "localhost:9092", "--color", color}, &out, &errOut)

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
			f, err := Parse([]string{"--brokers", "localhost:9092", "--color", tc.input}, &out, &errOut)

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
	_, err := Parse([]string{"--brokers", "localhost:9092,"}, &out, &errOut)

	// assert
	var pe *ParseError
	require.ErrorAs(t, err, &pe)
	assert.Contains(t, pe.Msg, "empty entry")
}

func TestParse__helpFlag__returnsErrHelpRequested(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	_, err := Parse([]string{"--help"}, &out, &errOut)

	// assert
	assert.ErrorIs(t, err, ErrHelpRequested)
}

func TestParse__positionalArgs__fails(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	_, err := Parse([]string{"something"}, &out, &errOut)

	// assert
	var pe *ParseError
	require.ErrorAs(t, err, &pe)
	assert.Contains(t, pe.Msg, "unexpected argument")
	assert.Contains(t, pe.Msg, "something")
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
	// Inline clusters skip the per-cluster pipeline (loadCluster +
	// normalizeClusterHard run only over YAML-loaded clusters). The CLI
	// validator must therefore upper-case the mechanism itself, otherwise
	// `--sasl-mechanism plain` would silently reach the strict
	// buildSASLMechanism and fail at dial time — violating the "CLI
	// always hard-fails at parse" rule from CLAUDE.md.
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
