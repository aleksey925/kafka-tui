package cli

import (
	"bytes"
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

	// assert
	require.NoError(t, err)
	assert.Equal(t, &Flags{
		ClusterName: "prod",
		Inline: CLICluster{
			Name:          "prod",
			Brokers:       []string{"broker-1:9092", "broker-2:9092"},
			Color:         "red",
			ReadOnly:      true,
			TLSEnabled:    true,
			TLSCAFile:     "/etc/ca.pem",
			TLSCertFile:   "/etc/c.pem",
			TLSKeyFile:    "/etc/k.pem",
			SASLMechanism: "PLAIN",
			SASLUsername:  "u",
			SASLPassword:  "p",
		},
	}, f)
}

func TestParse__inlineClusterWithoutName__defaultsToCli(t *testing.T) {
	// arrange
	var out, errOut bytes.Buffer

	// act
	f, err := Parse([]string{"--brokers", "localhost:9092"}, &out, &errOut)

	// assert
	require.NoError(t, err)
	assert.Equal(t, "cli", f.Inline.Name)
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
