package kafka

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aleksey925/kafka-tui/internal/config"
)

func TestDetectProtocol(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		cluster  config.Cluster
		expected Protocol
	}{
		{"plaintext", config.Cluster{}, ProtocolPlaintext},
		{"ssl", config.Cluster{TLS: &config.TLSConfig{}}, ProtocolSSL},
		{"sasl_plaintext", config.Cluster{SASL: &config.SASLConfig{Mechanism: "PLAIN"}}, ProtocolSASLPlaintext},
		{"sasl_ssl", config.Cluster{TLS: &config.TLSConfig{}, SASL: &config.SASLConfig{Mechanism: "PLAIN"}}, ProtocolSASLSSL},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, DetectProtocol(tc.cluster))
		})
	}
}

func TestIsInsecureTLS(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		c    config.Cluster
		want bool
	}{
		{"no tls", config.Cluster{}, false},
		{"tls with verify", config.Cluster{TLS: &config.TLSConfig{}}, false},
		{"tls skip_verify", config.Cluster{TLS: &config.TLSConfig{SkipVerify: true}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, IsInsecureTLS(tc.c))
		})
	}
}

func TestBuildClientOptions__noBrokers__error(t *testing.T) {
	t.Parallel()

	_, _, err := BuildClientOptions(config.Cluster{Name: "c"}, DialOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no brokers")
}

func TestBuildClientOptions__plaintext(t *testing.T) {
	t.Parallel()

	opts, proto, err := BuildClientOptions(config.Cluster{
		Name:    "c",
		Brokers: []string{"localhost:9092"},
	}, DialOptions{ClientID: "my-id"})

	require.NoError(t, err)
	assert.Equal(t, ProtocolPlaintext, proto)
	assert.Len(t, opts, 2) // SeedBrokers + ClientID
}

func TestBuildClientOptions__tlsAndSasl__sasl_ssl(t *testing.T) {
	t.Parallel()

	opts, proto, err := BuildClientOptions(config.Cluster{
		Name:    "c",
		Brokers: []string{"localhost:9092"},
		TLS:     &config.TLSConfig{},
		SASL:    &config.SASLConfig{Mechanism: "PLAIN", Username: "u", Password: "p"},
	}, DialOptions{})

	require.NoError(t, err)
	assert.Equal(t, ProtocolSASLSSL, proto)
	assert.Len(t, opts, 4) // SeedBrokers + ClientID + DialTLSConfig + SASL
}

func TestBuildClientOptions__extraOpts__appended(t *testing.T) {
	t.Parallel()

	extra := kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelNone, nil))

	opts, _, err := BuildClientOptions(config.Cluster{
		Name:    "c",
		Brokers: []string{"localhost:9092"},
	}, DialOptions{ExtraOpts: []kgo.Opt{extra}})

	require.NoError(t, err)
	assert.Len(t, opts, 3) // SeedBrokers + ClientID + extra logger
}

func TestBuildClientOptions__sasl(t *testing.T) {
	t.Parallel()

	// buildSASLMechanism is strict — input must be the canonical upper-case
	// form. The config loader normalizes YAML before this point, so a
	// bogus mechanism would have been quarantined at load.
	cases := []struct {
		mechanism string
		wantErr   bool
	}{
		{"PLAIN", false},
		{"SCRAM-SHA-256", false},
		{"SCRAM-SHA-512", false},
		// non-canonical (would normally be rejected/normalized upstream)
		{"plain", true},
		{"unknown", true},
		{"", true},
	}
	for _, tc := range cases {
		t.Run(tc.mechanism, func(t *testing.T) {
			t.Parallel()
			_, _, err := BuildClientOptions(config.Cluster{
				Name:    "c",
				Brokers: []string{"localhost:9092"},
				SASL:    &config.SASLConfig{Mechanism: tc.mechanism, Username: "u", Password: "p"},
			}, DialOptions{})
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestBuildClientOptions__sasl__missingUser__error(t *testing.T) {
	t.Parallel()

	_, _, err := BuildClientOptions(config.Cluster{
		Name:    "c",
		Brokers: []string{"localhost:9092"},
		SASL:    &config.SASLConfig{Mechanism: "PLAIN", Username: "", Password: "p"},
	}, DialOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "username is empty")
}

func TestBuildClientOptions__tls__inlineCA(t *testing.T) {
	t.Parallel()

	caPEM := generateSelfSignedCA(t)

	opts, proto, err := BuildClientOptions(config.Cluster{
		Name:    "c",
		Brokers: []string{"localhost:9092"},
		TLS:     &config.TLSConfig{CA: string(caPEM)},
	}, DialOptions{})
	require.NoError(t, err)
	assert.Equal(t, ProtocolSSL, proto)
	assert.NotEmpty(t, opts)
}

func TestBuildClientOptions__tls__caFromFile(t *testing.T) {
	t.Parallel()

	caPEM := generateSelfSignedCA(t)
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caFile, caPEM, 0o600))

	_, _, err := BuildClientOptions(config.Cluster{
		Name:    "c",
		Brokers: []string{"localhost:9092"},
		TLS:     &config.TLSConfig{CAFile: caFile},
	}, DialOptions{})
	require.NoError(t, err)
}

func TestBuildClientOptions__tls__missingFile__error(t *testing.T) {
	t.Parallel()

	_, _, err := BuildClientOptions(config.Cluster{
		Name:    "c",
		Brokers: []string{"localhost:9092"},
		TLS:     &config.TLSConfig{CAFile: "/no/such/path.pem"},
	}, DialOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ca: read")
}

func TestBuildClientOptions__tls__certWithoutKey__error(t *testing.T) {
	t.Parallel()

	caPEM := generateSelfSignedCA(t)

	_, _, err := BuildClientOptions(config.Cluster{
		Name:    "c",
		Brokers: []string{"localhost:9092"},
		TLS: &config.TLSConfig{
			CA:   string(caPEM),
			Cert: string(caPEM),
		},
	}, DialOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cert and key")
}

func TestBuildClientOptions__tls__invalidCA__error(t *testing.T) {
	t.Parallel()

	_, _, err := BuildClientOptions(config.Cluster{
		Name:    "c",
		Brokers: []string{"localhost:9092"},
		TLS:     &config.TLSConfig{CA: "not a real PEM"},
	}, DialOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no PEM certificates parsed")
}

func TestDial_Ping__kfake__ok(t *testing.T) {
	t.Parallel()

	cluster := startKfake(t)
	c, err := Dial(cluster, DialOptions{})
	require.NoError(t, err)
	t.Cleanup(c.Close)

	err = c.Ping(context.Background(), 5*time.Second)
	require.NoError(t, err)
	assert.Equal(t, ProtocolPlaintext, c.Protocol())
	assert.Equal(t, cluster.Name, c.Cluster().Name)
}

func TestDial__noBrokers__error(t *testing.T) {
	t.Parallel()

	_, err := Dial(config.Cluster{Name: "c"}, DialOptions{})
	require.Error(t, err)
}

func TestPing__unreachable__error(t *testing.T) {
	t.Parallel()

	c, err := Dial(config.Cluster{
		Name:    "unreachable",
		Brokers: []string{"127.0.0.1:1"}, // port 1 unlikely to accept Kafka
	}, DialOptions{})
	require.NoError(t, err)
	t.Cleanup(c.Close)

	err = c.Ping(context.Background(), 500*time.Millisecond)
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "ping"))
}

// startKfake spins up an in-process fake Kafka cluster and returns a
// config.Cluster whose Brokers list points at it. The cluster is closed when
// the test ends.
func startKfake(t *testing.T) config.Cluster {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)
	return config.Cluster{
		Name:    "kfake",
		Brokers: cluster.ListenAddrs(),
	}
}

// generateSelfSignedCA returns a PEM-encoded self-signed CA certificate
// suitable for the TLS option-assembly tests. We don't actually open any TLS
// connections, so the cert just needs to parse.
func generateSelfSignedCA(t *testing.T) []byte {
	t.Helper()
	// Reuse the standard library's self-signed cert helper so we avoid
	// pulling in extra deps. We use a minimal hand-rolled cert here for
	// reproducibility.
	const pemTemplate = `-----BEGIN CERTIFICATE-----
MIIBhTCCASugAwIBAgIQIRi6zePL6mKjOipn+dNuaTAKBggqhkjOPQQDAjASMRAw
DgYDVQQKEwdBY21lIENvMB4XDTE3MTAyMDE5NDMwNloXDTE4MTAyMDE5NDMwNlow
EjEQMA4GA1UEChMHQWNtZSBDbzBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABD0d
7VNhbWvZLWPuj/RtHFjvtJBEwOkhbN/BnnE8rnZR8+sbwnc/KhCk3FhnpHZnQz7B
5aETbbIgmuvewdjvSBSjYzBhMA4GA1UdDwEB/wQEAwICpDATBgNVHSUEDDAKBggr
BgEFBQcDATAPBgNVHRMBAf8EBTADAQH/MCkGA1UdEQQiMCCCDmxvY2FsaG9zdDo1
NDUzgg4xMjcuMC4wLjE6NTQ1MzAKBggqhkjOPQQDAgNIADBFAiEA2zpJEPQyz6/l
Wf86aX6PepsntZv2GYlA5UpabfT2EZICICpJ5h/iI+i341gBmLiAFQOyTDT+/wQc
6MF9+Yw1Yy0t
-----END CERTIFICATE-----
`
	block, _ := pem.Decode([]byte(pemTemplate))
	require.NotNil(t, block)
	_, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	return []byte(pemTemplate)
}
