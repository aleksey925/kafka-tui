// Package kafka wraps franz-go with SASL/TLS auto-detection and the admin
// helpers the TUI needs.
package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/aleksey925/kafka-tui/internal/config"
)

// Protocol is the security protocol implied by a cluster's TLS/SASL config.
type Protocol string

const (
	ProtocolPlaintext     Protocol = "PLAINTEXT"
	ProtocolSSL           Protocol = "SSL"
	ProtocolSASLPlaintext Protocol = "SASL_PLAINTEXT"
	ProtocolSASLSSL       Protocol = "SASL_SSL"

	DefaultClientID    = "kafka-tui"
	DefaultPingTimeout = 5 * time.Second
)

// SASL mechanism names accepted in cluster.sasl.mechanism (case-insensitive).
const (
	saslMechanismPlain    = "PLAIN"
	saslMechanismScram256 = "SCRAM-SHA-256"
	saslMechanismScram512 = "SCRAM-SHA-512"
)

// DialOptions tweak Dial behavior. The zero value is fine for production.
type DialOptions struct {
	ClientID  string
	ExtraOpts []kgo.Opt
}

// DetectProtocol returns the security protocol implied by the cluster's
// TLS / SASL configuration. An empty TLS section (`tls: {}`) is treated as
// TLS-with-system-CAs.
func DetectProtocol(c config.Cluster) Protocol {
	hasTLS := c.TLS != nil
	hasSASL := c.SASL != nil
	switch {
	case hasTLS && hasSASL:
		return ProtocolSASLSSL
	case hasTLS:
		return ProtocolSSL
	case hasSASL:
		return ProtocolSASLPlaintext
	default:
		return ProtocolPlaintext
	}
}

// BuildClientOptions assembles the kgo.Opt slice for a cluster without
// opening any connections.
func BuildClientOptions(c config.Cluster, dopts DialOptions) ([]kgo.Opt, Protocol, error) {
	if len(c.Brokers) == 0 {
		return nil, "", errors.New("kafka: no brokers")
	}

	clientID := dopts.ClientID
	if clientID == "" {
		clientID = DefaultClientID
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ClientID(clientID),
	}

	if c.TLS != nil {
		tlsCfg, err := buildTLSConfig(c.TLS)
		if err != nil {
			return nil, "", fmt.Errorf("kafka: tls: %w", err)
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	if c.SASL != nil {
		mech, err := buildSASLMechanism(c.SASL)
		if err != nil {
			return nil, "", fmt.Errorf("kafka: sasl: %w", err)
		}
		opts = append(opts, kgo.SASL(mech))
	}

	opts = append(opts, dopts.ExtraOpts...)
	return opts, DetectProtocol(c), nil
}

// buildTLSConfig converts a [config.TLSConfig] into a *tls.Config. An empty
// TLS section returns a config that uses the system root CAs.
func buildTLSConfig(t *config.TLSConfig) (*tls.Config, error) {
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: t.SkipVerify, //nolint:gosec // honoring user opt-in
	}

	caBytes, err := readMaterial("ca", t.CA, t.CAFile)
	if err != nil {
		return nil, err
	}
	if len(caBytes) > 0 {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caBytes) {
			return nil, errors.New("ca: no PEM certificates parsed")
		}
		cfg.RootCAs = pool
	}

	certBytes, err := readMaterial("cert", t.Cert, t.CertFile)
	if err != nil {
		return nil, err
	}
	keyBytes, err := readMaterial("key", t.Key, t.KeyFile)
	if err != nil {
		return nil, err
	}
	switch {
	case len(certBytes) == 0 && len(keyBytes) == 0:
		// no client cert, fine
	case len(certBytes) == 0 || len(keyBytes) == 0:
		return nil, errors.New("cert and key must both be set or both be empty")
	default:
		pair, err := tls.X509KeyPair(certBytes, keyBytes)
		if err != nil {
			return nil, fmt.Errorf("load client keypair: %w", err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}

	return cfg, nil
}

func readMaterial(label, inline, path string) ([]byte, error) {
	if inline != "" {
		return []byte(inline), nil
	}
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path) //nolint:gosec // user-provided cluster paths
	if err != nil {
		return nil, fmt.Errorf("%s: read %s: %w", label, path, err)
	}
	return data, nil
}

// buildSASLMechanism builds a franz-go SASL mechanism from already-normalized
// config. The mechanism string is upper-cased and validated at load
// time inside the per-cluster pipeline, so a bogus value never reaches
// this function — the cluster is quarantined in Loaded.InvalidClusters
// first.
func buildSASLMechanism(s *config.SASLConfig) (sasl.Mechanism, error) {
	if s.Mechanism == "" {
		return nil, errors.New("mechanism is empty")
	}
	if s.Username == "" {
		return nil, errors.New("username is empty")
	}
	switch s.Mechanism {
	case saslMechanismPlain:
		return plain.Auth{User: s.Username, Pass: s.Password}.AsMechanism(), nil
	case saslMechanismScram256:
		return scram.Auth{User: s.Username, Pass: s.Password}.AsSha256Mechanism(), nil
	case saslMechanismScram512:
		return scram.Auth{User: s.Username, Pass: s.Password}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("unsupported mechanism %q (allowed: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)", s.Mechanism)
	}
}

// Dial opens a Kafka client connected to the given cluster. Dial does not
// block on broker connectivity — call Ping to surface unreachable clusters.
func Dial(c config.Cluster, dopts DialOptions) (*Client, error) {
	opts, proto, err := BuildClientOptions(c, dopts)
	if err != nil {
		return nil, err
	}
	kc, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new client: %w", err)
	}
	return newClient(kc, c, proto), nil
}

// Ping issues a bounded broker-metadata request to verify connectivity.
func (c *Client) Ping(ctx context.Context, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = DefaultPingTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if _, err := c.adm.BrokerMetadata(ctx); err != nil {
		return fmt.Errorf("kafka: ping: %w", err)
	}
	return nil
}
