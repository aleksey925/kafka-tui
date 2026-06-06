package tui

import (
	"context"
	"fmt"
	"time"

	"github.com/aleksey925/kafka-tui/internal/config"
	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/clusters"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/messages"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/produce"
)

// Dialer constructs a connected [*kafka.Client] for a given cluster.
type Dialer interface {
	Dial(c config.Cluster) (*kafka.Client, error)
}

// DialerFunc adapts a function into a [Dialer].
type DialerFunc func(c config.Cluster) (*kafka.Client, error)

func (f DialerFunc) Dial(c config.Cluster) (*kafka.Client, error) { return f(c) }

func NewKafkaDialer(clientID string) Dialer {
	return DialerFunc(func(c config.Cluster) (*kafka.Client, error) {
		return kafka.Dial(c, kafka.DialOptions{ClientID: clientID})
	})
}

// NewClusterPinger returns a [clusters.Pinger] that dials, pings, and closes
// for every probe. The timeout is applied to the ping itself.
func NewClusterPinger(d Dialer, timeout time.Duration) clusters.Pinger {
	return clusters.PingerFunc(func(ctx context.Context, c config.Cluster) error {
		client, err := d.Dial(c)
		if err != nil {
			return fmt.Errorf("dial %q: %w", c.Name, err)
		}
		defer client.Close()
		if err := client.Ping(ctx, timeout); err != nil {
			return fmt.Errorf("ping %q: %w", c.Name, err)
		}
		return nil
	})
}

// Connector establishes a verified connection to a cluster: it dials and
// confirms the broker is reachable (Ping), returning a live client only on
// success. Unlike [Pinger], which probes and discards, the Connector hands
// back the live client to use.
type Connector interface {
	Connect(c config.Cluster) (*kafka.Client, error)
}

// ConnectorFunc adapts a function into a [Connector].
type ConnectorFunc func(c config.Cluster) (*kafka.Client, error)

func (f ConnectorFunc) Connect(c config.Cluster) (*kafka.Client, error) { return f(c) }

// NewKafkaConnector returns a [Connector] that dials, then pings to verify
// the broker is reachable. On a ping failure the half-open client is closed
// so a broker that constructs a client but can't be reached never leaks a
// connection. The timeout applies to the ping.
func NewKafkaConnector(d Dialer, timeout time.Duration) Connector {
	return ConnectorFunc(func(c config.Cluster) (*kafka.Client, error) {
		client, err := d.Dial(c)
		if err != nil {
			return nil, fmt.Errorf("dial: %w", err)
		}
		if err := client.Ping(context.Background(), timeout); err != nil {
			client.Close()
			return nil, fmt.Errorf("ping: %w", err)
		}
		return client, nil
	})
}

// Bootstrap collects all wiring required to host real screens. When nil,
// the host falls back to a placeholder body — used by tests that don't
// exercise screen rendering.
type Bootstrap struct {
	Loaded   *config.Loaded
	Clusters []config.Cluster
	// CLIName, when non-empty, marks the cluster from --brokers. The
	// inline cluster's auto-generated name lives here; the picker uses
	// it to render the "(cli)" badge on the inline row.
	CLIName string
	// AutoSelectCluster, when non-empty, names the cluster the app
	// should auto-connect to at startup instead of showing the picker.
	// Sourced from --cluster (selector) or from CLIName as a fallback
	// when --brokers was the only flag given.
	AutoSelectCluster       string
	GlobalPath, ProjectPath string
	LogPath                 string
	Connector               Connector
	Pinger                  clusters.Pinger
	// Editor defaults to [clusters.DefaultEditor] when nil.
	Editor clusters.Editor
	// Clipboard. nil disables copy.
	Clipboard messages.Clipboard
	// MessagesViewState persists per-(cluster, topic) seek state. nil
	// disables persistence; the screen always starts at `latest`.
	MessagesViewState messages.ViewStateRepository
	// RefreshIntervals persists the user-chosen auto-refresh cadence per
	// screen type. nil disables persistence; screens fall back to the
	// config-level default on every start.
	RefreshIntervals components.RefreshIntervalRepository
	Pager            produce.PagerOpener
	StartupWarnings  []string
	Now              func() time.Time
	// ConfigReloader re-reads config files from disk. nil disables manual reload.
	ConfigReloader func() (*config.Loaded, error)
	// ConfigSnapshots is the channel emitted by [config.Watcher]. nil
	// disables auto-reload.
	ConfigSnapshots <-chan config.Snapshot
}
