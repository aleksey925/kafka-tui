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

// Bootstrap collects all wiring required to host real screens. When nil,
// the host falls back to a placeholder body — used by tests that don't
// exercise screen rendering.
type Bootstrap struct {
	Loaded   *config.Loaded
	Clusters []config.Cluster
	// CLIName, when non-empty, marks the cluster from --brokers.
	CLIName                 string
	GlobalPath, ProjectPath string
	LogPath                 string
	Dialer                  Dialer
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
	ReadOnly         bool
	Now              func() time.Time
	// ConfigReloader re-reads config files from disk. nil disables manual reload.
	ConfigReloader func() (*config.Loaded, []config.Cluster, string, error)
	// ConfigSnapshots is the channel emitted by [config.Watcher]. nil
	// disables auto-reload.
	ConfigSnapshots <-chan config.Snapshot
	// BuildClusterList re-applies the CLI inline cluster on top of a
	// freshly-loaded list.
	BuildClusterList func([]config.Cluster) ([]config.Cluster, string)
}
