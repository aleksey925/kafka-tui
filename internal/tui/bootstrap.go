package tui

import (
	"context"
	"fmt"
	"time"

	"github.com/aleksey925/kafka-tui/internal/config"
	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/clusters"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/messages"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/produce"
)

// Dialer constructs a connected [*kafka.Client] for a given cluster.
// Production wires this to [kafka.Dial]; tests inject a fake.
type Dialer interface {
	Dial(c config.Cluster) (*kafka.Client, error)
}

// DialerFunc adapts a function into a [Dialer].
type DialerFunc func(c config.Cluster) (*kafka.Client, error)

// Dial calls f.
func (f DialerFunc) Dial(c config.Cluster) (*kafka.Client, error) { return f(c) }

// NewKafkaDialer returns a [Dialer] that calls [kafka.Dial] with the supplied
// dial options (today: just the client ID).
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

// Bootstrap collects all wiring required to host the real screens. It is
// passed to [Model] via [Options.Bootstrap]. When nil, the host falls back
// to a placeholder body — used only by unit tests that don't exercise
// screen rendering.
type Bootstrap struct {
	// Loaded is the merged config snapshot. Required.
	Loaded *config.Loaded
	// Clusters is the resolved cluster list (loaded.Clusters merged with the
	// CLI inline cluster, if any). Order is preserved.
	Clusters []config.Cluster
	// CLIName, when non-empty, marks the cluster from --brokers.
	CLIName string
	// GlobalPath / ProjectPath are the absolute paths shown in the clusters
	// edit-target chooser. Either may be empty.
	GlobalPath, ProjectPath string
	// LogPath is the resolved absolute path of the rotating log file.
	LogPath string
	// Dialer constructs *kafka.Client for the active cluster. Required.
	Dialer Dialer
	// Pinger probes connectivity from the clusters screen. Required.
	Pinger clusters.Pinger
	// Editor opens clusters.yaml. Defaults to [clusters.DefaultEditor] when nil.
	Editor clusters.Editor
	// History persists produce form entries. nil disables history.
	History produce.History
	// Clipboard is forwarded to messages detail for copy hotkeys. nil disables copy.
	Clipboard messages.Clipboard
	// Pager opens the produce value field in $EDITOR. nil disables ctrl+e.
	Pager produce.PagerOpener
	// StartupWarnings is surfaced as toasts on the clusters screen.
	StartupWarnings []string
	// ReadOnly forwards the global read-only mode to all destructive screens.
	ReadOnly bool
	// Now is the injected clock. Defaults to time.Now.
	Now func() time.Time
	// ConfigReloader re-reads config.yaml/clusters.yaml from disk and
	// returns a fresh snapshot. Wired by main.go from the original CLI
	// flags. nil disables manual reload (`r` on the clusters screen).
	ConfigReloader func() (*config.Loaded, []config.Cluster, string, error)
	// ConfigSnapshots is the channel emitted by [config.Watcher] for every
	// reload triggered by a filesystem event. nil disables auto-reload.
	ConfigSnapshots <-chan config.Snapshot
	// BuildClusterList re-applies the CLI inline cluster on top of a
	// freshly-loaded clusters list. Wired together with ConfigSnapshots.
	BuildClusterList func([]config.Cluster) ([]config.Cluster, string)
}
