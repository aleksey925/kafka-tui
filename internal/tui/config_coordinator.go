// Config coordination — the host's reactions to live edits delivered by
// [config.Watcher]. Snapshots from the watcher's channel are surfaced as
// a typed [configSnapshotMsg] inside the Bubble Tea event loop so they
// participate in the same Update flow as keystrokes.

package tui

import (
	"fmt"
	"log/slog"
	"reflect"

	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/config"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/clusters"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/groups"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/logs"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/messages"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/produce"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/topics"
)

// configSnapshotMsg carries a fresh config snapshot from the file watcher.
// One Snapshot per tea.Cmd; after handling, the host re-arms the listener
// so subsequent file edits keep arriving.
type configSnapshotMsg struct {
	snapshot config.Snapshot
}

// watchConfigSnapshots returns a tea.Cmd that blocks on the watcher's
// Snapshots channel until the next event, then surfaces it as a typed msg.
// Returns nil when no watcher is wired (e.g. tests without bootstrap).
func (m *Model) watchConfigSnapshots() tea.Cmd {
	if m.boot == nil || m.boot.ConfigSnapshots == nil {
		return nil
	}
	ch := m.boot.ConfigSnapshots
	return func() tea.Msg {
		s, ok := <-ch
		if !ok {
			// channel closed (watcher.Close() called or fsnotify died) —
			// log so future "config didn't reload" reports have a trail.
			slog.Warn("config watcher snapshot channel closed; auto-reload disabled")
			return nil
		}
		return configSnapshotMsg{snapshot: s}
	}
}

// handleConfigSnapshot applies a fresh config snapshot delivered by the
// file watcher. Updates Bootstrap state for everyone; if the user is
// currently looking at the clusters screen, also pushes the fresh list
// into the model and a toast that honestly distinguishes "the cluster
// list changed" from "some other config field changed". Other screens
// stay silent (their data isn't config-derived in a way that re-render
// makes sense without a re-instantiate).
func (m *Model) handleConfigSnapshot(snap config.Snapshot) {
	if m.boot == nil {
		return
	}
	if snap.Err != nil {
		// surface parse errors; without this a syntax error in
		// clusters.yaml would silently keep the stale config and the
		// user would only notice on the next reconnect.
		slog.Error("config watcher: reload failed", "err", snap.Err)
		if cs, ok := m.active.(*clusters.Model); ok {
			cs.Toasts().Push(components.ToastError, "config reload failed: "+snap.Err.Error())
		}
		return
	}
	if snap.Loaded == nil {
		return
	}
	list := snap.Loaded.Clusters
	cli := ""
	if m.boot.BuildClusterList != nil {
		list, cli = m.boot.BuildClusterList(snap.Loaded.Clusters)
	}
	clustersChanged := !reflect.DeepEqual(m.boot.Clusters, list) || m.boot.CLIName != cli
	m.boot.Loaded = snap.Loaded
	m.boot.Clusters = list
	m.boot.CLIName = cli
	cs, onClusters := m.active.(*clusters.Model)
	if onClusters {
		cs.SetClusters(list, cli)
		if clustersChanged {
			cs.Toasts().Push(components.ToastSuccess, fmt.Sprintf("clusters reloaded · %d", len(list)))
		} else {
			cs.Toasts().Push(components.ToastInfo, "config reloaded")
		}
	}
	// active cluster's fields changed under us — the live *kafka.Client is
	// still wired to the previous broker/auth values, so warn the user
	// that a reconnect is required.
	if snap.ActiveClusterChanged {
		warning := "active cluster changed in config — reconnect to apply"
		// always log: even if the chrome can't surface it (e.g. on the
		// configsrc screen which has no toast queue), the warning sits
		// in the file log for troubleshooting.
		slog.Warn(warning)
		// route through the active screen's toast queue when possible —
		// promoteFlash will pick it up on the next render. Direct
		// assignment to m.flash would be wiped by promoteFlash because
		// the host-side flash slot is not a separate source.
		if q, ok := activeToastQueue(m.active); ok {
			q.Push(components.ToastWarning, warning)
		}
	}
}

// activeToastQueue exposes the active screen's toast queue when the
// concrete model has one. Returns ok=false for screens without queues
// (currently only configsrc).
func activeToastQueue(s Screen) (*components.Toasts, bool) {
	switch a := s.(type) {
	case *clusters.Model:
		return a.Toasts(), true
	case *topics.Model:
		return a.Toasts(), true
	case *messages.Model:
		return a.Toasts(), true
	case *produce.Model:
		return a.Toasts(), true
	case *groups.Model:
		return a.Toasts(), true
	case *logs.Model:
		return a.Toasts(), true
	case *topics.ConfigsModel:
		return a.Toasts(), true
	}
	return nil, false
}

// reloadClusters re-reads config files via Bootstrap.ConfigReloader and
// pushes the fresh list into the clusters screen. Errors are surfaced
// through the screen's toast queue (which the global flash bar promotes).
func (m *Model) reloadClusters(s *clusters.Model) {
	if m.boot == nil || m.boot.ConfigReloader == nil {
		s.Toasts().Push(components.ToastWarning, "reload not configured")
		return
	}
	loaded, list, cli, err := m.boot.ConfigReloader()
	if err != nil {
		s.Toasts().Push(components.ToastError, "reload: "+err.Error())
		return
	}
	m.boot.Loaded = loaded
	m.boot.Clusters = list
	m.boot.CLIName = cli
	s.SetClusters(list, cli)
	s.Toasts().Push(components.ToastSuccess, fmt.Sprintf("reloaded %d clusters", len(list)))
}
