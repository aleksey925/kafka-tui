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
// One Snapshot per tea.Cmd; the host re-arms the listener after handling.
type configSnapshotMsg struct {
	snapshot config.Snapshot
}

func (m *Model) watchConfigSnapshots() tea.Cmd {
	if m.boot == nil || m.boot.ConfigSnapshots == nil {
		return nil
	}
	ch := m.boot.ConfigSnapshots
	return func() tea.Msg {
		s, ok := <-ch
		if !ok {
			slog.Warn("config watcher snapshot channel closed; auto-reload disabled")
			return nil
		}
		return configSnapshotMsg{snapshot: s}
	}
}

// handleConfigSnapshot applies a fresh config snapshot. Updates Bootstrap
// state and surfaces a toast on the clusters screen. Other screens stay
// silent — their data isn't config-derived without re-instantiation.
func (m *Model) handleConfigSnapshot(snap config.Snapshot) {
	if m.boot == nil {
		return
	}
	if snap.Err != nil {
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
			cs.Toasts().Push(components.ToastSuccess, fmt.Sprintf("clusters refreshed · %d", len(list)))
		} else {
			cs.Toasts().Push(components.ToastInfo, "config refreshed")
		}
	}
	// the live *kafka.Client is still wired to the previous broker/auth
	// values, so warn the user that a reconnect is required.
	if snap.ActiveClusterChanged {
		warning := "active cluster changed in config — reconnect to apply"
		slog.Warn(warning)
		// route via the screen's toast queue so promoteFlash picks it up;
		// direct assignment to m.flash would be wiped by promoteFlash.
		if q, ok := activeToastQueue(m.active); ok {
			q.Push(components.ToastWarning, warning)
		}
	}
}

// activeToastQueue exposes the active screen's toast queue. Returns
// ok=false for screens without queues (currently only configsrc).
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

func (m *Model) reloadClusters(s *clusters.Model) {
	if m.boot == nil || m.boot.ConfigReloader == nil {
		s.Toasts().Push(components.ToastWarning, "reload not configured")
		return
	}
	loaded, list, cli, err := m.boot.ConfigReloader()
	if err != nil {
		s.Toasts().Push(components.ToastError, "refresh: "+err.Error())
		return
	}
	m.boot.Loaded = loaded
	m.boot.Clusters = list
	m.boot.CLIName = cli
	s.SetClusters(list, cli)
	s.Toasts().Push(components.ToastSuccess, fmt.Sprintf("refreshed · %d clusters", len(list)))
}
