package tui

import (
	"fmt"

	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/config"
	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/logging"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/clusters"
)

func (m *Model) updateHeaderForActive(name, color string, readOnly, fromCLI, insecureTLS bool) {
	m.activeClu = name
	m.clusterClr = color
	m.clusterRO = readOnly
	m.fromCLI = fromCLI
	m.header = layout.HeaderInfo{
		Cluster:      name,
		ClusterColor: color,
		ReadOnly:     readOnly,
		FromCLI:      fromCLI,
		InsecureTLS:  insecureTLS,
		Context:      m.activeClusterContext(name),
	}
	logging.SetCluster(name)
}

// activeClusterContext derives the configuration-source label for the
// header from the loaded provenance map. Returns "" when no provenance is
// tracked (CLI inline clusters, or boot wiring missing) — the layout
// renders "cli" or "—" via its own fallback in that case.
func (m *Model) activeClusterContext(name string) string {
	if m.boot == nil || m.boot.Loaded == nil {
		return ""
	}
	return config.ClusterContext(m.boot.Loaded.Sources, name)
}

// connectResultMsg carries the outcome of an async connect attempt. On
// success it hands back the live *kafka.Client; gen pins it to the dispatch
// that issued it so a result arriving after the user moved on (a newer
// connect, a different screen) is dropped instead of swapping the client out
// from under the active session.
type connectResultMsg struct {
	name   string
	gen    uint64
	client *kafka.Client
	err    error
}

// connectCluster begins an async connect to the named cluster. The connect
// (dial + connectivity check) runs in the background; no cluster-bound screen
// is mounted here — [Model.handleConnectResult] decides where the user lands.
// This is the host's connect gate; see § Connecting to a cluster in CLAUDE.md.
func (m *Model) connectCluster(name string) tea.Cmd {
	if m.boot == nil || m.boot.Connector == nil {
		return nil
	}
	clu := findCluster(m.boot.Clusters, name)
	if clu == nil {
		// only `:cluster <name>` reaches here with a bad name (the picker and
		// auto-select paths pre-validate); without feedback the command would
		// be a silent no-op.
		if q, ok := activeToastQueue(m.active); ok {
			q.Push(components.ToastError, unknownClusterReason(m.boot, name))
		}
		return nil
	}
	m.connectGen++
	m.connectName = name
	if cs, ok := m.active.(*clusters.Model); ok {
		cs.SetConnectionStatus(name, clusters.StatusChecking)
	} else if q, ok := activeToastQueue(m.active); ok {
		q.Push(components.ToastInfo, fmt.Sprintf("connecting to %q…", name))
	}
	return connectCmd(m.boot.Connector, *clu, m.connectGen)
}

func connectCmd(c Connector, clu config.Cluster, gen uint64) tea.Cmd {
	return func() tea.Msg {
		client, err := c.Connect(clu)
		if err != nil {
			return connectResultMsg{name: clu.Name, gen: gen, err: err}
		}
		return connectResultMsg{name: clu.Name, gen: gen, client: client}
	}
}

// handleConnectResult applies a connect outcome. A stale result (superseded
// by a newer connect) is dropped; a stale success also closes its
// now-orphaned client so the background goroutine's connection doesn't leak.
func (m *Model) handleConnectResult(msg connectResultMsg) tea.Cmd {
	if msg.gen != m.connectGen {
		if msg.client != nil {
			msg.client.Close()
		}
		// a stale result is the only thing that would have cleared msg.name,
		// so drop its row back to unknown instead of leaving it stuck at
		// "checking…" — unless a newer connect for that same row is still in
		// flight (it will resolve the row itself; clearing would wipe its
		// live "checking…").
		if msg.name != m.connectName {
			if cs, ok := m.active.(*clusters.Model); ok {
				cs.ClearConnecting(msg.name)
			}
		}
		return nil
	}
	if msg.err != nil {
		return m.failConnect(msg.name, msg.err)
	}
	clu := findCluster(m.boot.Clusters, msg.name)
	if clu == nil {
		msg.client.Close()
		return nil
	}
	if m.client != nil {
		m.client.Close()
	}
	m.client = msg.client
	// closeActive must run BEFORE clear so the old screen's snapshot
	// (which closeActive captures into sessionState) is wiped along with
	// everything else. Reversing the order on `:cluster <name>` would
	// re-pollute the map after the clear, and the new cluster's topics
	// screen would inherit the stale state via restoreState.
	m.closeActive()
	clear(m.sessionState)
	m.updateHeaderForActive(
		clu.Name,
		clu.Color,
		clu.ReadOnly,
		msg.name == m.boot.CLIName,
		kafka.IsInsecureTLS(*clu),
	)
	return m.replaceScreen(ScreenTopics, "")
}

// failConnect routes a connect failure to the clusters picker: the user
// always lands on the picker with the reason, never on a half-mounted
// cluster-bound screen. When the picker is already active its row is marked
// failed; otherwise the picker is mounted first.
func (m *Model) failConnect(name string, err error) tea.Cmd {
	var initCmd tea.Cmd
	cs, ok := m.active.(*clusters.Model)
	if !ok {
		// a `:cluster` switch failed from a connected screen — surface the
		// picker so the failure has a home, then mark the row on the fresh
		// instance replaceScreen just mounted.
		initCmd = m.replaceScreen(ScreenClusters, "")
		cs, _ = m.active.(*clusters.Model)
	}
	if cs != nil {
		cs.SetConnectionStatus(name, clusters.StatusFailed)
	}
	if q, ok := activeToastQueue(m.active); ok {
		q.Push(components.ToastError, fmt.Sprintf("connect %q failed: %v", name, err))
	}
	return initCmd
}

// unknownClusterReason explains why a `:cluster <name>` switch found no
// connectable target: the name is either quarantined (invalid config, with
// its load reason) or absent from the loaded set entirely.
func unknownClusterReason(b *Bootstrap, name string) string {
	if b != nil && b.Loaded != nil {
		for _, ic := range b.Loaded.InvalidClusters {
			if ic.Cluster.Name == name {
				return fmt.Sprintf("cluster %q failed to load: %v", name, ic.Reason)
			}
		}
	}
	return fmt.Sprintf("cluster %q not found", name)
}

func findCluster(list []config.Cluster, name string) *config.Cluster {
	for i := range list {
		if list[i].Name == name {
			return &list[i]
		}
	}
	return nil
}
