package tui

import (
	"fmt"

	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/config"
	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
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

// connectCluster dials the named cluster and replaces the topics screen on
// the stack. Closes the previous *kafka.Client, if any.
func (m *Model) connectCluster(name string) tea.Cmd {
	if m.boot == nil || m.boot.Dialer == nil {
		return nil
	}
	clu := findCluster(m.boot.Clusters, name)
	if clu == nil {
		return nil
	}
	client, err := m.boot.Dialer.Dial(*clu)
	if err != nil {
		msg := fmt.Sprintf("connect %q failed: %v", name, err)
		if q, ok := activeToastQueue(m.active); ok {
			q.Push(components.ToastError, msg)
			return nil
		}
		next := m.replaceScreen(ScreenClusters, "")
		if q, ok := activeToastQueue(m.active); ok {
			q.Push(components.ToastError, msg)
		}
		return next
	}
	if m.client != nil {
		m.client.Close()
	}
	m.client = client
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
		clu.ReadOnly || (m.boot != nil && m.boot.ReadOnly),
		name == m.boot.CLIName,
		kafka.IsInsecureTLS(*clu),
	)
	return m.replaceScreen(ScreenTopics, "")
}

func findCluster(list []config.Cluster, name string) *config.Cluster {
	for i := range list {
		if list[i].Name == name {
			return &list[i]
		}
	}
	return nil
}
