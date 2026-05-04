// Cluster lifecycle — dialing the active *kafka.Client when the user
// connects to a cluster, swapping it out on cluster switch, and
// updating the header chrome to reflect the live identity.

package tui

import (
	"fmt"

	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/config"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/clusters"
)

// updateHeaderForActive refreshes the header chrome when the active cluster
// changes (typically after [connectCluster]).
func (m *Model) updateHeaderForActive(name, color string, readOnly, fromCLI bool) {
	m.activeClu = name
	m.clusterClr = color
	m.clusterRO = readOnly
	m.fromCLI = fromCLI
	m.header = layout.HeaderInfo{
		Cluster:      name,
		ClusterColor: color,
		ReadOnly:     readOnly,
		FromCLI:      fromCLI,
	}
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
		// surface dial errors via the clusters screen toast queue when we
		// can; otherwise log and stay on the clusters screen.
		if cs, ok := m.active.(*clusters.Model); ok {
			cs.Toasts().Push(components.ToastError, fmt.Sprintf("connect %q failed: %v", name, err))
		}
		return nil
	}
	if m.client != nil {
		m.client.Close()
	}
	m.client = client
	m.updateHeaderForActive(clu.Name, clu.Color, clu.ReadOnly || (m.boot != nil && m.boot.ReadOnly), name == m.boot.CLIName)
	return m.replaceScreen(ScreenTopics, "")
}

// findCluster returns a pointer to the cluster with the given name, or nil.
func findCluster(list []config.Cluster, name string) *config.Cluster {
	for i := range list {
		if list[i].Name == name {
			return &list[i]
		}
	}
	return nil
}
