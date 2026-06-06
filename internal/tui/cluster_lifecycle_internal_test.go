package tui

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/config"
	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/clusters"
)

func TestHandleConnectResult__staleSameNameKeepsChecking(t *testing.T) {
	// arrange — two connects to the same row; the first is superseded.
	m := pickerModel(t, &config.Loaded{
		Config:   config.Defaults(),
		Clusters: []config.Cluster{{Name: "a", Brokers: []string{"x:9092"}}},
	})
	cs := m.active.(*clusters.Model)
	m.connectCluster("a") // gen 1
	m.connectCluster("a") // gen 2, row still checking
	require.Equal(t, clusters.StatusChecking, cs.Status("a"))

	// act — the superseded gen-1 result arrives after gen 2 is in flight.
	m.handleConnectResult(connectResultMsg{name: "a", gen: 1})

	// assert — the live gen-2 "checking…" must survive.
	assert.Equal(t, clusters.StatusChecking, cs.Status("a"))
}

func TestHandleConnectResult__staleDifferentNameClearsAbandonedRow(t *testing.T) {
	// arrange — connect to a, then supersede it with a connect to b.
	m := pickerModel(t, &config.Loaded{
		Config: config.Defaults(),
		Clusters: []config.Cluster{
			{Name: "a", Brokers: []string{"x:9092"}},
			{Name: "b", Brokers: []string{"y:9092"}},
		},
	})
	cs := m.active.(*clusters.Model)
	m.connectCluster("a") // gen 1
	m.connectCluster("b") // gen 2, abandons a's in-flight connect

	// act — a's superseded result arrives; nothing else will clear its row.
	m.handleConnectResult(connectResultMsg{name: "a", gen: 1})

	// assert — a drops back to unknown, b keeps its live checking.
	assert.Equal(t, clusters.StatusUnknown, cs.Status("a"))
	assert.Equal(t, clusters.StatusChecking, cs.Status("b"))
}

func TestConnectCluster__unknownNameRaisesToast(t *testing.T) {
	// arrange
	m := pickerModel(t, &config.Loaded{
		Config:   config.Defaults(),
		Clusters: []config.Cluster{{Name: "a", Brokers: []string{"x:9092"}}},
	})
	cs := m.active.(*clusters.Model)

	// act — `:cluster ghost` reaches connectCluster with a name nobody loaded.
	cmd := m.connectCluster("ghost")

	// assert — no silent no-op: the row stays untouched and a toast explains.
	assert.Nil(t, cmd)
	require.Equal(t, 1, cs.Toasts().Len())
	assert.Contains(t, cs.Toasts().Items()[0].Message, "ghost")
	assert.Contains(t, cs.Toasts().Items()[0].Message, "not found")
}

func TestConnectCluster__invalidNameReportsLoadReason(t *testing.T) {
	// arrange — a quarantined cluster is absent from Clusters but present in
	// InvalidClusters with its failure reason.
	m := pickerModel(t, &config.Loaded{
		Config:   config.Defaults(),
		Clusters: nil,
		InvalidClusters: []config.InvalidCluster{
			{Cluster: config.Cluster{Name: "broken"}, Reason: errors.New("vault: connection refused")},
		},
	})
	cs := m.active.(*clusters.Model)

	// act
	cmd := m.connectCluster("broken")

	// assert
	assert.Nil(t, cmd)
	require.Equal(t, 1, cs.Toasts().Len())
	assert.Contains(t, cs.Toasts().Items()[0].Message, "broken")
	assert.Contains(t, cs.Toasts().Items()[0].Message, "connection refused")
}

func pickerModel(t *testing.T, loaded *config.Loaded) *Model {
	t.Helper()
	return New(Options{
		Initial: ScreenClusters,
		Width:   80,
		Height:  24,
		Bootstrap: &Bootstrap{
			Loaded:    loaded,
			Clusters:  loaded.Clusters,
			Connector: ConnectorFunc(func(config.Cluster) (*kafka.Client, error) { return nil, nil }),
		},
	})
}
