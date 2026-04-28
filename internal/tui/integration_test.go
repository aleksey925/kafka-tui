package tui_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/config"
	"github.com/aleksey925/kafka-tui/internal/tui"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/clusters"
)

// TestModel_BootstrapRendersClustersBody verifies the host wires the real
// clusters screen instead of the "coming soon" placeholder.
func TestModel_BootstrapRendersClustersBody(t *testing.T) {
	// arrange
	loaded := &config.Loaded{
		Config:   config.Defaults(),
		Clusters: []config.Cluster{{Name: "local", Brokers: []string{"localhost:9092"}}},
	}
	boot := &tui.Bootstrap{
		Loaded:   loaded,
		Clusters: loaded.Clusters,
		Pinger: clusters.PingerFunc(func(ctx context.Context, c config.Cluster) error {
			return nil
		}),
	}
	m := tui.New(tui.Options{
		Initial:   tui.ScreenClusters,
		Width:     100,
		Height:    24,
		Bootstrap: boot,
	})

	// act
	out := m.Render()

	// assert
	assert.NotContains(t, out, "coming soon")
	assert.Contains(t, out, "local")
	assert.Contains(t, out, "localhost:9092")
}

// TestModel_NoBootstrapFallsBackToPlaceholder pins the test path: tests that
// don't supply a Bootstrap continue to see the legacy placeholder body.
func TestModel_NoBootstrapFallsBackToPlaceholder(t *testing.T) {
	m := tui.New(tui.Options{Initial: tui.ScreenTopics, Width: 80, Height: 24})

	out := m.Render()

	assert.Contains(t, out, "topics — coming soon")
}
