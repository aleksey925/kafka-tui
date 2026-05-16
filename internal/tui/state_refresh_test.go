package tui_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/state"
	"github.com/aleksey925/kafka-tui/internal/tui"
)

func TestStateRefreshIntervals_RoundTrip(t *testing.T) {
	repo := tui.NewStateRefreshIntervals(openStore(t), slog.Default())

	require.NoError(t, repo.SaveRefreshInterval(t.Context(), "topics", 5*time.Second))
	got, ok, err := repo.LoadRefreshInterval(t.Context(), "topics")

	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, 5*time.Second, got)
}

func TestStateRefreshIntervals_AbsentReturnsFalse(t *testing.T) {
	repo := tui.NewStateRefreshIntervals(openStore(t), slog.Default())

	_, ok, err := repo.LoadRefreshInterval(t.Context(), "topics")

	require.NoError(t, err)
	assert.False(t, ok, "missing row must surface as ok=false")
}

// Load errors must degrade to ok=false so the screen falls back to its
// config-level default instead of refusing to open.
func TestStateRefreshIntervals_LoadAfterCloseDegradesToFalse(t *testing.T) {
	store, err := state.Open(context.Background(), ":memory:")
	require.NoError(t, err)
	require.NoError(t, store.Close())

	repo := tui.NewStateRefreshIntervals(store, slog.Default())
	_, ok, err := repo.LoadRefreshInterval(t.Context(), "topics")

	require.NoError(t, err, "load errors must be swallowed — caller falls back to config")
	assert.False(t, ok)
}

func TestNewStateRefreshIntervals_NilLoggerFallsBackToDefault(t *testing.T) {
	repo := tui.NewStateRefreshIntervals(openStore(t), nil)

	// any call exercises the wrapped logger; no panic = pass.
	_, _, _ = repo.LoadRefreshInterval(t.Context(), "x")
}
