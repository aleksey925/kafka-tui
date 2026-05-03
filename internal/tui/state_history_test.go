package tui_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/state"
	"github.com/aleksey925/kafka-tui/internal/tui"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/produce"
)

func TestStateHistory_AddAndLastForTopic(t *testing.T) {
	store := openStore(t)
	hist := tui.NewStateHistory(store, slog.Default())
	entry := produce.Entry{
		Cluster:     "alpha",
		Topic:       "orders",
		Key:         []byte("k"),
		Value:       []byte("v"),
		Headers:     []kafka.Header{{Key: "h", Value: []byte("1")}},
		Partition:   3,
		Compression: kafka.CompressionLZ4,
		Timestamp:   time.Now().UTC().Truncate(time.Second),
	}

	hist.Add(entry)
	got, ok := hist.LastForTopic("orders")

	require.True(t, ok)
	assert.Equal(t, entry, got)
}

func TestStateHistory_LastForTopic_MissingReturnsFalse(t *testing.T) {
	hist := tui.NewStateHistory(openStore(t), slog.Default())

	_, ok := hist.LastForTopic("nope")

	assert.False(t, ok)
}

func TestStateHistory_RecentReturnsNewestFirst(t *testing.T) {
	store := openStore(t)
	hist := tui.NewStateHistory(store, slog.Default())
	first := produce.Entry{
		Cluster: "c", Topic: "a", Value: []byte("1"),
		Timestamp: time.Now().Add(-time.Hour).UTC().Truncate(time.Second),
	}
	second := produce.Entry{
		Cluster: "c", Topic: "b", Value: []byte("2"),
		Timestamp: time.Now().UTC().Truncate(time.Second),
	}

	hist.Add(first)
	hist.Add(second)
	got := hist.Recent(10)

	require.Len(t, got, 2)
	assert.Equal(t, second.Topic, got[0].Topic)
	assert.Equal(t, first.Topic, got[1].Topic)
}

func TestNewStateHistory_NilLoggerFallsBackToDefault(t *testing.T) {
	hist := tui.NewStateHistory(openStore(t), nil)

	// any call exercises the wrapped logger; no panic = pass.
	_, _ = hist.LastForTopic("x")
}

func openStore(t *testing.T) *state.Store {
	t.Helper()
	store, err := state.Open(context.Background(), ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}
