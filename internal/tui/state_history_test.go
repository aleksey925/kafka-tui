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
	hist := tui.NewStateHistory(store, 0, slog.Default())
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
	hist := tui.NewStateHistory(openStore(t), 0, slog.Default())

	_, ok := hist.LastForTopic("nope")

	assert.False(t, ok)
}

func TestStateHistory_RecentReturnsNewestFirst(t *testing.T) {
	store := openStore(t)
	hist := tui.NewStateHistory(store, 0, slog.Default())
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
	hist := tui.NewStateHistory(openStore(t), 0, nil)

	// any call exercises the wrapped logger; no panic = pass.
	_, _ = hist.LastForTopic("x")
}

func TestStateHistory_AddTrimsToHistSize(t *testing.T) {
	hist := tui.NewStateHistory(openStore(t), 2, slog.Default())
	base := time.Now().UTC().Truncate(time.Second)
	for i := range 5 {
		hist.Add(produce.Entry{
			Cluster:   "c",
			Topic:     "t",
			Value:     []byte{byte(i)},
			Timestamp: base.Add(time.Duration(i) * time.Second),
		})
	}

	got := hist.Recent(10)

	// trim cap is 2; the two newest entries (i = 4, 3) survive.
	require.Len(t, got, 2)
	assert.Equal(t, []byte{4}, got[0].Value)
	assert.Equal(t, []byte{3}, got[1].Value)
}

func openStore(t *testing.T) *state.Store {
	t.Helper()
	store, err := state.Open(context.Background(), ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}
