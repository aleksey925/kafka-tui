package state_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/state"
)

func TestOpen_CreatesParentDirectoryAndAppliesSchema(t *testing.T) {
	// arrange
	root := t.TempDir()
	path := filepath.Join(root, "nested", "kafka-tui", "state.db")

	// act
	store, err := state.Open(t.Context(), path)

	// assert
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	assert.Equal(t, path, store.Path())

	// schema_version is populated when migrations apply.
	db := openRaw(t, path)
	t.Cleanup(func() { _ = db.Close() })

	var version int
	require.NoError(t, db.QueryRow("SELECT MAX(version) FROM schema_version").Scan(&version))
	assert.Equal(t, 1, version)

	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var tables []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		tables = append(tables, name)
	}
	require.NoError(t, rows.Err())
	assert.Contains(t, tables, "produce_history")
	assert.Contains(t, tables, "schema_version")
}

func TestOpen_InMemoryIsIsolated(t *testing.T) {
	// arrange + act
	store, err := state.Open(t.Context(), ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	// assert
	require.NoError(t, store.AddProduce(t.Context(), entry("c", "topic-a", 0, time.Unix(1, 0)), 5))
	got, err := store.RecentProduce(t.Context(), 5)
	require.NoError(t, err)
	require.Len(t, got, 1)
}

func TestApplyMigrations_Idempotent(t *testing.T) {
	// arrange
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := state.Open(t.Context(), path)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	// act — reopening must not re-run migrations or fail.
	store2, err := state.Open(t.Context(), path)

	// assert
	require.NoError(t, err)
	t.Cleanup(func() { _ = store2.Close() })

	db := openRaw(t, path)
	t.Cleanup(func() { _ = db.Close() })
	var rowCount int
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM schema_version").Scan(&rowCount))
	assert.Equal(t, 1, rowCount)
}

func TestAddProduce_RoundTripsEveryField(t *testing.T) {
	// arrange
	store := openMemory(t)
	added := state.ProduceEntry{
		Cluster: "prod",
		Topic:   "events",
		Key:     []byte("user-42"),
		Value:   []byte(`{"hello":"world"}`),
		Headers: []kafka.Header{
			{Key: "trace-id", Value: []byte("abc-123")},
			{Key: "binary", Value: []byte{0x00, 0x01, 0xff}},
		},
		Partition:   2,
		Compression: kafka.CompressionZstd,
		Timestamp:   time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC),
	}

	// act
	require.NoError(t, store.AddProduce(t.Context(), added, 10))
	got, ok, err := store.LastProduceForTopic(t.Context(), "events")

	// assert
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, added, got)
}

func TestAddProduce_EmptyKeyAndValueAreNullable(t *testing.T) {
	// arrange
	store := openMemory(t)
	added := state.ProduceEntry{
		Cluster:     "c",
		Topic:       "t",
		Partition:   kafka.PartitionAuto,
		Compression: kafka.CompressionNone,
		Timestamp:   time.Unix(1, 0).UTC(),
	}

	// act
	require.NoError(t, store.AddProduce(t.Context(), added, 0))
	got, ok, err := store.LastProduceForTopic(t.Context(), "t")

	// assert
	require.NoError(t, err)
	require.True(t, ok)
	assert.Nil(t, got.Key)
	assert.Nil(t, got.Value)
	assert.Nil(t, got.Headers)
	assert.Equal(t, kafka.CompressionNone, got.Compression)
	assert.Equal(t, kafka.PartitionAuto, got.Partition)
}

func TestLastProduceForTopic_MissingTopicReturnsFalse(t *testing.T) {
	// arrange
	store := openMemory(t)
	require.NoError(t, store.AddProduce(t.Context(), entry("c", "alpha", 0, time.Unix(1, 0)), 0))

	// act
	got, ok, err := store.LastProduceForTopic(t.Context(), "beta")

	// assert
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, state.ProduceEntry{}, got)
}

func TestLastProduceForTopic_PicksNewest(t *testing.T) {
	// arrange
	store := openMemory(t)
	older := entry("c", "events", 0, time.Unix(100, 0))
	older.Value = []byte("old")
	newer := entry("c", "events", 1, time.Unix(200, 0))
	newer.Value = []byte("new")
	require.NoError(t, store.AddProduce(t.Context(), older, 0))
	require.NoError(t, store.AddProduce(t.Context(), newer, 0))

	// act
	got, ok, err := store.LastProduceForTopic(t.Context(), "events")

	// assert
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, []byte("new"), got.Value)
	assert.Equal(t, int32(1), got.Partition)
}

func TestRecentProduce_OrdersNewestFirst(t *testing.T) {
	// arrange
	store := openMemory(t)
	entries := []state.ProduceEntry{
		entry("alpha", "t1", 0, time.Unix(100, 0)),
		entry("beta", "t2", 0, time.Unix(200, 0)),
		entry("gamma", "t3", 0, time.Unix(300, 0)),
	}
	for _, e := range entries {
		require.NoError(t, store.AddProduce(t.Context(), e, 0))
	}

	// act
	got, err := store.RecentProduce(t.Context(), 5)

	// assert
	require.NoError(t, err)
	gotTopics := topics(got)
	assert.Equal(t, []string{"t3", "t2", "t1"}, gotTopics)
}

func TestRecentProduce_RespectsLimit(t *testing.T) {
	// arrange
	store := openMemory(t)
	for _, p := range []int32{0, 1, 2, 3, 4} {
		require.NoError(t, store.AddProduce(t.Context(),
			entry("c", "t", p, time.Unix(int64(p+1), 0)), 0))
	}

	// act
	got, err := store.RecentProduce(t.Context(), 2)

	// assert
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, int32(4), got[0].Partition)
	assert.Equal(t, int32(3), got[1].Partition)
}

func TestRecentProduce_ZeroOrNegativeLimit(t *testing.T) {
	// arrange
	store := openMemory(t)
	require.NoError(t, store.AddProduce(t.Context(), entry("c", "t", 0, time.Unix(1, 0)), 0))

	// act
	gotZero, errZero := store.RecentProduce(t.Context(), 0)
	gotNeg, errNeg := store.RecentProduce(t.Context(), -3)

	// assert
	require.NoError(t, errZero)
	require.NoError(t, errNeg)
	assert.Empty(t, gotZero)
	assert.Empty(t, gotNeg)
}

func TestAddProduce_TrimsToHistorySize(t *testing.T) {
	// arrange
	store := openMemory(t)
	for _, p := range []int32{0, 1, 2, 3, 4, 5} {
		require.NoError(t, store.AddProduce(t.Context(),
			entry("c", "events", p, time.Unix(int64(p+1), 0)), 3))
	}

	// act
	got, err := store.RecentProduce(t.Context(), 100)

	// assert
	require.NoError(t, err)
	require.Len(t, got, 3)
	gotPartitions := []int32{got[0].Partition, got[1].Partition, got[2].Partition}
	assert.Equal(t, []int32{5, 4, 3}, gotPartitions)
}

func TestAddProduce_ZeroHistorySizeKeepsEverything(t *testing.T) {
	// arrange
	store := openMemory(t)
	for _, p := range []int32{0, 1, 2, 3} {
		require.NoError(t, store.AddProduce(t.Context(),
			entry("c", "events", p, time.Unix(int64(p+1), 0)), 0))
	}

	// act
	got, err := store.RecentProduce(t.Context(), 100)

	// assert
	require.NoError(t, err)
	assert.Len(t, got, 4)
}

func TestAddProduce_DefaultsTimestampWhenZero(t *testing.T) {
	// arrange
	store := openMemory(t)
	before := time.Now().Add(-time.Second)
	added := entry("c", "t", 0, time.Time{})

	// act
	require.NoError(t, store.AddProduce(t.Context(), added, 0))
	got, ok, err := store.LastProduceForTopic(t.Context(), "t")

	// assert
	require.NoError(t, err)
	require.True(t, ok)
	assert.True(t, got.Timestamp.After(before),
		"persisted ts %s should be after %s", got.Timestamp, before)
}

func TestClose_IsSafeToCallTwice(t *testing.T) {
	// arrange
	store, err := state.Open(t.Context(), ":memory:")
	require.NoError(t, err)

	// act
	require.NoError(t, store.Close())
	err2 := store.Close()

	// assert — modernc returns sql.ErrConnDone or nil; both are acceptable.
	if err2 != nil {
		assert.ErrorIs(t, err2, sql.ErrConnDone)
	}
}

func TestDefaultPath_ReturnsExpectedSuffix(t *testing.T) {
	// act
	got, err := state.DefaultPath()

	// assert
	require.NoError(t, err)
	assert.True(t, filepath.IsAbs(got))
	assert.Equal(t, filepath.Join(".local", "share", "kafka-tui", "state.db"),
		filepath.Join(filepath.Base(filepath.Dir(filepath.Dir(filepath.Dir(got)))),
			filepath.Base(filepath.Dir(filepath.Dir(got))),
			filepath.Base(filepath.Dir(got)),
			filepath.Base(got)))
}

func TestAddProduce_AfterClose_ReturnsError(t *testing.T) {
	// arrange
	store, err := state.Open(t.Context(), ":memory:")
	require.NoError(t, err)
	require.NoError(t, store.Close())

	// act
	err = store.AddProduce(t.Context(), entry("c", "t", 0, time.Unix(1, 0)), 0)

	// assert
	require.Error(t, err)
}

func TestAddProduce_ContextCancellationStopsInsert(t *testing.T) {
	// arrange
	store := openMemory(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	// act
	err := store.AddProduce(ctx, entry("c", "t", 0, time.Unix(1, 0)), 0)

	// assert
	require.Error(t, err)
	got, listErr := store.RecentProduce(t.Context(), 10)
	require.NoError(t, listErr)
	assert.Empty(t, got)
}

func TestDefaultPath_HomeUnsetReturnsError(t *testing.T) {
	// arrange — os.UserHomeDir consults different env vars per platform.
	switch runtime.GOOS {
	case "windows":
		t.Setenv("USERPROFILE", "")
		t.Setenv("HOMEDRIVE", "")
		t.Setenv("HOMEPATH", "")
	case "plan9":
		t.Setenv("home", "")
	default:
		t.Setenv("HOME", "")
	}

	// act
	_, err := state.DefaultPath()

	// assert
	require.Error(t, err)
}

func TestOpen_EmptyPathResolvesViaDefaultPath(t *testing.T) {
	// arrange
	tmp := t.TempDir()
	switch runtime.GOOS {
	case "windows":
		t.Setenv("USERPROFILE", tmp)
	default:
		t.Setenv("HOME", tmp)
	}

	// act
	store, err := state.Open(t.Context(), "")

	// assert
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	expected := filepath.Join(tmp, ".local", "share", "kafka-tui", "state.db")
	assert.Equal(t, expected, store.Path())

	info, statErr := os.Stat(expected)
	require.NoError(t, statErr)
	assert.False(t, info.IsDir())
}

func TestOpen_CannotCreateParentDirectoryReturnsError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: MkdirAll succeeds against a regular file")
	}
	// arrange — make the parent path a file, so MkdirAll fails.
	tmp := t.TempDir()
	blocker := filepath.Join(tmp, "blocker")
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0o644))
	dbPath := filepath.Join(blocker, "nested", "state.db")

	// act
	_, err := state.Open(t.Context(), dbPath)

	// assert
	require.Error(t, err)
}

func TestClose_NilReceiverIsSafe(t *testing.T) {
	// arrange
	var s *state.Store

	// act + assert
	require.NoError(t, s.Close())
}

func TestOpen_ContextCancelledBeforePingReturnsError(t *testing.T) {
	// arrange
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "state.db")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	// act
	store, err := state.Open(ctx, dbPath)

	// assert
	require.Error(t, err)
	if store != nil {
		_ = store.Close()
	}
}

// helpers

func openMemory(t *testing.T) *state.Store {
	t.Helper()
	store, err := state.Open(t.Context(), ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func openRaw(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	return db
}

func entry(cluster, topic string, partition int32, ts time.Time) state.ProduceEntry {
	return state.ProduceEntry{
		Cluster:     cluster,
		Topic:       topic,
		Partition:   partition,
		Compression: kafka.CompressionNone,
		Timestamp:   ts,
	}
}

func topics(entries []state.ProduceEntry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.Topic
	}
	return out
}
