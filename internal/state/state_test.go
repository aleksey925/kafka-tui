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
	assert.Equal(t, 4, version)

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
	assert.Contains(t, tables, "schema_version")
	assert.Contains(t, tables, "messages_view_state")
	assert.Contains(t, tables, "refresh_intervals")
	assert.NotContains(t, tables, "produce_history",
		"migration v4 must drop the legacy produce_history table")
}

// Regression: on shared hosts a 0o644 DB (sqlite's default + umask) lets
// other accounts read persisted view state.
func TestOpen_RestrictsFilePermissions(t *testing.T) {
	// arrange
	root := t.TempDir()
	path := filepath.Join(root, "kafka-tui", "state.db")

	// act
	store, err := state.Open(t.Context(), path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	// assert — parent dir 0o700, DB file 0o600.
	dirInfo, err := os.Stat(filepath.Dir(path))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o700), dirInfo.Mode().Perm(),
		"state directory must be user-private (0o700)")

	fileInfo, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), fileInfo.Mode().Perm(),
		"state DB must be user-private (0o600)")
}

// Regression: ":memory:" databases are per-connection in SQLite, so without
// SetMaxOpenConns(1) a follow-up Load could hit a different connection that
// never saw the Save and return ok=false.
func TestOpen_InMemoryConnectionIsPinned(t *testing.T) {
	// arrange + act
	store, err := state.Open(t.Context(), ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	// assert
	require.NoError(t, store.SaveRefreshInterval(t.Context(), "topics", 5*time.Second))
	got, ok, err := store.LoadRefreshInterval(t.Context(), "topics")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, 5*time.Second, got)
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
	assert.Equal(t, 4, rowCount)
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

func TestSaveRefreshInterval_AfterClose_ReturnsError(t *testing.T) {
	// arrange
	store, err := state.Open(t.Context(), ":memory:")
	require.NoError(t, err)
	require.NoError(t, store.Close())

	// act
	err = store.SaveRefreshInterval(t.Context(), "topics", time.Second)

	// assert
	require.Error(t, err)
}

func TestSaveRefreshInterval_ContextCancellationStopsWrite(t *testing.T) {
	// arrange
	store := openMemory(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	// act
	err := store.SaveRefreshInterval(ctx, "topics", time.Second)

	// assert
	require.Error(t, err)
	_, ok, loadErr := store.LoadRefreshInterval(t.Context(), "topics")
	require.NoError(t, loadErr)
	assert.False(t, ok, "canceled write must not leave a row behind")
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

func TestMessagesView_RoundTrip(t *testing.T) {
	store, err := state.Open(t.Context(), ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	ts := time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC)
	view := state.MessagesView{
		SeekMode:   3,
		Partition:  2,
		Offset:     500,
		Timestamp:  ts,
		HasPart:    true,
		Partitions: "0-2,5",
	}
	require.NoError(t, store.SaveMessagesView(t.Context(), "c1", "orders", view))

	got, ok, err := store.LoadMessagesView(t.Context(), "c1", "orders")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, view.SeekMode, got.SeekMode)
	assert.Equal(t, view.Partition, got.Partition)
	assert.Equal(t, view.Offset, got.Offset)
	assert.True(t, ts.Equal(got.Timestamp))
	assert.Equal(t, view.HasPart, got.HasPart)
	assert.Equal(t, view.Partitions, got.Partitions)
}

func TestMessagesView_AbsentReturnsFalse(t *testing.T) {
	store, err := state.Open(t.Context(), ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	_, ok, err := store.LoadMessagesView(t.Context(), "c1", "orders")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestMessagesView_PerClusterIsolation(t *testing.T) {
	store, err := state.Open(t.Context(), ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	require.NoError(t, store.SaveMessagesView(t.Context(), "c1", "orders", state.MessagesView{SeekMode: 1}))
	require.NoError(t, store.SaveMessagesView(t.Context(), "c2", "orders", state.MessagesView{SeekMode: 2}))

	v1, _, _ := store.LoadMessagesView(t.Context(), "c1", "orders")
	v2, _, _ := store.LoadMessagesView(t.Context(), "c2", "orders")
	assert.Equal(t, 1, v1.SeekMode)
	assert.Equal(t, 2, v2.SeekMode)
}

func TestMessagesView_UpsertOverwrites(t *testing.T) {
	store, err := state.Open(t.Context(), ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	require.NoError(t, store.SaveMessagesView(t.Context(), "c1", "orders", state.MessagesView{SeekMode: 1, Partitions: "0"}))
	require.NoError(t, store.SaveMessagesView(t.Context(), "c1", "orders", state.MessagesView{SeekMode: 5, Partitions: "0-4"}))

	got, _, _ := store.LoadMessagesView(t.Context(), "c1", "orders")
	assert.Equal(t, 5, got.SeekMode)
	assert.Equal(t, "0-4", got.Partitions)
}

func TestRefreshInterval_RoundTrip(t *testing.T) {
	store := openMemory(t)

	require.NoError(t, store.SaveRefreshInterval(t.Context(), "topics", 5*time.Second))

	got, ok, err := store.LoadRefreshInterval(t.Context(), "topics")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, 5*time.Second, got)
}

func TestRefreshInterval_AbsentReturnsFalse(t *testing.T) {
	store := openMemory(t)

	_, ok, err := store.LoadRefreshInterval(t.Context(), "topics")
	require.NoError(t, err)
	assert.False(t, ok, "missing row must surface as ok=false so callers fall back to config")
}

// Manual is a real user choice — 0 must round-trip and stay distinguishable
// from "no row" via the ok flag.
func TestRefreshInterval_ManualPersists(t *testing.T) {
	store := openMemory(t)

	require.NoError(t, store.SaveRefreshInterval(t.Context(), "topics", 0))

	got, ok, err := store.LoadRefreshInterval(t.Context(), "topics")
	require.NoError(t, err)
	assert.True(t, ok, "Manual (0) must be reported as present, not absent")
	assert.Equal(t, time.Duration(0), got)
}

func TestRefreshInterval_PerScreenIsolation(t *testing.T) {
	store := openMemory(t)

	require.NoError(t, store.SaveRefreshInterval(t.Context(), "topics", 5*time.Second))
	require.NoError(t, store.SaveRefreshInterval(t.Context(), "groups", time.Minute))

	topicsVal, _, _ := store.LoadRefreshInterval(t.Context(), "topics")
	groupsVal, _, _ := store.LoadRefreshInterval(t.Context(), "groups")
	assert.Equal(t, 5*time.Second, topicsVal)
	assert.Equal(t, time.Minute, groupsVal)
}

func TestRefreshInterval_UpsertOverwrites(t *testing.T) {
	store := openMemory(t)

	require.NoError(t, store.SaveRefreshInterval(t.Context(), "topics", 5*time.Second))
	require.NoError(t, store.SaveRefreshInterval(t.Context(), "topics", time.Minute))

	got, _, _ := store.LoadRefreshInterval(t.Context(), "topics")
	assert.Equal(t, time.Minute, got)
}

func TestRefreshInterval_NegativeClampedOnSave(t *testing.T) {
	store := openMemory(t)

	require.NoError(t, store.SaveRefreshInterval(t.Context(), "topics", -5*time.Second))

	got, ok, err := store.LoadRefreshInterval(t.Context(), "topics")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, time.Duration(0), got,
		"negative durations are not meaningful — clamp to 0 (Manual) on the way in")
}
