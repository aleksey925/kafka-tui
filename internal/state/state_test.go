package state_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/state"
)

func TestOpen_CreatesParentDirectoryAndStartsEmpty(t *testing.T) {
	// arrange
	root := t.TempDir()
	path := filepath.Join(root, "nested", "kafka-tui", "state.json")

	// act
	store, err := state.Open(t.Context(), path)

	// assert
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	// nothing on disk yet — load was a no-op on a missing file.
	_, statErr := os.Stat(path)
	assert.True(t, os.IsNotExist(statErr), "fresh open must not create the state file pre-emptively")

	// parent dir must exist with 0o700 perms.
	dirInfo, err := os.Stat(filepath.Dir(path))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o700), dirInfo.Mode().Perm())
}

func TestOpen_RestrictsFilePermissions(t *testing.T) {
	// arrange
	path := filepath.Join(t.TempDir(), "kafka-tui", "state.json")
	store, err := state.Open(t.Context(), path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	// act — first save materializes the file.
	require.NoError(t, store.SaveRefreshInterval(t.Context(), "topics", time.Second))

	// assert — parent dir 0o700, file 0o600.
	dirInfo, err := os.Stat(filepath.Dir(path))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o700), dirInfo.Mode().Perm())

	fileInfo, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), fileInfo.Mode().Perm())
}

func TestOpen_InMemoryStaysInMemory(t *testing.T) {
	// arrange + act
	store, err := state.Open(t.Context(), ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	// assert — writes survive across calls but never touch disk.
	require.NoError(t, store.SaveRefreshInterval(t.Context(), "topics", 5*time.Second))
	got, ok, err := store.LoadRefreshInterval(t.Context(), "topics")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, 5*time.Second, got)
}

func TestOpen_ReloadsPersistedSnapshot(t *testing.T) {
	// arrange
	path := filepath.Join(t.TempDir(), "state.json")
	first, err := state.Open(t.Context(), path)
	require.NoError(t, err)
	view := state.MessagesView{
		SeekMode:   3,
		Partition:  7,
		Offset:     1042,
		Timestamp:  time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC),
		HasPart:    true,
		Partitions: "0-2,5",
	}
	require.NoError(t, first.SaveMessagesView(t.Context(), "stage", "orders", view))
	require.NoError(t, first.SaveRefreshInterval(t.Context(), "topics", 30*time.Second))
	require.NoError(t, first.Close())

	// act — reopen against the same path.
	second, err := state.Open(t.Context(), path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })

	gotView, ok, err := second.LoadMessagesView(t.Context(), "stage", "orders")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, view, gotView)

	gotInt, ok, err := second.LoadRefreshInterval(t.Context(), "topics")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, 30*time.Second, gotInt)
}

func TestOpen_MalformedFileIsQuarantined(t *testing.T) {
	// arrange — pre-seed a non-JSON file at the expected path.
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	require.NoError(t, os.WriteFile(path, []byte("this is not json"), 0o600))

	// act
	store, err := state.Open(t.Context(), path)
	require.NoError(t, err, "broken file must not prevent the store from opening")
	t.Cleanup(func() { _ = store.Close() })

	// assert — original file moved aside, no usable state on the new one.
	_, ok, err := store.LoadRefreshInterval(t.Context(), "topics")
	require.NoError(t, err)
	assert.False(t, ok, "quarantined snapshot must not leak rows into the fresh store")

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var foundBroken bool
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "state.json.broken-") {
			foundBroken = true
		}
	}
	assert.True(t, foundBroken, "malformed file must be renamed to state.json.broken-<unixts>")
}

func TestOpen_UnknownVersionIsQuarantined(t *testing.T) {
	// arrange — valid JSON, but a version number this binary doesn't recognize.
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	bogus := map[string]any{
		"version":           999,
		"refresh_intervals": map[string]int64{"topics": int64(time.Hour)},
	}
	buf, err := json.Marshal(bogus)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, buf, 0o600))

	// act
	store, err := state.Open(t.Context(), path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	// assert — payload was NOT loaded; the file was moved aside.
	_, ok, err := store.LoadRefreshInterval(t.Context(), "topics")
	require.NoError(t, err)
	assert.False(t, ok, "out-of-band version must not be silently honored")

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var foundBroken bool
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "state.json.broken-") {
			foundBroken = true
		}
	}
	assert.True(t, foundBroken, "unknown version must be quarantined")
}

func TestSaveMessagesView_RoundTripsEveryField(t *testing.T) {
	store := openMemory(t)
	view := state.MessagesView{
		SeekMode:   4,
		Partition:  2,
		Offset:     500,
		Timestamp:  time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC),
		HasPart:    true,
		Partitions: "0-3",
	}

	require.NoError(t, store.SaveMessagesView(t.Context(), "c1", "orders", view))
	got, ok, err := store.LoadMessagesView(t.Context(), "c1", "orders")

	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, view, got)
}

func TestLoadMessagesView_MissingReturnsFalse(t *testing.T) {
	store := openMemory(t)

	_, ok, err := store.LoadMessagesView(t.Context(), "c1", "nope")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestSaveMessagesView_PerClusterAndTopicIsolation(t *testing.T) {
	store := openMemory(t)

	require.NoError(t, store.SaveMessagesView(t.Context(), "c1", "orders", state.MessagesView{SeekMode: 1}))
	require.NoError(t, store.SaveMessagesView(t.Context(), "c2", "orders", state.MessagesView{SeekMode: 2}))
	require.NoError(t, store.SaveMessagesView(t.Context(), "c1", "events", state.MessagesView{SeekMode: 3}))

	for _, tc := range []struct {
		cluster, topic string
		wantMode       int
	}{
		{"c1", "orders", 1},
		{"c2", "orders", 2},
		{"c1", "events", 3},
	} {
		got, ok, err := store.LoadMessagesView(t.Context(), tc.cluster, tc.topic)
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, tc.wantMode, got.SeekMode, "(%s, %s)", tc.cluster, tc.topic)
	}
}

func TestSaveMessagesView_UpsertOverwrites(t *testing.T) {
	store := openMemory(t)

	require.NoError(t, store.SaveMessagesView(t.Context(), "c1", "orders", state.MessagesView{SeekMode: 1, Partitions: "0"}))
	require.NoError(t, store.SaveMessagesView(t.Context(), "c1", "orders", state.MessagesView{SeekMode: 5, Partitions: "0-4"}))

	got, ok, err := store.LoadMessagesView(t.Context(), "c1", "orders")
	require.NoError(t, err)
	require.True(t, ok)
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

func TestRefreshInterval_NegativeClampedOnSave(t *testing.T) {
	store := openMemory(t)

	require.NoError(t, store.SaveRefreshInterval(t.Context(), "topics", -5*time.Second))

	got, ok, err := store.LoadRefreshInterval(t.Context(), "topics")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, time.Duration(0), got)
}

// Regression: a manually-edited file with a negative duration must clamp
// on load, not propagate the bogus value through the Refresher contract.
func TestRefreshInterval_NegativeClampedOnLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	require.NoError(t, os.WriteFile(path, []byte(`{
		"version": 1,
		"refresh_intervals": {"topics": -5000000000}
	}`), 0o600))

	store, err := state.Open(t.Context(), path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	got, ok, err := store.LoadRefreshInterval(t.Context(), "topics")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, time.Duration(0), got)
}

func TestConcurrentSavesDoNotCorrupt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	store, err := state.Open(t.Context(), path)
	require.NoError(t, err)

	var wg sync.WaitGroup
	const writers = 8
	const ops = 25
	wg.Add(writers)
	for w := range writers {
		go func() {
			defer wg.Done()
			for i := range ops {
				_ = store.SaveRefreshInterval(t.Context(),
					fmt.Sprintf("screen-%d", w),
					time.Duration(i+1)*time.Second)
			}
		}()
	}
	wg.Wait()
	require.NoError(t, store.Close())

	// reopen against the same path so the assertion exercises the on-disk
	// file's integrity, not the original in-memory snapshot.
	reopened, err := state.Open(t.Context(), path)
	require.NoError(t, err, "file must still be parseable after the save storm")
	t.Cleanup(func() { _ = reopened.Close() })

	// last-writer-wins per key — every screen gets the final value (ops*1s).
	for w := range writers {
		got, ok, err := reopened.LoadRefreshInterval(t.Context(), fmt.Sprintf("screen-%d", w))
		require.NoError(t, err)
		require.True(t, ok, "screen-%d must be present on disk", w)
		assert.Equal(t, time.Duration(ops)*time.Second, got)
	}
}

func TestSave_AtomicReplaceLeavesNoTmpFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	store, err := state.Open(t.Context(), path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	for i := range 5 {
		require.NoError(t, store.SaveRefreshInterval(t.Context(), "topics",
			time.Duration(i+1)*time.Second))
	}

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		assert.NotContains(t, e.Name(), ".tmp",
			"save must temp+rename atomically — leftover %q means a path leaked", e.Name())
	}
}

func TestClose_IsSafeToCallTwice(t *testing.T) {
	store, err := state.Open(t.Context(), ":memory:")
	require.NoError(t, err)

	require.NoError(t, store.Close())
	require.NoError(t, store.Close())
}

func TestClose_NilReceiverIsSafe(t *testing.T) {
	var s *state.Store
	require.NoError(t, s.Close())
}

func TestDefaultPath_ReturnsExpectedSuffix(t *testing.T) {
	got, err := state.DefaultPath()
	require.NoError(t, err)
	assert.True(t, filepath.IsAbs(got))
	assert.Equal(t,
		filepath.Join(".local", "share", "kafka-tui", "state.json"),
		filepath.Join(filepath.Base(filepath.Dir(filepath.Dir(filepath.Dir(got)))),
			filepath.Base(filepath.Dir(filepath.Dir(got))),
			filepath.Base(filepath.Dir(got)),
			filepath.Base(got)))
}

func TestDefaultPath_HomeUnsetReturnsError(t *testing.T) {
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

	_, err := state.DefaultPath()
	require.Error(t, err)
}

func TestOpen_EmptyPathResolvesViaDefaultPath(t *testing.T) {
	tmp := t.TempDir()
	switch runtime.GOOS {
	case "windows":
		t.Setenv("USERPROFILE", tmp)
	default:
		t.Setenv("HOME", tmp)
	}

	store, err := state.Open(t.Context(), "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	// the resolved path is materialized on first save.
	require.NoError(t, store.SaveRefreshInterval(t.Context(), "topics", time.Second))
	expected := filepath.Join(tmp, ".local", "share", "kafka-tui", "state.json")
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
	path := filepath.Join(blocker, "nested", "state.json")

	_, err := state.Open(t.Context(), path)
	require.Error(t, err)
}

// helpers

func openMemory(t *testing.T) *state.Store {
	t.Helper()
	store, err := state.Open(t.Context(), ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}
