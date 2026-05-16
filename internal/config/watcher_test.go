package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aleksey925/kafka-tui/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	watcherDebounce      = 30 * time.Millisecond
	watcherSnapshotDelay = 600 * time.Millisecond
)

func TestWatcher_InitialLoadReturned(t *testing.T) {
	// arrange
	homeDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(homeDir, ".kafka-tui"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(homeDir, ".kafka-tui", "config.yaml"),
		[]byte("logging:\n  level: warn\n"),
		0o644,
	))

	// act
	w, initial, err := config.NewWatcher(config.LoaderOptions{
		HomeDir:  homeDir,
		StartDir: t.TempDir(),
	}, "", watcherDebounce)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	// assert
	assert.Equal(t, "warn", initial.Config.Logging.Level)
}

func TestWatcher_EmitsSnapshotAfterEdit(t *testing.T) {
	// arrange
	homeDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(homeDir, ".kafka-tui"), 0o755))
	cfgPath := filepath.Join(homeDir, ".kafka-tui", "config.yaml")
	require.NoError(t, os.WriteFile(cfgPath, []byte("logging:\n  level: info\n"), 0o644))

	w, _, err := config.NewWatcher(config.LoaderOptions{
		HomeDir:  homeDir,
		StartDir: t.TempDir(),
	}, "", watcherDebounce)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	// act
	require.NoError(t, os.WriteFile(cfgPath, []byte("logging:\n  level: debug\n"), 0o644))
	snap := waitForSnapshot(t, w)

	// assert
	require.NoError(t, snap.Err)
	require.NotNil(t, snap.Loaded)
	assert.Equal(t, "debug", snap.Loaded.Config.Logging.Level)
	assert.False(t, snap.ActiveClusterChanged)
}

func TestWatcher_DebouncesRapidEdits(t *testing.T) {
	// arrange
	homeDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(homeDir, ".kafka-tui"), 0o755))
	cfgPath := filepath.Join(homeDir, ".kafka-tui", "config.yaml")
	require.NoError(t, os.WriteFile(cfgPath, []byte("logging:\n  level: info\n"), 0o644))

	w, _, err := config.NewWatcher(config.LoaderOptions{
		HomeDir:  homeDir,
		StartDir: t.TempDir(),
	}, "", 200*time.Millisecond)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	// act
	for i, level := range []string{"warn", "error", "debug"} {
		require.NoError(t, os.WriteFile(cfgPath, []byte("logging:\n  level: "+level+"\n"), 0o644))
		// burst: write quickly without giving the debounce a chance to fire
		if i < 2 {
			time.Sleep(20 * time.Millisecond)
		}
	}
	first := waitForSnapshot(t, w)
	// after the first snapshot, no further snapshot should arrive for a burst
	// of rapid edits collapsed into one debounce window
	second := tryReceiveSnapshot(w, 250*time.Millisecond)

	// assert
	require.NoError(t, first.Err)
	require.NotNil(t, first.Loaded)
	assert.Equal(t, "debug", first.Loaded.Config.Logging.Level)
	assert.Nil(t, second, "rapid bursts must collapse into a single snapshot")
}

func TestWatcher_DetectsActiveClusterChange(t *testing.T) {
	// arrange
	homeDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(homeDir, ".kafka-tui"), 0o755))
	clustersPath := filepath.Join(homeDir, ".kafka-tui", "clusters.yaml")
	require.NoError(t, os.WriteFile(clustersPath, []byte(""+
		"clusters:\n"+
		"  - name: prod\n"+
		"    brokers: [b1:9092]\n"+
		"  - name: stage\n"+
		"    brokers: [b2:9092]\n"), 0o644))

	w, initial, err := config.NewWatcher(config.LoaderOptions{
		HomeDir:  homeDir,
		StartDir: t.TempDir(),
	}, "prod", watcherDebounce)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	require.Len(t, initial.Clusters, 2)

	// act: change the active cluster's brokers
	require.NoError(t, os.WriteFile(clustersPath, []byte(""+
		"clusters:\n"+
		"  - name: prod\n"+
		"    brokers: [b1-new:9092]\n"+
		"  - name: stage\n"+
		"    brokers: [b2:9092]\n"), 0o644))
	snap := waitForSnapshot(t, w)

	// assert
	require.NoError(t, snap.Err)
	require.NotNil(t, snap.Loaded)
	assert.True(t, snap.ActiveClusterChanged)
}

func TestWatcher_IgnoresInactiveClusterChanges(t *testing.T) {
	// arrange
	homeDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(homeDir, ".kafka-tui"), 0o755))
	clustersPath := filepath.Join(homeDir, ".kafka-tui", "clusters.yaml")
	require.NoError(t, os.WriteFile(clustersPath, []byte(""+
		"clusters:\n"+
		"  - name: prod\n"+
		"    brokers: [b1:9092]\n"+
		"  - name: stage\n"+
		"    brokers: [b2:9092]\n"), 0o644))

	w, _, err := config.NewWatcher(config.LoaderOptions{
		HomeDir:  homeDir,
		StartDir: t.TempDir(),
	}, "prod", watcherDebounce)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	// act: change a non-active cluster
	require.NoError(t, os.WriteFile(clustersPath, []byte(""+
		"clusters:\n"+
		"  - name: prod\n"+
		"    brokers: [b1:9092]\n"+
		"  - name: stage\n"+
		"    brokers: [b2-new:9092]\n"), 0o644))
	snap := waitForSnapshot(t, w)

	// assert
	require.NoError(t, snap.Err)
	require.NotNil(t, snap.Loaded)
	assert.False(t, snap.ActiveClusterChanged)
}

func TestWatcher_ReactsToProjectAndGlobalLayer(t *testing.T) {
	// arrange
	homeDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(homeDir, ".kafka-tui"), 0o755))
	globalCfg := filepath.Join(homeDir, ".kafka-tui", "config.yaml")
	require.NoError(t, os.WriteFile(globalCfg, []byte("logging:\n  level: info\n"), 0o644))

	projectRoot := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(projectRoot, ".kafka-tui"), 0o755))
	projectCfg := filepath.Join(projectRoot, ".kafka-tui", "config.yaml")
	require.NoError(t, os.WriteFile(projectCfg, []byte("logging:\n  level: warn\n"), 0o644))

	w, initial, err := config.NewWatcher(config.LoaderOptions{
		HomeDir:  homeDir,
		StartDir: projectRoot,
	}, "", watcherDebounce)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	assert.Equal(t, "warn", initial.Config.Logging.Level)

	// act: change the global layer (lower priority)
	require.NoError(t, os.WriteFile(globalCfg, []byte(""+
		"logging:\n"+
		"  level: info\n"+
		"  file: /tmp/global.log\n"), 0o644))
	snap := waitForSnapshot(t, w)

	// assert: project still wins for level, global file value is picked up
	require.NoError(t, snap.Err)
	require.NotNil(t, snap.Loaded)
	assert.Equal(t, "warn", snap.Loaded.Config.Logging.Level)
	assert.Equal(t, "/tmp/global.log", snap.Loaded.Config.Logging.File)

	// act: change the project layer
	require.NoError(t, os.WriteFile(projectCfg, []byte("logging:\n  level: error\n"), 0o644))
	snap2 := waitForSnapshot(t, w)

	// assert
	require.NoError(t, snap2.Err)
	require.NotNil(t, snap2.Loaded)
	assert.Equal(t, "error", snap2.Loaded.Config.Logging.Level)
}

func TestWatcher_ReloadsOnFilePlaceholderChange(t *testing.T) {
	// arrange
	homeDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(homeDir, ".kafka-tui"), 0o755))
	secretDir := t.TempDir()
	tokenPath := filepath.Join(secretDir, "token.txt")
	require.NoError(t, os.WriteFile(tokenPath, []byte("first-secret"), 0o644))

	clustersPath := filepath.Join(homeDir, ".kafka-tui", "clusters.yaml")
	require.NoError(t, os.WriteFile(clustersPath, []byte(""+
		"clusters:\n"+
		"  - name: prod\n"+
		"    brokers: [b1:9092]\n"+
		"    sasl:\n"+
		"      mechanism: PLAIN\n"+
		"      username: kafka\n"+
		"      password: ${file:"+tokenPath+"}\n"), 0o644))

	w, initial, err := config.NewWatcher(config.LoaderOptions{
		HomeDir:  homeDir,
		StartDir: t.TempDir(),
	}, "prod", watcherDebounce)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	require.NotNil(t, initial.Clusters[0].SASL)
	assert.Equal(t, "first-secret", initial.Clusters[0].SASL.Password)

	// act: rewrite the placeholder file
	require.NoError(t, os.WriteFile(tokenPath, []byte("rotated-secret"), 0o644))
	snap := waitForSnapshot(t, w)

	// assert
	require.NoError(t, snap.Err)
	require.NotNil(t, snap.Loaded)
	require.NotNil(t, snap.Loaded.Clusters[0].SASL)
	assert.Equal(t, "rotated-secret", snap.Loaded.Clusters[0].SASL.Password)
	assert.True(t, snap.ActiveClusterChanged)
}

func TestWatcher_IgnoresUnrelatedFiles(t *testing.T) {
	// arrange
	homeDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(homeDir, ".kafka-tui"), 0o755))
	cfgPath := filepath.Join(homeDir, ".kafka-tui", "config.yaml")
	require.NoError(t, os.WriteFile(cfgPath, []byte("logging:\n  level: info\n"), 0o644))

	w, _, err := config.NewWatcher(config.LoaderOptions{
		HomeDir:  homeDir,
		StartDir: t.TempDir(),
	}, "", watcherDebounce)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	// act: write a sibling file that we don't watch
	unrelated := filepath.Join(homeDir, ".kafka-tui", "notes.txt")
	require.NoError(t, os.WriteFile(unrelated, []byte("scratch"), 0o644))

	// assert: no snapshot
	snap := tryReceiveSnapshot(w, 300*time.Millisecond)
	assert.Nil(t, snap)
}

func TestWatcher_ClosePreventsFurtherSnapshots(t *testing.T) {
	// arrange
	homeDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(homeDir, ".kafka-tui"), 0o755))
	cfgPath := filepath.Join(homeDir, ".kafka-tui", "config.yaml")
	require.NoError(t, os.WriteFile(cfgPath, []byte("logging:\n  level: info\n"), 0o644))

	w, _, err := config.NewWatcher(config.LoaderOptions{
		HomeDir:  homeDir,
		StartDir: t.TempDir(),
	}, "", watcherDebounce)
	require.NoError(t, err)

	// act
	require.NoError(t, w.Close())

	// assert: channel closed
	_, ok := <-w.Snapshots()
	assert.False(t, ok)
}

// helpers

func waitForSnapshot(t *testing.T, w *config.Watcher) config.Snapshot {
	t.Helper()
	select {
	case snap, ok := <-w.Snapshots():
		require.True(t, ok, "snapshot channel closed unexpectedly")
		return snap
	case <-time.After(watcherSnapshotDelay):
		t.Fatal("timed out waiting for snapshot")
		return config.Snapshot{}
	}
}

func tryReceiveSnapshot(w *config.Watcher, wait time.Duration) *config.Snapshot {
	select {
	case snap, ok := <-w.Snapshots():
		if !ok {
			return nil
		}
		return &snap
	case <-time.After(wait):
		return nil
	}
}
