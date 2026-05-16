package logging

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseLevel__knownLevels(t *testing.T) {
	cases := []struct {
		in   string
		want slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"DEBUG", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"error", slog.LevelError},
		{"  Error ", slog.LevelError},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			// act
			got, err := ParseLevel(c.in)

			// assert
			require.NoError(t, err)
			assert.Equal(t, c.want, got)
		})
	}
}

func TestParseLevel__empty__defaultsToInfo(t *testing.T) {
	// act
	got, err := ParseLevel("")

	// assert
	require.NoError(t, err)
	assert.Equal(t, slog.LevelInfo, got)
}

func TestParseLevel__unknown__returnsError(t *testing.T) {
	// act
	_, err := ParseLevel("trace")

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown level")
}

func TestResolveFilePath__tildeExpanded(t *testing.T) {
	// arrange
	home := t.TempDir()

	// act
	got, err := ResolveFilePath("~/logs/kafka-tui.log", home)

	// assert
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(home, "logs", "kafka-tui.log"), got)
}

func TestResolveFilePath__envPlaceholder(t *testing.T) {
	// arrange
	t.Setenv("KAFKA_TUI_LOG_DIR", "/var/log/kt")

	// act
	got, err := ResolveFilePath("${env:KAFKA_TUI_LOG_DIR}/app.log", "")

	// assert
	require.NoError(t, err)
	assert.Equal(t, "/var/log/kt/app.log", got)
}

func TestResolveFilePath__envWithDefault__usesDefault(t *testing.T) {
	// arrange — variable explicitly unset, restored after the test
	unsetEnv(t, "KAFKA_TUI_NOT_SET")

	// act
	got, err := ResolveFilePath("${env:KAFKA_TUI_NOT_SET:-/tmp/fallback.log}", "")

	// assert
	require.NoError(t, err)
	assert.Equal(t, "/tmp/fallback.log", got)
}

func TestResolveFilePath__envWithDefault__envWins(t *testing.T) {
	// arrange
	t.Setenv("KAFKA_TUI_X", "/from/env")

	// act
	got, err := ResolveFilePath("${env:KAFKA_TUI_X:-/fallback}", "")

	// assert
	require.NoError(t, err)
	assert.Equal(t, "/from/env", got)
}

func TestResolveFilePath__missingEnvNoDefault__returnsError(t *testing.T) {
	// arrange
	unsetEnv(t, "KAFKA_TUI_REALLY_MISSING")

	// act
	_, err := ResolveFilePath("${env:KAFKA_TUI_REALLY_MISSING}/app.log", "")

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KAFKA_TUI_REALLY_MISSING")
}

func TestResolveFilePath__multiplePlaceholdersAndTilde(t *testing.T) {
	// arrange
	home := t.TempDir()
	t.Setenv("APP", "kafka-tui")
	t.Setenv("ENV", "prod")

	// act
	got, err := ResolveFilePath("~/logs/${env:APP}/${env:ENV}.log", home)

	// assert
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(home, "logs", "kafka-tui", "prod.log"), got)
}

func TestResolveFilePath__tildeUserNotSupported(t *testing.T) {
	// act
	_, err := ResolveFilePath("~root/log", "/h")

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "~/")
}

func TestResolveFilePath__empty__returnsEmpty(t *testing.T) {
	// act
	got, err := ResolveFilePath("", "/h")

	// assert
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestInit__createsFileAndWritesAtConfiguredLevel(t *testing.T) {
	// arrange
	dir := t.TempDir()
	logPath := filepath.Join(dir, "sub", "kafka-tui.log")

	// act
	lg, err := Init(Options{
		Level:     "warn",
		File:      logPath,
		MaxSizeMB: 1,
		MaxFiles:  3,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = lg.Close() })

	lg.Logger.Debug("debug-line")
	lg.Logger.Info("info-line")
	lg.Logger.Warn("warn-line")
	lg.Logger.Error("error-line")
	require.NoError(t, lg.Close())

	// assert
	assert.Equal(t, logPath, lg.ResolvedAt)
	data, readErr := os.ReadFile(logPath) //nolint:gosec // test-controlled path
	require.NoError(t, readErr)
	body := string(data)
	assert.NotContains(t, body, "debug-line")
	assert.NotContains(t, body, "info-line")
	assert.Contains(t, body, "warn-line")
	assert.Contains(t, body, "error-line")
}

// Regression: log files can contain broker addresses, cluster names,
// and any context written at debug. The default 0o644 file mode let
// other accounts on shared hosts read those logs.
func TestInit__restrictsFilePermissions(t *testing.T) {
	// arrange
	dir := t.TempDir()
	logPath := filepath.Join(dir, "logs", "kafka-tui.log")

	// act
	lg, err := Init(Options{Level: "info", File: logPath})
	require.NoError(t, err)
	t.Cleanup(func() { _ = lg.Close() })

	// assert — parent dir 0o700, log file 0o600.
	dirInfo, err := os.Stat(filepath.Dir(logPath))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o700), dirInfo.Mode().Perm(),
		"log directory must be user-private (0o700)")

	fileInfo, err := os.Stat(logPath)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), fileInfo.Mode().Perm(),
		"log file must be user-private (0o600)")
}

func TestInit__defaultRotationParams(t *testing.T) {
	// arrange
	dir := t.TempDir()
	logPath := filepath.Join(dir, "k.log")

	// act
	lg, err := Init(Options{Level: "info", File: logPath})
	require.NoError(t, err)
	t.Cleanup(func() { _ = lg.Close() })

	// assert — writer is created with a non-nil rotating writer and resolved path matches.
	assert.Equal(t, logPath, lg.ResolvedAt)
	require.NotNil(t, lg.Writer)
	rw, ok := lg.Writer.(*RotatingWriter)
	require.True(t, ok, "writer should be *RotatingWriter")
	assert.Equal(t, int64(DefaultMaxSizeMB)*1024*1024, rw.maxBytes)
	assert.Equal(t, DefaultMaxFiles, rw.maxFiles)
}

func TestInit__invalidLevel__error(t *testing.T) {
	// arrange
	dir := t.TempDir()

	// act
	_, err := Init(Options{Level: "verbose", File: filepath.Join(dir, "log")})

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown level")
}

func TestInit__emptyFile__error(t *testing.T) {
	// act
	_, err := Init(Options{Level: "info", File: ""})

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty log file path")
}

func TestRotatingWriter__rotatesWhenSizeExceeded(t *testing.T) {
	// arrange
	dir := t.TempDir()
	path := filepath.Join(dir, "rot.log")
	rw, err := NewRotatingWriter(path, 1, 3) // 1 MB threshold
	require.NoError(t, err)
	t.Cleanup(func() { _ = rw.Close() })

	// rotation trigger is "current size + new payload would exceed maxBytes",
	// so we shrink maxBytes for the test to keep payloads modest.
	rw.maxBytes = 50

	// act — write enough to trigger several rotations
	payload := []byte(strings.Repeat("x", 30) + "\n") // 31 bytes
	for range 5 {
		_, writeErr := rw.Write(payload)
		require.NoError(t, writeErr)
	}
	require.NoError(t, rw.Close())

	// assert — main file plus archives
	_, err = os.Stat(path)
	require.NoError(t, err)
	_, err = os.Stat(path + ".1")
	require.NoError(t, err)
}

func TestRotatingWriter__keepsAtMostMaxFilesArchives(t *testing.T) {
	// arrange
	dir := t.TempDir()
	path := filepath.Join(dir, "rot.log")
	rw, err := NewRotatingWriter(path, 1, 2)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rw.Close() })
	rw.maxBytes = 30

	payload := []byte(strings.Repeat("y", 25) + "\n") // 26 bytes

	// act — 6 writes => 5 rotations
	for range 6 {
		_, writeErr := rw.Write(payload)
		require.NoError(t, writeErr)
	}
	require.NoError(t, rw.Close())

	// assert — only .1 and .2 should exist (older are pruned)
	_, err = os.Stat(path + ".1")
	require.NoError(t, err)
	_, err = os.Stat(path + ".2")
	require.NoError(t, err)
	_, err = os.Stat(path + ".3")
	assert.True(t, os.IsNotExist(err))
}

func TestRotatingWriter__pruneStaleArchives(t *testing.T) {
	// arrange — pre-existing higher-numbered archive on disk
	dir := t.TempDir()
	path := filepath.Join(dir, "rot.log")
	require.NoError(t, os.WriteFile(path+".5", []byte("old"), 0o644))

	rw, err := NewRotatingWriter(path, 1, 2)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rw.Close() })
	rw.maxBytes = 30

	// act — trigger one rotation so prune runs
	for range 2 {
		_, writeErr := rw.Write([]byte(strings.Repeat("z", 25) + "\n"))
		require.NoError(t, writeErr)
	}
	require.NoError(t, rw.Close())

	// assert
	_, err = os.Stat(path + ".5")
	assert.True(t, os.IsNotExist(err), "stale archive should be pruned")
}

func TestNewRotatingWriter__invalidParams(t *testing.T) {
	// arrange
	dir := t.TempDir()
	path := filepath.Join(dir, "rot.log")

	// act + assert
	_, err := NewRotatingWriter(path, 0, 3)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max_size_mb")

	_, err = NewRotatingWriter(path, 1, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max_files")
}

func TestOpenInPager__missingFile__error(t *testing.T) {
	// act
	err := OpenInPager(context.Background(), filepath.Join(t.TempDir(), "no-such-file.log"))

	// assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestOpenInPager__usesPagerEnv(t *testing.T) {
	// arrange — fake pager script that writes its argument into a marker file.
	dir := t.TempDir()
	logFile := filepath.Join(dir, "out.log")
	require.NoError(t, os.WriteFile(logFile, []byte("hello\n"), 0o644))

	marker := filepath.Join(dir, "marker.txt")
	pager := filepath.Join(dir, "fake-pager.sh")
	script := "#!/bin/sh\necho \"$1\" > " + marker + "\n"
	require.NoError(t, os.WriteFile(pager, []byte(script), 0o755))
	t.Setenv("PAGER", pager)

	// act
	err := OpenInPager(context.Background(), logFile)

	// assert
	require.NoError(t, err)
	got, readErr := os.ReadFile(marker) //nolint:gosec // test-controlled path
	require.NoError(t, readErr)
	assert.Equal(t, logFile+"\n", string(got))
}

// unsetEnv removes an env var for the duration of the test, capturing the
// original value (if any) and restoring it during t.Cleanup. Avoids leaving
// processes-wide env state mutated when the test ends.
func unsetEnv(t *testing.T, name string) {
	t.Helper()
	prev, had := os.LookupEnv(name)
	require.NoError(t, os.Unsetenv(name))
	t.Cleanup(func() {
		if had {
			_ = os.Setenv(name, prev)
		} else {
			_ = os.Unsetenv(name)
		}
	})
}
