// Package logging configures slog-based file logging for kafka-tui with
// size-based rotation, level selection, and a path-resolution helper that
// expands `~` and `${env:VAR}` / `${env:VAR:-default}` placeholders.
//
// The full placeholder pipeline (file/vault) lives in the config package
// (Tasks 4-5). For the log-file path, env+`~` is sufficient.
package logging

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	// DefaultMaxSizeMB is the rotation threshold when not configured.
	DefaultMaxSizeMB = 10
	// DefaultMaxFiles is the number of rotated files to keep when not configured.
	DefaultMaxFiles = 5
	// DefaultLevel is used when the configured level is empty.
	DefaultLevel = "info"
)

// Options configures Init.
type Options struct {
	// Level is one of "debug", "info", "warn", "error" (case-insensitive).
	// Empty defaults to DefaultLevel.
	Level string
	// File is the destination path. Supports `~` and `${env:VAR}` placeholders.
	File string
	// MaxSizeMB is the rotation threshold; 0 falls back to DefaultMaxSizeMB.
	MaxSizeMB int
	// MaxFiles is the number of rotated archives kept; 0 falls back to DefaultMaxFiles.
	MaxFiles int
	// HomeDir overrides $HOME for `~` expansion (used in tests).
	HomeDir string
}

// Logger bundles the slog.Logger with the underlying writer so callers can
// flush/close at shutdown.
type Logger struct {
	Logger     *slog.Logger
	Writer     io.WriteCloser
	ResolvedAt string
}

// Close flushes and closes the underlying writer.
func (l *Logger) Close() error {
	if l == nil || l.Writer == nil {
		return nil
	}
	if err := l.Writer.Close(); err != nil {
		return fmt.Errorf("logging: close writer: %w", err)
	}
	return nil
}

// Init opens (creating parent dirs as needed) the log file and returns a
// configured *Logger. The returned Logger is also installed as slog.Default.
func Init(opts Options) (*Logger, error) {
	level, err := ParseLevel(opts.Level)
	if err != nil {
		return nil, err
	}

	resolved, err := ResolveFilePath(opts.File, opts.HomeDir)
	if err != nil {
		return nil, err
	}
	if resolved == "" {
		return nil, errors.New("logging: empty log file path")
	}

	maxSize := opts.MaxSizeMB
	if maxSize <= 0 {
		maxSize = DefaultMaxSizeMB
	}
	maxFiles := opts.MaxFiles
	if maxFiles <= 0 {
		maxFiles = DefaultMaxFiles
	}

	if mkErr := os.MkdirAll(filepath.Dir(resolved), 0o755); mkErr != nil {
		return nil, fmt.Errorf("logging: create log dir: %w", mkErr)
	}

	w, err := NewRotatingWriter(resolved, maxSize, maxFiles)
	if err != nil {
		return nil, err
	}

	handler := slog.NewTextHandler(w, &slog.HandlerOptions{Level: level})
	logger := slog.New(handler)
	slog.SetDefault(logger)

	return &Logger{Logger: logger, Writer: w, ResolvedAt: resolved}, nil
}

// ParseLevel maps a textual level into slog.Level. Empty falls back to DefaultLevel.
func ParseLevel(s string) (slog.Level, error) {
	if s == "" {
		s = DefaultLevel
	}
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("logging: unknown level %q (allowed: debug, info, warn, error)", s)
	}
}

// envPlaceholder matches `${env:NAME}` and `${env:NAME:-default}`.
// `:-default` may itself contain any character except a closing brace.
var envPlaceholder = regexp.MustCompile(`\$\{env:([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}`)

// ResolveFilePath expands a leading `~` (using homeDir or $HOME) and any
// `${env:VAR}` / `${env:VAR:-default}` placeholders inside path.
//
// An unresolved placeholder without a default returns an error.
func ResolveFilePath(path, homeDir string) (string, error) {
	if path == "" {
		return "", nil
	}

	expanded, err := expandEnvPlaceholders(path)
	if err != nil {
		return "", err
	}
	return expandHome(expanded, homeDir)
}

func expandEnvPlaceholders(s string) (string, error) {
	var firstErr error
	out := envPlaceholder.ReplaceAllStringFunc(s, func(match string) string {
		groups := envPlaceholder.FindStringSubmatch(match)
		name := groups[1]
		hasDefault := strings.Contains(match, ":-")
		def := groups[2]

		val, present := os.LookupEnv(name)
		switch {
		case present:
			return val
		case hasDefault:
			return def
		default:
			if firstErr == nil {
				firstErr = fmt.Errorf("logging: env var %q is not set and has no default", name)
			}
			return match
		}
	})
	if firstErr != nil {
		return "", firstErr
	}
	return out, nil
}

func expandHome(path, homeDir string) (string, error) {
	if path == "" || path[0] != '~' {
		return path, nil
	}
	if len(path) > 1 && path[1] != '/' {
		return "", fmt.Errorf("logging: %q: only ~/ is supported, not ~user/", path)
	}
	if homeDir == "" {
		h, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("logging: cannot resolve home dir: %w", err)
		}
		homeDir = h
	}
	return filepath.Join(homeDir, path[1:]), nil
}

// OpenInPager opens path in $PAGER (or `less -R` as a fallback). It blocks
// until the pager exits and wires stdin/stdout/stderr through.
//
// If path does not exist, a clear error is returned so the CLI can print
// a friendly message.
func OpenInPager(ctx context.Context, path string) error {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("logging: log file not found at %s", path)
		}
		return fmt.Errorf("logging: stat %s: %w", path, err)
	}

	pagerCmd := os.Getenv("PAGER")
	if strings.TrimSpace(pagerCmd) == "" {
		pagerCmd = "less -R"
	}

	parts := strings.Fields(pagerCmd)
	if len(parts) == 0 {
		return errors.New("logging: PAGER is empty after parsing")
	}
	args := make([]string, 0, len(parts))
	args = append(args, parts[1:]...)
	args = append(args, path)
	cmd := exec.CommandContext(ctx, parts[0], args...) //nolint:gosec // PAGER is intentionally user-controlled.
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("logging: pager %q failed: %w", parts[0], err)
	}
	return nil
}
