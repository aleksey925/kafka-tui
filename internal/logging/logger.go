// Package logging configures slog-based file logging with size-based rotation
// and `~` / `${env:VAR}` path expansion.
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
	DefaultMaxSizeMB = 10
	DefaultMaxFiles  = 5
	DefaultLevel     = "info"
)

// Options configures Init. File supports `~` and `${env:VAR}` placeholders.
// Zero MaxSizeMB / MaxFiles fall back to defaults.
type Options struct {
	Level     string
	File      string
	MaxSizeMB int
	MaxFiles  int
	HomeDir   string
}

// Logger bundles the slog.Logger with the underlying writer so callers can
// flush/close at shutdown.
type Logger struct {
	Logger     *slog.Logger
	Writer     io.WriteCloser
	ResolvedAt string
}

func (l *Logger) Close() error {
	if l == nil || l.Writer == nil {
		return nil
	}
	if err := l.Writer.Close(); err != nil {
		return fmt.Errorf("logging: close writer: %w", err)
	}
	return nil
}

// Init opens the log file (creating parent dirs as needed). Init does not
// touch slog's global default so test isolation is preserved — callers that
// want it global must call slog.SetDefault themselves.
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

	// 0o700 keeps the log directory user-private (logs themselves are
	// opened 0o600 in RotatingWriter.open).
	if mkErr := os.MkdirAll(filepath.Dir(resolved), 0o700); mkErr != nil {
		return nil, fmt.Errorf("logging: create log dir: %w", mkErr)
	}

	w, err := NewRotatingWriter(resolved, maxSize, maxFiles)
	if err != nil {
		return nil, err
	}

	handler := slog.NewTextHandler(w, &slog.HandlerOptions{Level: level})
	logger := slog.New(handler)

	return &Logger{Logger: logger, Writer: w, ResolvedAt: resolved}, nil
}

// ParseLevel maps a canonical level string into slog.Level. Empty →
// DefaultLevel. Input is expected to already be normalized (lowercase,
// trimmed) by config.PostProcessConfig — this parser is strict and only
// matches the canonical set.
func ParseLevel(s string) (slog.Level, error) {
	if s == "" {
		s = DefaultLevel
	}
	switch s {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
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

// ResolveFilePath expands a leading `~` and any `${env:VAR}` /
// `${env:VAR:-default}` placeholders. Unresolved placeholders without a
// default return an error.
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
// until the pager exits and wires stdin/stdout/stderr through. A missing
// file returns a clear error so the CLI can print a friendly message.
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
