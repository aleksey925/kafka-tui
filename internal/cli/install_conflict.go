package cli

import (
	"fmt"
	"os"
	"path/filepath"
)

const binaryName = "kafka-tui"

// resolvedExecPath returns the canonical path of the running binary, following
// symlinks so a symlinked install and its target are recognized as one entry.
func resolvedExecPath() (string, error) {
	execPath, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("get executable path: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(execPath)
	if err != nil {
		return "", fmt.Errorf("resolve executable path: %w", err)
	}
	return resolved, nil
}

// duplicateBinaries returns canonical paths of every other kafka-tui executable
// found on PATH, excluding the currently running one.
func duplicateBinaries(current string) []string {
	entries := filepath.SplitList(os.Getenv("PATH"))
	seen := map[string]bool{current: true}
	dups := make([]string, 0, len(entries))
	for _, dir := range entries {
		if dir == "" {
			continue
		}
		resolved, err := filepath.EvalSymlinks(filepath.Join(dir, binaryName))
		if err != nil || seen[resolved] {
			continue
		}
		seen[resolved] = true
		dups = append(dups, resolved)
	}
	return dups
}

// WarnDuplicateInstalls prints a warning when several kafka-tui binaries shadow
// each other on PATH, since the one that runs then depends on PATH order — a
// brew install next to a leftover ~/.local/bin copy is the common case.
func WarnDuplicateInstalls() {
	current, err := resolvedExecPath()
	if err != nil {
		return
	}
	dups := duplicateBinaries(current)
	if len(dups) == 0 {
		return
	}
	_, _ = fmt.Fprintln(os.Stderr, "\nWarning: multiple kafka-tui binaries found on PATH (the one that runs depends on PATH order):")
	_, _ = fmt.Fprintf(os.Stderr, "  running:      %s\n", current)
	for _, d := range dups {
		_, _ = fmt.Fprintf(os.Stderr, "  also on PATH: %s\n", d)
	}
	_, _ = fmt.Fprintln(os.Stderr, "Keep only one to avoid version confusion.")
}
