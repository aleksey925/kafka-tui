package logging

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// RotatingWriter writes to a file and rotates it when the byte count would
// exceed maxBytes. Archives are `<path>.1`, `<path>.2`, ...; archives beyond
// maxFiles are pruned. Write/Close are safe to call from multiple goroutines.
type RotatingWriter struct {
	path     string
	maxBytes int64
	maxFiles int

	mu   sync.Mutex
	file *os.File
	size int64
}

// NewRotatingWriter opens (or creates) path. maxSizeMB must be >0; maxFiles
// is the number of archives kept (>=1).
func NewRotatingWriter(path string, maxSizeMB, maxFiles int) (*RotatingWriter, error) {
	if maxSizeMB <= 0 {
		return nil, fmt.Errorf("logging: max_size_mb must be > 0 (got %d)", maxSizeMB)
	}
	if maxFiles < 1 {
		return nil, fmt.Errorf("logging: max_files must be >= 1 (got %d)", maxFiles)
	}

	w := &RotatingWriter{
		path:     path,
		maxBytes: int64(maxSizeMB) * 1024 * 1024,
		maxFiles: maxFiles,
	}
	if err := w.open(); err != nil {
		return nil, err
	}
	return w, nil
}

// Write rotates before writing when the existing file size plus len(p)
// would exceed maxBytes. A single payload larger than maxBytes is still
// written in full to keep individual log records intact.
func (w *RotatingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil && w.size > 0 && w.size+int64(len(p)) > w.maxBytes {
		if err := w.rotateLocked(); err != nil {
			return 0, err
		}
	}
	if w.file == nil {
		if err := w.open(); err != nil {
			return 0, err
		}
	}

	n, err := w.file.Write(p)
	w.size += int64(n)
	if err != nil {
		return n, fmt.Errorf("logging: write: %w", err)
	}
	return n, nil
}

func (w *RotatingWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	w.size = 0
	if err != nil {
		return fmt.Errorf("logging: close: %w", err)
	}
	return nil
}

func (w *RotatingWriter) open() error {
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("logging: open %s: %w", w.path, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("logging: stat %s: %w", w.path, err)
	}
	w.file = f
	w.size = info.Size()
	return nil
}

func (w *RotatingWriter) rotateLocked() error {
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return fmt.Errorf("logging: close before rotate: %w", err)
		}
		w.file = nil
		w.size = 0
	}

	// shift archives: .N-1 -> .N, .N-2 -> .N-1, ..., .1 -> .2; oldest dropped.
	for i := w.maxFiles - 1; i >= 1; i-- {
		src := fmt.Sprintf("%s.%d", w.path, i)
		dst := fmt.Sprintf("%s.%d", w.path, i+1)
		if i == w.maxFiles-1 {
			// the file at this position would shift past the cap — remove it.
			_ = os.Remove(dst)
		}
		if err := renameIfExists(src, dst); err != nil {
			return err
		}
	}
	if err := renameIfExists(w.path, w.path+".1"); err != nil {
		return err
	}
	if err := pruneArchives(w.path, w.maxFiles); err != nil {
		return err
	}
	return w.open()
}

func renameIfExists(src, dst string) error {
	if _, err := os.Stat(src); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("logging: stat %s: %w", src, err)
	}
	if err := os.Rename(src, dst); err != nil {
		return fmt.Errorf("logging: rename %s -> %s: %w", src, dst, err)
	}
	return nil
}

// pruneArchives removes any `.N` archives where N > maxFiles. Guards against
// pre-existing archives created with a higher max_files setting.
func pruneArchives(base string, maxFiles int) error {
	dir, name := splitBase(base)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("logging: read dir %s: %w", dir, err)
	}
	prefix := name + "."
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), prefix) {
			continue
		}
		idx, ok := parseArchiveIndex(e.Name(), prefix)
		if !ok {
			continue
		}
		if idx > maxFiles {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
	return nil
}

func splitBase(base string) (dir, name string) {
	for i := len(base) - 1; i >= 0; i-- {
		if base[i] == os.PathSeparator {
			return base[:i], base[i+1:]
		}
	}
	return ".", base
}

func parseArchiveIndex(filename, prefix string) (int, bool) {
	suffix := strings.TrimPrefix(filename, prefix)
	if suffix == filename {
		return 0, false
	}
	n, err := strconv.Atoi(suffix)
	if err != nil || n < 1 {
		return 0, false
	}
	return n, true
}
