package config

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// DefaultDebounce is the time the watcher waits after the last filesystem
// event before triggering a reload.
const DefaultDebounce = 500 * time.Millisecond

// Snapshot is a reload result emitted by Watcher.
type Snapshot struct {
	// Loaded is the new merged config when the reload succeeded; nil otherwise.
	Loaded *Loaded
	// Err is non-nil when the reload failed.
	Err error
	// ActiveClusterChanged is true when the active cluster's fields differ
	// from the previous snapshot. The TUI uses this to surface a warning
	// toast advising the user to reconnect.
	ActiveClusterChanged bool
}

// Watcher monitors the contributing config files (the four config slots plus
// every path referenced by a ${file:...} placeholder) and emits Snapshots on
// change. Filesystem events are debounced to coalesce editor saves.
type Watcher struct {
	opts          LoaderOptions
	activeCluster string
	debounce      time.Duration

	fsw       *fsnotify.Watcher
	snapshots chan Snapshot

	last         *Loaded
	watchedDirs  map[string]struct{}
	trackedFiles map[string]struct{}

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewWatcher loads the configuration once and starts watching every file
// that contributed to the result. The initial Loaded value is returned so
// the caller can wire it into the application; subsequent reloads are
// delivered on the channel returned by Snapshots.
//
// activeCluster, when non-empty, enables ActiveClusterChanged detection. A
// debounce of zero falls back to DefaultDebounce.
func NewWatcher(opts LoaderOptions, activeCluster string, debounce time.Duration) (*Watcher, *Loaded, error) {
	if debounce <= 0 {
		debounce = DefaultDebounce
	}
	initial, err := Load(opts)
	if err != nil {
		return nil, nil, err
	}
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, nil, fmt.Errorf("config: create fs watcher: %w", err)
	}
	w := &Watcher{
		opts:          opts,
		activeCluster: activeCluster,
		debounce:      debounce,
		fsw:           fsw,
		snapshots:     make(chan Snapshot, 1),
		last:          initial,
		watchedDirs:   map[string]struct{}{},
		trackedFiles:  map[string]struct{}{},
	}
	if err := w.refreshWatchedFiles(); err != nil {
		_ = fsw.Close()
		return nil, nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	w.wg.Add(1)
	go w.run(ctx)
	return w, initial, nil
}

// Snapshots returns the channel on which reload events are delivered. The
// channel is closed when the watcher is closed.
func (w *Watcher) Snapshots() <-chan Snapshot { return w.snapshots }

// Close stops the watcher's background goroutine and releases the underlying
// fsnotify resources. The Snapshots channel is closed once the goroutine
// exits.
func (w *Watcher) Close() error {
	w.cancel()
	err := w.fsw.Close()
	w.wg.Wait()
	if err != nil {
		return fmt.Errorf("config: close fs watcher: %w", err)
	}
	return nil
}

func (w *Watcher) run(ctx context.Context) {
	defer w.wg.Done()
	defer close(w.snapshots)

	var timer *time.Timer
	var timerC <-chan time.Time
	stopTimer := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer = nil
		timerC = nil
	}
	scheduleReload := func() {
		if timer == nil {
			timer = time.NewTimer(w.debounce)
			timerC = timer.C
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(w.debounce)
	}

	for {
		select {
		case <-ctx.Done():
			stopTimer()
			return
		case ev, ok := <-w.fsw.Events:
			if !ok {
				stopTimer()
				return
			}
			if !w.isTracked(ev.Name) {
				continue
			}
			scheduleReload()
		case _, ok := <-w.fsw.Errors:
			if !ok {
				stopTimer()
				return
			}
		case <-timerC:
			timer = nil
			timerC = nil
			w.reload(ctx)
		}
	}
}

func (w *Watcher) reload(ctx context.Context) {
	loaded, err := Load(w.opts)
	if err != nil {
		w.send(ctx, Snapshot{Err: err})
		return
	}
	snap := Snapshot{Loaded: loaded}
	if w.activeCluster != "" {
		snap.ActiveClusterChanged = activeClusterChanged(w.activeCluster, w.last, loaded)
	}
	w.last = loaded
	// best-effort: track newly referenced files; ignore errors so a transient
	// permission glitch on a placeholder file does not silence reloads.
	_ = w.refreshWatchedFiles()
	w.send(ctx, snap)
}

func (w *Watcher) send(ctx context.Context, snap Snapshot) {
	select {
	case w.snapshots <- snap:
	case <-ctx.Done():
	}
}

// isTracked reports whether name (the file path reported by fsnotify) is one
// of the files we care about.
func (w *Watcher) isTracked(name string) bool {
	abs, err := filepath.Abs(name)
	if err != nil {
		return false
	}
	_, ok := w.trackedFiles[abs]
	return ok
}

// refreshWatchedFiles recomputes the set of files we want to be notified
// about and updates the underlying fsnotify watcher accordingly. We watch
// parent directories rather than the files themselves so that editors that
// save by rename (atomic write) keep producing events, and so a not-yet-
// existing file becomes observable as soon as it appears.
func (w *Watcher) refreshWatchedFiles() error {
	files, err := w.contributingPaths()
	if err != nil {
		return err
	}
	newDirs := map[string]struct{}{}
	newFiles := map[string]struct{}{}
	for _, p := range files {
		abs, err := filepath.Abs(p)
		if err != nil {
			continue
		}
		newFiles[abs] = struct{}{}
		newDirs[filepath.Dir(abs)] = struct{}{}
	}
	for d := range w.watchedDirs {
		if _, keep := newDirs[d]; keep {
			continue
		}
		_ = w.fsw.Remove(d)
	}
	for d := range newDirs {
		if _, already := w.watchedDirs[d]; already {
			continue
		}
		if _, statErr := os.Stat(d); statErr != nil {
			continue
		}
		if addErr := w.fsw.Add(d); addErr != nil {
			continue
		}
	}
	w.watchedDirs = newDirs
	w.trackedFiles = newFiles
	return nil
}

// contributingPaths returns absolute or as-given paths for every file that
// can affect the merged config: the four hierarchy slots (or the explicit
// override) plus every path referenced by a ${file:...} placeholder in any
// of those YAML files.
func (w *Watcher) contributingPaths() ([]string, error) {
	configFiles, clustersFiles, err := resolveFilePaths(w.opts)
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(configFiles)+len(clustersFiles))
	for _, f := range configFiles {
		paths = append(paths, f.Path)
	}
	for _, f := range clustersFiles {
		paths = append(paths, f.Path)
	}
	seen := map[string]struct{}{}
	for _, p := range paths {
		seen[p] = struct{}{}
	}
	for _, p := range append([]string{}, paths...) {
		data, err := os.ReadFile(p) //nolint:gosec // p comes from the config hierarchy
		if err != nil {
			continue
		}
		for _, fp := range discoverFilePlaceholders(data) {
			if _, ok := seen[fp]; ok {
				continue
			}
			seen[fp] = struct{}{}
			paths = append(paths, fp)
		}
	}
	return paths, nil
}

var filePlaceholderRe = regexp.MustCompile(`\$\{file:([^}]+)\}`)

// discoverFilePlaceholders scans raw YAML bytes for every ${file:...}
// placeholder body. The body is the path verbatim — it mirrors what the
// placeholder resolver passes to its FileReader.
func discoverFilePlaceholders(content []byte) []string {
	matches := filePlaceholderRe.FindAllSubmatch(content, -1)
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		out = append(out, string(m[1]))
	}
	return out
}

// activeClusterChanged returns true when the cluster named name has any
// field difference between prev and next. A cluster appearing only in one
// of the two snapshots also counts as a change.
func activeClusterChanged(name string, prev, next *Loaded) bool {
	p := findClusterByName(prev, name)
	n := findClusterByName(next, name)
	if p == nil && n == nil {
		return false
	}
	if p == nil || n == nil {
		return true
	}
	return !reflect.DeepEqual(*p, *n)
}

func findClusterByName(l *Loaded, name string) *Cluster {
	if l == nil {
		return nil
	}
	for i := range l.Clusters {
		if l.Clusters[i].Name == name {
			return &l.Clusters[i]
		}
	}
	return nil
}
