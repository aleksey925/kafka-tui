// Package logs implements the `:logs` viewer screen.
package logs

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/help"
	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

const DefaultFollowInterval = 500 * time.Millisecond

type Action struct {
	Back bool
}

type Options struct {
	Path           string
	FollowInterval time.Duration
	Now            func() time.Time
	Styles         theme.Styles
	// MaxLines caps the in-memory buffer. Zero means unlimited.
	MaxLines int
}

type Model struct {
	path           string
	followInterval time.Duration
	maxLines       int

	lines    []string
	missing  bool
	loadErr  string
	readOff  int64
	follow   bool
	cursor   int
	viewport *components.Viewport

	search      string
	matches     []int
	matchCursor int

	width, height int

	toasts *components.Toasts
	action Action
	styles theme.Styles
	now    func() time.Time
}

func New(opts Options) *Model {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	styles := opts.Styles
	if styles.Palette.Foreground == nil {
		styles = theme.DefaultStyles()
	}
	interval := opts.FollowInterval
	if interval <= 0 {
		interval = DefaultFollowInterval
	}
	return &Model{
		path:           opts.Path,
		followInterval: interval,
		maxLines:       opts.MaxLines,
		toasts:         components.NewToasts(components.WithToastClock(now), components.WithToastStyles(styles)),
		viewport:       components.NewViewport(),
		styles:         styles,
		now:            now,
	}
}

func (m *Model) Init() tea.Cmd {
	return loadCmd(m.path, 0)
}

func (m *Model) Action() Action { return m.action }

func (m *Model) ConsumeAction() Action {
	a := m.action
	m.action = Action{}
	return a
}

func (m *Model) Path() string { return m.path }

func (m *Model) Lines() []string {
	out := make([]string, len(m.lines))
	copy(out, m.lines)
	return out
}

func (m *Model) Following() bool { return m.follow }

func (m *Model) Missing() bool { return m.missing }

func (m *Model) Cursor() int { return m.cursor }

func (m *Model) Toasts() *components.Toasts { return m.toasts }

func (m *Model) LatestFlash() (components.Toast, bool) {
	if m.toasts == nil {
		return components.Toast{}, false
	}
	return m.toasts.Latest()
}

func (m *Model) Title() string {
	body := fmt.Sprintf("Logs · %d lines", len(m.lines))
	if m.search != "" {
		// match the host-wide search-marker convention used by every
		// other screen (topics / clusters / messages / groups): the
		// trailing query is wrapped in `</…>` so users learn one shape.
		body = fmt.Sprintf("Logs · %d matches / %d lines </%s>", len(m.matches), len(m.lines), m.search)
	}
	if m.follow {
		body += " ● LIVE"
	}
	return body
}

func (m *Model) Breadcrumb() string { return m.path }

// SetSearch rebuilds match indices and jumps to the first match so the user
// sees the result of each keystroke live.
func (m *Model) SetSearch(query string) {
	m.search = query
	m.recomputeMatches()
	m.matchCursor = 0
	if len(m.matches) > 0 {
		m.cursor = m.matches[0]
		m.syncViewport()
	}
}

func (m *Model) ActiveFilter() string { return m.search }

func (m *Model) SetSize(w, h int) {
	m.width, m.height = w, h
	m.syncViewport()
}

// syncViewport pushes the current lines/cursor/size into the bounded scroller.
// SetCursor calls EnsureCursorVisible, so the scrollTop tracks the selected
// row automatically — the legacy clampViewport logic now lives in there.
func (m *Model) syncViewport() {
	m.viewport.SetSize(m.width, m.bodyHeight())
	m.viewport.SetLines(m.lines)
	if len(m.lines) == 0 {
		m.viewport.ClearCursor()
		return
	}
	m.viewport.SetCursor(m.cursor)
}

func (m *Model) KeyHints() []layout.KeyHint {
	return layout.HintsFromBindings(m.bindings())
}

func (m *Model) HelpSections() []help.Section {
	return help.SectionsFromBindings(m.bindings())
}

func (m *Model) bindings() []keymap.Binding {
	followLabel := "follow tail"
	if m.follow {
		followLabel = "stop follow"
	}
	return []keymap.Binding{
		{Keys: []string{"f"}, Label: followLabel, Category: "Logs", Hint: true, Handler: m.toggleFollow},
		{Keys: []string{"n"}, Label: "next match", Category: "Search", Hint: true, Handler: m.actNextMatch},
		{Keys: []string{"N"}, Label: "previous match", Category: "Search", Hint: true, Handler: m.actPrevMatch},
		{Keys: []string{"j", "down"}, Label: "scroll down", Category: "Movement", Handler: m.actMoveDown},
		{Keys: []string{"k", "up"}, Label: "scroll up", Category: "Movement", Handler: m.actMoveUp},
		{Keys: []string{"ctrl+f", "pgdown"}, Label: "page down", Category: "Movement", Handler: m.actPageDown},
		{Keys: []string{"ctrl+b", "pgup"}, Label: "page up", Category: "Movement", Handler: m.actPageUp},
		{Keys: []string{"home"}, Label: "scroll to top", Category: "Movement", Hint: true, Handler: m.actScrollTop},
		{Keys: []string{"end"}, Label: "scroll to bottom", Category: "Movement", Hint: true, Handler: m.actScrollBottom},
		{Keys: []string{"esc", "q"}, Label: "back", Category: "Logs", Handler: m.actBack},
		{Keys: []string{"/"}, Label: "filter lines", Category: "Search", Hint: true},
	}
}

func (m *Model) actNextMatch() tea.Cmd { m.jumpMatch(+1); return nil }
func (m *Model) actPrevMatch() tea.Cmd { m.jumpMatch(-1); return nil }
func (m *Model) actBack() tea.Cmd      { m.action.Back = true; return nil }
func (m *Model) actMoveDown() tea.Cmd  { m.move(+1); return nil }
func (m *Model) actMoveUp() tea.Cmd    { m.move(-1); return nil }
func (m *Model) actPageDown() tea.Cmd  { m.move(+m.pageStep()); return nil }
func (m *Model) actPageUp() tea.Cmd    { m.move(-m.pageStep()); return nil }
func (m *Model) actScrollBottom() tea.Cmd {
	m.cursor = max(0, len(m.lines)-1)
	m.syncViewport()
	return nil
}

func (m *Model) actScrollTop() tea.Cmd {
	m.cursor = 0
	m.syncViewport()
	return nil
}

func (m *Model) Update(msg tea.Msg) tea.Cmd {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.SetSize(msg.Width, msg.Height)
		return nil
	case LoadedMsg:
		return m.handleLoaded(msg)
	case AppendedMsg:
		return m.handleAppended(msg)
	case FollowTickMsg:
		return m.handleFollowTick()
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	}
	return nil
}

func (m *Model) handleKey(key tea.KeyPressMsg) tea.Cmd {
	if m.toasts != nil {
		_, _ = m.toasts.Update(key)
	}
	if cmd, ok := keymap.Dispatch(m.bindings(), key); ok {
		return cmd
	}
	return nil
}

func (m *Model) move(delta int) {
	if len(m.lines) == 0 {
		m.cursor = 0
		return
	}
	m.cursor = clamp(m.cursor+delta, 0, len(m.lines)-1)
	m.syncViewport()
}

func (m *Model) pageStep() int {
	h := m.bodyHeight()
	if h > 1 {
		return h - 1
	}
	return 1
}

func (m *Model) jumpMatch(direction int) {
	if len(m.matches) == 0 {
		return
	}
	m.matchCursor = (m.matchCursor + direction + len(m.matches)) % len(m.matches)
	m.cursor = m.matches[m.matchCursor]
	m.syncViewport()
}

func (m *Model) recomputeMatches() {
	m.matches = m.matches[:0]
	if m.search == "" {
		return
	}
	needle := strings.ToLower(m.search)
	for i, line := range m.lines {
		if strings.Contains(strings.ToLower(line), needle) {
			m.matches = append(m.matches, i)
		}
	}
}

func (m *Model) bodyHeight() int {
	if m.height <= 0 {
		return len(m.lines)
	}
	body := m.height - 4
	if body < 1 {
		return 1
	}
	return body
}

func (m *Model) toggleFollow() tea.Cmd {
	m.follow = !m.follow
	if m.follow {
		m.toasts.Push(components.ToastInfo, "follow mode on")
		return tickCmd(m.followInterval)
	}
	m.toasts.Push(components.ToastInfo, "follow mode off")
	return nil
}

func (m *Model) handleLoaded(msg LoadedMsg) tea.Cmd {
	if msg.Missing {
		m.missing = true
		m.lines = nil
		m.readOff = 0
		return m.followTick()
	}
	m.missing = false
	if msg.Err != nil {
		m.loadErr = msg.Err.Error()
		m.toasts.Push(components.ToastError, "load logs: "+msg.Err.Error())
		return m.followTick()
	}
	m.loadErr = ""
	// detect rotation: a Truncated AppendedMsg triggers a fresh loadCmd
	// while m.lines still holds the pre-rotation tail. on the very first
	// cold load m.lines is nil, so we treat that as initial-open.
	isFirstLoad := m.lines == nil
	prevCursor := m.cursor
	m.lines = msg.Lines
	m.readOff = msg.NextOffset
	m.trimLines()
	switch {
	case isFirstLoad, m.follow:
		// initial open and follow-mode want the tail. anything else
		// preserves the user's reading position across reloads.
		m.cursor = max(0, len(m.lines)-1)
	default:
		m.cursor = clamp(prevCursor, 0, max(0, len(m.lines)-1))
	}
	m.recomputeMatches()
	m.syncViewport()
	return m.followTick()
}

// followTick schedules the next tail tick when follow mode is on. Centralized
// so every load/append/error path keeps the tick chain alive — a missing
// schedule would silently break LIVE mode after rotation or transient errors.
func (m *Model) followTick() tea.Cmd {
	if !m.follow {
		return nil
	}
	return tickCmd(m.followInterval)
}

func (m *Model) handleAppended(msg AppendedMsg) tea.Cmd {
	if msg.Err != nil {
		m.toasts.Push(components.ToastError, "tail: "+msg.Err.Error())
		// keep follow loop alive on transient errors.
		return tickCmd(m.followInterval)
	}
	if len(msg.Lines) > 0 {
		atBottom := m.cursor >= len(m.lines)-1
		m.lines = append(m.lines, msg.Lines...)
		m.trimLines()
		if atBottom {
			m.cursor = len(m.lines) - 1
		}
		m.recomputeMatches()
		m.syncViewport()
	}
	if msg.Truncated {
		// log rotation: restart from the beginning
		m.readOff = msg.NextOffset
		return loadCmd(m.path, 0)
	}
	m.readOff = msg.NextOffset
	if !m.follow {
		return nil
	}
	return tickCmd(m.followInterval)
}

func (m *Model) handleFollowTick() tea.Cmd {
	if !m.follow {
		return nil
	}
	return appendCmd(m.path, m.readOff)
}

func (m *Model) trimLines() {
	if m.maxLines <= 0 || len(m.lines) <= m.maxLines {
		return
	}
	drop := len(m.lines) - m.maxLines
	m.lines = m.lines[drop:]
	if m.cursor >= drop {
		m.cursor -= drop
	} else {
		m.cursor = 0
	}
}

func (m *Model) View() string {
	var parts []string
	if m.missing {
		parts = append(parts, m.styles.StatusWarn.Render("No log file found at "+m.path))
	} else {
		parts = append(parts, m.renderBody())
	}
	// search prompt is owned by the host; matches surface in the frame title.
	return strings.Join(parts, "\n")
}

func (m *Model) renderBody() string {
	if len(m.lines) == 0 {
		return m.styles.StatusInfo.Render("(empty)")
	}
	// sync first so scroll position reflects any cursor moves since the last
	// frame; renderBody is the single consumer of viewport state in logs.
	m.syncViewport()
	h := m.bodyHeight()
	start := m.viewport.ScrollOffset()
	end := min(start+h, len(m.lines))
	out := make([]string, 0, end-start)
	for i := start; i < end; i++ {
		out = append(out, m.renderLine(i))
	}
	return strings.Join(out, "\n")
}

func (m *Model) renderLine(idx int) string {
	line := m.lines[idx]
	rendered := m.colorizeLevel(line)
	prefix := "  "
	if idx == m.cursor {
		prefix = m.styles.HintKey.Render("> ")
	}
	return prefix + rendered
}

// colorizeLevel finds the first level token and applies a theme color.
// Case-sensitive — slog text handlers emit uppercase levels.
func (m *Model) colorizeLevel(line string) string {
	tag, idx := detectLevel(line)
	if idx < 0 {
		return line
	}
	style := m.levelStyle(tag)
	return line[:idx] + style.Render(tag) + line[idx+len(tag):]
}

func (m *Model) levelStyle(tag string) lipgloss.Style {
	switch tag {
	case "ERROR":
		return lipgloss.NewStyle().Foreground(m.styles.Palette.StatusError).Bold(true)
	case "WARN":
		return lipgloss.NewStyle().Foreground(m.styles.Palette.StatusWarn).Bold(true)
	case "DEBUG":
		return lipgloss.NewStyle().Foreground(m.styles.Palette.Muted)
	case "INFO":
		return lipgloss.NewStyle().Foreground(m.styles.Palette.Foreground)
	default:
		return lipgloss.NewStyle()
	}
}

// detectLevel matches level tokens bounded by non-letters on both sides
// (so "infos" / "errored" do not match). Returns ("", -1) on no match.
func detectLevel(s string) (string, int) {
	tags := []string{"ERROR", "WARN", "DEBUG", "INFO"}
	bestIdx := -1
	bestTag := ""
	for _, t := range tags {
		if i := indexBoundary(s, t); i >= 0 {
			if bestIdx < 0 || i < bestIdx {
				bestIdx = i
				bestTag = t
			}
		}
	}
	return bestTag, bestIdx
}

func indexBoundary(s, sub string) int {
	off := 0
	for {
		i := strings.Index(s[off:], sub)
		if i < 0 {
			return -1
		}
		abs := off + i
		left := abs == 0 || !isLetter(s[abs-1])
		right := abs+len(sub) == len(s) || !isLetter(s[abs+len(sub)])
		if left && right {
			return abs
		}
		off = abs + 1
		if off >= len(s) {
			return -1
		}
	}
}

func isLetter(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

// ----- Messages -----

type LoadedMsg struct {
	Lines      []string
	NextOffset int64
	Missing    bool
	Err        error
}

// AppendedMsg.Truncated is true when the underlying file shrank (rotation):
// the host should treat the offset as invalid and trigger a fresh load.
type AppendedMsg struct {
	Lines      []string
	NextOffset int64
	Truncated  bool
	Err        error
}

type FollowTickMsg struct{}

func loadCmd(path string, off int64) tea.Cmd {
	return func() tea.Msg {
		lines, next, err := readAll(path, off)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return LoadedMsg{Missing: true}
			}
			return LoadedMsg{Err: err}
		}
		return LoadedMsg{Lines: lines, NextOffset: next}
	}
}

func appendCmd(path string, from int64) tea.Cmd {
	return func() tea.Msg {
		info, err := os.Stat(path)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return AppendedMsg{Truncated: true, NextOffset: 0}
			}
			return AppendedMsg{Err: err}
		}
		if info.Size() < from {
			return AppendedMsg{Truncated: true, NextOffset: 0}
		}
		if info.Size() == from {
			return AppendedMsg{NextOffset: from}
		}
		lines, next, err := readAll(path, from)
		if err != nil {
			return AppendedMsg{Err: err}
		}
		return AppendedMsg{Lines: lines, NextOffset: next}
	}
}

func tickCmd(d time.Duration) tea.Cmd {
	return tea.Tick(d, func(time.Time) tea.Msg {
		return FollowTickMsg{}
	})
}

// readAll reads from offset to EOF; trailing fragments without a final
// newline are included so no content is lost between consecutive reads.
func readAll(path string, off int64) ([]string, int64, error) {
	f, err := os.Open(path) //nolint:gosec // path is from user-supplied config.
	if err != nil {
		return nil, off, fmt.Errorf("logs: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	// stat BEFORE scanning so writes that arrive while we read don't bump the
	// reported next-offset past content we never observed.
	info, err := f.Stat()
	if err != nil {
		return nil, off, fmt.Errorf("logs: stat %s: %w", path, err)
	}
	size := info.Size()
	if size <= off {
		return nil, size, nil
	}
	if off > 0 {
		if _, seekErr := f.Seek(off, io.SeekStart); seekErr != nil {
			return nil, off, fmt.Errorf("logs: seek %s: %w", path, seekErr)
		}
	}
	var lines []string
	scanner := bufio.NewScanner(io.LimitReader(f, size-off))
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if scanErr := scanner.Err(); scanErr != nil {
		return nil, off, fmt.Errorf("logs: read %s: %w", path, scanErr)
	}
	return lines, size, nil
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
