// Package logs implements the `:logs` viewer screen — a paginated, searchable
// view of the configured log file with optional follow-mode (tail -f) and
// color-coded level tags (DEBUG, INFO, WARN, ERROR).
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
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// DefaultFollowInterval is how often the model polls the log file for new
// content while follow-mode is active.
const DefaultFollowInterval = 500 * time.Millisecond

// Action describes the screen's pending intent for the host (router).
type Action struct {
	// Back signals the user pressed esc/q on the list view.
	Back bool
}

// Options configure a [Model].
type Options struct {
	// Path is the absolute path of the log file to display. Required.
	Path string
	// FollowInterval, when > 0, overrides DefaultFollowInterval.
	FollowInterval time.Duration
	// Now is the injected clock (defaults to time.Now). Used only for the
	// toast queue today; reserved for future timestamp formatting.
	Now func() time.Time
	// Styles overrides the theme palette (mostly for tests).
	Styles theme.Styles
	// MaxLines caps how many lines are kept in memory at once. Older lines
	// are dropped when the buffer overflows. Zero means "unlimited".
	MaxLines int
}

// Model is the logs viewer screen.
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
	viewport int

	// search state. mirrors the table component, but operates over the raw
	// lines slice rather than table rows.
	searchActive bool
	search       string
	matches      []int
	matchCursor  int

	gPrimed bool

	width, height int

	toasts *components.Toasts
	action Action
	styles theme.Styles
	now    func() time.Time
}

// New constructs a Model.
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
		styles:         styles,
		now:            now,
	}
}

// Init dispatches the initial file load.
func (m *Model) Init() tea.Cmd {
	return loadCmd(m.path, 0)
}

// Action returns the current pending action.
func (m *Model) Action() Action { return m.action }

// ConsumeAction returns the pending action and clears it.
func (m *Model) ConsumeAction() Action {
	a := m.action
	m.action = Action{}
	return a
}

// Path returns the log file path the screen is bound to.
func (m *Model) Path() string { return m.path }

// Lines returns the loaded lines (defensive copy) for tests.
func (m *Model) Lines() []string {
	out := make([]string, len(m.lines))
	copy(out, m.lines)
	return out
}

// Following reports whether follow-mode is on (for tests).
func (m *Model) Following() bool { return m.follow }

// Missing reports whether the log file is missing (for tests).
func (m *Model) Missing() bool { return m.missing }

// Cursor returns the current cursor index.
func (m *Model) Cursor() int { return m.cursor }

// Toasts exposes the toast queue (for tests).
func (m *Model) Toasts() *components.Toasts { return m.toasts }

// SetSize updates width/height.
func (m *Model) SetSize(w, h int) {
	m.width, m.height = w, h
	m.clampViewport()
}

// KeyHints returns the screen-specific hints.
func (m *Model) KeyHints() []layout.KeyHint {
	hint := "f"
	label := "follow"
	if m.follow {
		hint = "f"
		label = "stop follow"
	}
	return []layout.KeyHint{
		{Key: hint, Label: label},
		{Key: "/", Label: "search"},
		{Key: "n/N", Label: "next/prev match"},
		{Key: "gg/G", Label: "top/bottom"},
		{Key: "esc/q", Label: "back"},
	}
}

// Update routes messages.
func (m *Model) Update(msg tea.Msg) (*Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.SetSize(msg.Width, msg.Height)
		return m, nil
	case LoadedMsg:
		m.handleLoaded(msg)
		return m, nil
	case AppendedMsg:
		cmd := m.handleAppended(msg)
		return m, cmd
	case FollowTickMsg:
		cmd := m.handleFollowTick()
		return m, cmd
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

func (m *Model) handleKey(key tea.KeyPressMsg) (*Model, tea.Cmd) {
	if m.toasts != nil {
		_, _ = m.toasts.Update(key)
	}
	if m.searchActive {
		m.handleSearchKey(key)
		return m, nil
	}
	if m.gPrimed {
		m.gPrimed = false
		if key.String() == "g" {
			m.cursor = 0
			m.clampViewport()
			return m, nil
		}
		// otherwise fall through; treat the second key normally
	}
	switch key.String() {
	case "esc", "q":
		m.action.Back = true
		return m, nil
	case "f":
		cmd := m.toggleFollow()
		return m, cmd
	case "/":
		m.searchActive = true
		m.search = ""
		return m, nil
	case "n":
		m.jumpMatch(+1)
		return m, nil
	case "N":
		m.jumpMatch(-1)
		return m, nil
	case "j", "down":
		m.move(+1)
		return m, nil
	case "k", "up":
		m.move(-1)
		return m, nil
	case "ctrl+d":
		m.move(+m.pageStep())
		return m, nil
	case "ctrl+u":
		m.move(-m.pageStep())
		return m, nil
	case "g":
		m.gPrimed = true
		return m, nil
	case "G":
		m.cursor = max(0, len(m.lines)-1)
		m.clampViewport()
		return m, nil
	}
	return m, nil
}

func (m *Model) handleSearchKey(key tea.KeyPressMsg) {
	switch key.String() {
	case "esc":
		m.searchActive = false
		m.search = ""
		m.matches = nil
		m.matchCursor = 0
	case "enter":
		m.searchActive = false
		m.recomputeMatches()
		if len(m.matches) > 0 {
			m.cursor = m.matches[0]
			m.matchCursor = 0
			m.clampViewport()
		}
	case "backspace":
		if n := len(m.search); n > 0 {
			m.search = m.search[:n-1]
		}
	default:
		if t := key.Text; t != "" {
			m.search += t
		}
	}
}

func (m *Model) move(delta int) {
	if len(m.lines) == 0 {
		m.cursor = 0
		return
	}
	m.cursor = clamp(m.cursor+delta, 0, len(m.lines)-1)
	m.clampViewport()
}

func (m *Model) pageStep() int {
	h := m.bodyHeight()
	if h > 1 {
		return h / 2
	}
	return 5
}

func (m *Model) jumpMatch(direction int) {
	if len(m.matches) == 0 {
		return
	}
	m.matchCursor = (m.matchCursor + direction + len(m.matches)) % len(m.matches)
	m.cursor = m.matches[m.matchCursor]
	m.clampViewport()
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
	// reserve one line each for header, search line, key hints.
	body := m.height - 4
	if body < 1 {
		return 1
	}
	return body
}

func (m *Model) clampViewport() {
	if len(m.lines) == 0 {
		m.cursor = 0
		m.viewport = 0
		return
	}
	if m.cursor < 0 {
		m.cursor = 0
	}
	if m.cursor > len(m.lines)-1 {
		m.cursor = len(m.lines) - 1
	}
	h := m.bodyHeight()
	if h <= 0 || h >= len(m.lines) {
		m.viewport = 0
		return
	}
	if m.cursor < m.viewport {
		m.viewport = m.cursor
	}
	if m.cursor >= m.viewport+h {
		m.viewport = m.cursor - h + 1
	}
	if m.viewport < 0 {
		m.viewport = 0
	}
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

func (m *Model) handleLoaded(msg LoadedMsg) {
	if msg.Missing {
		m.missing = true
		m.lines = nil
		m.readOff = 0
		return
	}
	m.missing = false
	if msg.Err != nil {
		m.loadErr = msg.Err.Error()
		m.toasts.Push(components.ToastError, "load logs: "+msg.Err.Error())
		return
	}
	m.loadErr = ""
	m.lines = msg.Lines
	m.readOff = msg.NextOffset
	m.trimLines()
	m.cursor = max(0, len(m.lines)-1)
	m.recomputeMatches()
	m.clampViewport()
}

func (m *Model) handleAppended(msg AppendedMsg) tea.Cmd {
	if msg.Err != nil {
		m.toasts.Push(components.ToastError, "tail: "+msg.Err.Error())
		// keep follow loop alive — transient errors should not stop the screen
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
		m.clampViewport()
	}
	if msg.Truncated {
		// the underlying file shrank (rotation): restart from the beginning
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

// View renders the screen body.
func (m *Model) View() string {
	parts := []string{m.headerLine()}
	if m.missing {
		parts = append(parts, m.styles.StatusWarn.Render("No log file found at "+m.path))
	} else {
		parts = append(parts, m.renderBody())
	}
	if m.searchActive || m.search != "" {
		parts = append(parts, m.renderSearchLine())
	}
	if t := m.toasts.View(); t != "" {
		parts = append(parts, t)
	}
	return strings.Join(parts, "\n")
}

func (m *Model) headerLine() string {
	body := fmt.Sprintf("logs · %s · %d lines", m.path, len(m.lines))
	if m.follow {
		body += "  " + m.styles.HintKey.Render("● LIVE")
	}
	if m.loadErr != "" {
		body += "  " + m.styles.StatusErr.Render("error: "+m.loadErr)
	}
	return m.styles.StatusInfo.Render(body)
}

func (m *Model) renderBody() string {
	if len(m.lines) == 0 {
		return m.styles.StatusInfo.Render("(empty)")
	}
	h := m.bodyHeight()
	start := m.viewport
	end := start + h
	if h <= 0 || end > len(m.lines) {
		end = len(m.lines)
	}
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

// colorizeLevel finds the first level token (DEBUG/INFO/WARN/ERROR) in the
// line and renders it with the matching theme color. If none match, the line
// is returned as-is. Detection is case-sensitive — slog text handlers emit
// uppercase levels.
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

// detectLevel returns the first level tag found in s (DEBUG, INFO, WARN, or
// ERROR) along with its byte index. Tokens are matched only when bounded by
// non-letter characters on both sides — so "infos" or "errored" do not match.
// Returns ("", -1) when no level is present.
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

func (m *Model) renderSearchLine() string {
	prefix := m.styles.CommandHL.Render("/")
	body := prefix + m.styles.Command.Render(m.search)
	if !m.searchActive && len(m.matches) > 0 {
		body += "  " + m.styles.StatusInfo.Render(
			fmt.Sprintf("[%d/%d]", m.matchCursor+1, len(m.matches)),
		)
	} else if !m.searchActive && m.search != "" {
		body += "  " + m.styles.StatusWarn.Render("no matches")
	}
	return body
}

// ----- Messages -----

// LoadedMsg is dispatched on initial load (or after rotation truncation).
type LoadedMsg struct {
	Lines      []string
	NextOffset int64
	Missing    bool
	Err        error
}

// AppendedMsg is dispatched after a follow-tick yields new bytes.
//
// Truncated is true when the underlying file shrank (log rotation): the host
// should treat the offset as invalid and trigger a fresh load.
type AppendedMsg struct {
	Lines      []string
	NextOffset int64
	Truncated  bool
	Err        error
}

// FollowTickMsg is the periodic poll while follow-mode is active.
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
			// log rotation: file shrank
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

// readAll reads from offset to EOF and returns the lines plus the new offset.
// The trailing fragment without a final newline is included as its own line so
// no content is lost between consecutive reads. Lines never carry trailing
// newlines.
func readAll(path string, off int64) ([]string, int64, error) {
	f, err := os.Open(path) //nolint:gosec // path is from user-supplied config.
	if err != nil {
		return nil, off, fmt.Errorf("logs: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	// stat BEFORE scanning so writes that arrive while we read don't bump the
	// reported next-offset past content we never observed (those bytes get
	// picked up on the next tick instead).
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
