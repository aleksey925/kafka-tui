package components

import (
	"strings"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
)

// Viewport is the shared bounded-scrollable region used everywhere content
// can exceed its allotted space: form textareas / lists, the message detail
// viewer, the log tail. One scroll keymap, one clamp implementation, one
// place to fix bugs.
//
// The caller supplies pre-styled lines (ANSI styling already baked in). The
// viewport knows nothing about content origin — it slices a window, optionally
// wraps lines to width, and (when given a cursor line) keeps that line in
// view. Streaming sources call AppendLines; if the user is parked at the
// bottom the window auto-follows the tail, otherwise it stays put so the user
// can read what they scrolled to.
//
// Wrap mode is the default — hardwrap by character via [ansi.Hardwrap], which
// preserves ANSI sequences across breaks. With wrap off, lines are truncated
// to width with the horizontal scroll offset honored first via
// [ansi.TruncateLeft], matching the legacy messages-detail viewer.
type Viewport struct {
	width, height int

	lines      []string
	cursorLine int // -1 = no cursor

	scrollTop int
	hScroll   int

	wrap       bool
	followTail bool
}

// NewViewport returns a viewport with wrap on, no cursor, no follow-tail.
func NewViewport() *Viewport {
	return &Viewport{
		cursorLine: -1,
		wrap:       true,
	}
}

func (v *Viewport) SetSize(w, h int) {
	v.width, v.height = w, h
	v.clamp()
}

func (v *Viewport) Size() (int, int) { return v.width, v.height }

// SetLines replaces the content. ScrollTop and hScroll are clamped to the
// new content size; the caller is responsible for resetting them explicitly
// (e.g. via Reset / ScrollToTop) if a content swap should jump back to the
// top.
func (v *Viewport) SetLines(lines []string) {
	v.lines = lines
	v.clamp()
}

// AppendLines extends the buffer. If follow-tail is enabled and the viewport
// was at the bottom before the append, the window slides to keep showing the
// newly arrived content. If the user had scrolled up, the new lines stack
// silently and the user keeps their reading position — standard `tail -f`
// semantics.
func (v *Viewport) AppendLines(lines []string) {
	wasAtBottom := v.IsAtBottom()
	v.lines = append(v.lines, lines...)
	if v.followTail && wasAtBottom {
		v.ScrollToBottom()
		return
	}
	v.clamp()
}

func (v *Viewport) Lines() []string { return v.lines }

func (v *Viewport) TotalLines() int { return len(v.lines) }

// SetCursor pins a line as "the cursor" and scrolls so it is visible.
// Use -1 (or ClearCursor) when there is no cursor (read-only viewer).
func (v *Viewport) SetCursor(line int) {
	v.cursorLine = line
	v.EnsureCursorVisible()
}

func (v *Viewport) ClearCursor() { v.cursorLine = -1 }

func (v *Viewport) Cursor() int { return v.cursorLine }

func (v *Viewport) SetWrap(on bool) {
	if v.wrap == on {
		return
	}
	v.wrap = on
	if on {
		// hScroll is meaningless once wrap is on; reset so toggling back off
		// doesn't restore a stale offset that points past the new geometry.
		v.hScroll = 0
	}
	v.clamp()
}

func (v *Viewport) Wrap() bool { return v.wrap }

func (v *Viewport) SetFollowTail(on bool) { v.followTail = on }

func (v *Viewport) FollowTail() bool { return v.followTail }

func (v *Viewport) ScrollOffset() int { return v.scrollTop }

func (v *Viewport) HScrollOffset() int { return v.hScroll }

// IsAtBottom reports whether the last line is in the visible window.
// True when content fits entirely, true when scrolled all the way down.
func (v *Viewport) IsAtBottom() bool {
	if v.height <= 0 || len(v.lines) == 0 {
		return true
	}
	if len(v.lines) <= v.height {
		return true
	}
	return v.scrollTop >= len(v.lines)-v.height
}

func (v *Viewport) ScrollBy(delta int) {
	v.scrollTop += delta
	v.clamp()
}

func (v *Viewport) HScrollBy(delta int) {
	if v.wrap {
		return
	}
	v.hScroll += delta
	v.clamp()
}

func (v *Viewport) PageDown() { v.ScrollBy(v.PageStep()) }

func (v *Viewport) PageUp() { v.ScrollBy(-v.PageStep()) }

func (v *Viewport) ScrollToTop() {
	v.scrollTop = 0
	v.clamp()
}

func (v *Viewport) ScrollToBottom() {
	if len(v.lines) <= v.height || v.height <= 0 {
		v.scrollTop = 0
		return
	}
	v.scrollTop = len(v.lines) - v.height
}

// EnsureCursorVisible nudges scrollTop so cursorLine falls inside the
// visible window. No-op when the cursor is unset or geometry is unknown.
func (v *Viewport) EnsureCursorVisible() {
	if v.cursorLine < 0 || v.height <= 0 || len(v.lines) == 0 {
		return
	}
	if v.cursorLine < v.scrollTop {
		v.scrollTop = v.cursorLine
	}
	if v.cursorLine >= v.scrollTop+v.height {
		v.scrollTop = v.cursorLine - v.height + 1
	}
	v.clamp()
}

// Reset clears scroll state. Content, size, wrap, follow-tail
// and cursor are preserved — callers can clear those explicitly if needed.
func (v *Viewport) Reset() {
	v.scrollTop = 0
	v.hScroll = 0
}

// HandleKey processes the shared viewport keymap and returns true when a
// key was consumed: j/k vertical, ctrl+b/f and pgup/pgdn for pages,
// home/end for jumps, h/l for hScroll (when wrap off), w toggles wrap.
func (v *Viewport) HandleKey(key tea.KeyPressMsg) bool {
	s := key.String()
	switch s {
	case "j", "down":
		v.ScrollBy(+1)
		return true
	case "k", "up":
		v.ScrollBy(-1)
		return true
	case "ctrl+f", "pgdown":
		v.PageDown()
		return true
	case "ctrl+b", "pgup":
		v.PageUp()
		return true
	case "end":
		v.ScrollToBottom()
		return true
	case "home":
		v.ScrollToTop()
		return true
	case "h", "left":
		if !v.wrap {
			v.HScrollBy(-v.HStep())
			return true
		}
	case "l", "right":
		if !v.wrap {
			v.HScrollBy(+v.HStep())
			return true
		}
	case "w":
		v.SetWrap(!v.wrap)
		return true
	}
	return false
}

// View renders the visible window. With wrap on, returns the slice as-is —
// content is assumed already wrapped to width. With wrap off, each visible
// line is truncated to width after applying hScroll.
func (v *Viewport) View() string {
	if v.height <= 0 || len(v.lines) == 0 {
		return ""
	}
	start := v.scrollTop
	end := min(start+v.height, len(v.lines))
	if start >= end {
		return ""
	}
	visible := v.lines[start:end]
	if v.wrap || v.width <= 0 {
		return strings.Join(visible, "\n")
	}
	out := make([]string, len(visible))
	for i, line := range visible {
		s := line
		if v.hScroll > 0 {
			s = ansi.TruncateLeft(s, v.hScroll, "")
		}
		s = ansi.Truncate(s, v.width, "")
		out[i] = s
	}
	return strings.Join(out, "\n")
}

// PageStep is the vertical jump used by PageDown / PageUp — one screenful
// minus one line so the user keeps a row of context across the boundary.
// Exposed for screens that build their own scroll commands instead of using
// HandleKey.
func (v *Viewport) PageStep() int {
	if v.height <= 1 {
		return 1
	}
	return v.height - 1
}

// HStep is the horizontal jump used by HScrollBy bindings — a quarter of
// the viewport width, with a floor of 1 cell so narrow terminals can still
// pan one column at a time.
func (v *Viewport) HStep() int {
	if v.width <= 4 {
		return 1
	}
	return v.width / 4
}

func (v *Viewport) clamp() {
	if v.scrollTop < 0 {
		v.scrollTop = 0
	}
	if v.height > 0 && len(v.lines) > v.height {
		if v.scrollTop > len(v.lines)-v.height {
			v.scrollTop = len(v.lines) - v.height
		}
	} else {
		v.scrollTop = 0
	}
	if v.hScroll < 0 {
		v.hScroll = 0
	}
	// hScroll==0 is the common case (wrap mode, or no panning yet); skip the
	// O(N) StringWidth scan over every line for it. Large log buffers (10k+
	// lines) clamp on every append; the scan would dominate.
	if v.hScroll > 0 && v.width > 0 {
		maxW := maxLineWidth(v.lines)
		if maxW > 0 && v.hScroll > maxW-v.width {
			if maxW > v.width {
				v.hScroll = maxW - v.width
			} else {
				v.hScroll = 0
			}
		}
	}
}

func maxLineWidth(lines []string) int {
	m := 0
	for _, line := range lines {
		if w := ansi.StringWidth(line); w > m {
			m = w
		}
	}
	return m
}

// WrapLines hardwraps each input line to width, producing the flat visual
// line list a Viewport expects in wrap mode. Empty lines stay empty (one
// visual line). ANSI sequences in the input are preserved across breaks.
// width <= 0 returns the input unchanged.
func WrapLines(lines []string, width int) []string {
	if width <= 0 {
		return lines
	}
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		if line == "" {
			out = append(out, "")
			continue
		}
		wrapped := ansi.Hardwrap(line, width, false)
		out = append(out, strings.Split(wrapped, "\n")...)
	}
	return out
}

// CursorVisualLine maps a logical (lineIdx, col) cursor — where col is a
// rune offset inside the logical line — to the visual line index produced
// by [WrapLines] (or unchanged logical lines when wrap is off). It is the
// inverse of the WrapLines transformation for cursor positioning, so a
// caller can pass the result to [Viewport.SetCursor] without re-deriving
// the visual line manually.
func CursorVisualLine(logicalLines []string, lineIdx, col, width int, wrap bool) int {
	if lineIdx < 0 {
		return 0
	}
	if !wrap || width <= 0 {
		if lineIdx >= len(logicalLines) {
			return max(len(logicalLines)-1, 0)
		}
		return lineIdx
	}
	// count visual lines for everything BEFORE the cursor's logical line,
	// then add the cursor's own offset within its wrapped logical line.
	visual := 0
	for i := 0; i < lineIdx && i < len(logicalLines); i++ {
		if logicalLines[i] == "" {
			visual++
			continue
		}
		wrapped := ansi.Hardwrap(logicalLines[i], width, false)
		visual += strings.Count(wrapped, "\n") + 1
	}
	if lineIdx >= len(logicalLines) {
		return visual
	}
	line := logicalLines[lineIdx]
	if line == "" || col <= 0 {
		return visual
	}
	runes := []rune(line)
	if col > len(runes) {
		col = len(runes)
	}
	prefix := string(runes[:col])
	wrapped := ansi.Hardwrap(prefix, width, false)
	visualOffset := strings.Count(wrapped, "\n")
	// the cursor character (or its trailing-column virtual cell when col ==
	// len) is rendered AT position col. If the wrapped prefix ends flush
	// against the width boundary that character actually shows on the next
	// visual line, not at the trailing column of the current one — without
	// this bump the viewport scrolls to the wrong line on each wrap boundary.
	lastLine := wrapped
	if i := strings.LastIndex(wrapped, "\n"); i >= 0 {
		lastLine = wrapped[i+1:]
	}
	if ansi.StringWidth(lastLine) >= width {
		visualOffset++
	}
	return visual + visualOffset
}
