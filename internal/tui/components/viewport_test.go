package components_test

import (
	"strings"
	"testing"

	"charm.land/lipgloss/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
)

func TestViewport_View_ReturnsVisibleWindow(t *testing.T) {
	v := components.NewViewport()
	v.SetSize(40, 3)
	v.SetLines([]string{"a", "b", "c", "d", "e"})

	got := strings.Split(v.View(), "\n")
	assert.Equal(t, []string{"a", "b", "c"}, got)
}

func TestViewport_ScrollDown_AdvancesWindow(t *testing.T) {
	v := components.NewViewport()
	v.SetSize(40, 3)
	v.SetLines([]string{"a", "b", "c", "d", "e"})

	v.ScrollBy(+1)

	assert.Equal(t, []string{"b", "c", "d"}, strings.Split(v.View(), "\n"))
}

func TestViewport_PageDown_StopsAtBottom(t *testing.T) {
	v := components.NewViewport()
	v.SetSize(40, 3)
	v.SetLines([]string{"a", "b", "c", "d", "e"})

	v.PageDown()

	// pageStep = height-1 = 2, so first page lands at scrollTop=2, window is c/d/e.
	assert.Equal(t, []string{"c", "d", "e"}, strings.Split(v.View(), "\n"))
	assert.True(t, v.IsAtBottom())
}

func TestViewport_ScrollBy_ClampsToBounds(t *testing.T) {
	v := components.NewViewport()
	v.SetSize(40, 3)
	v.SetLines([]string{"a", "b", "c", "d", "e"})

	v.ScrollBy(-100)
	assert.Equal(t, 0, v.ScrollOffset(), "negative scroll clamps to 0")

	v.ScrollBy(+100)
	assert.Equal(t, 2, v.ScrollOffset(), "over-scroll clamps to totalLines-height")
}

func TestViewport_SetCursor_ScrollsBelowToReveal(t *testing.T) {
	v := components.NewViewport()
	v.SetSize(40, 3)
	v.SetLines([]string{"l0", "l1", "l2", "l3", "l4", "l5"})

	v.SetCursor(5)

	// height=3, cursor at line 5 → window must end at line 5 → scrollTop=3
	assert.Equal(t, 3, v.ScrollOffset())
}

func TestViewport_SetCursor_ScrollsAboveToReveal(t *testing.T) {
	v := components.NewViewport()
	v.SetSize(40, 3)
	v.SetLines([]string{"l0", "l1", "l2", "l3", "l4", "l5"})
	v.ScrollBy(+3) // window now [3,6)
	require.Equal(t, 3, v.ScrollOffset())

	v.SetCursor(0)

	assert.Equal(t, 0, v.ScrollOffset(), "cursor above window must pull scroll up")
}

func TestViewport_FollowTail_AppendSlidesWhenAtBottom(t *testing.T) {
	v := components.NewViewport()
	v.SetSize(40, 3)
	v.SetFollowTail(true)
	v.SetLines([]string{"a", "b", "c"})
	require.True(t, v.IsAtBottom())

	v.AppendLines([]string{"d", "e"})

	assert.True(t, v.IsAtBottom(), "follow-tail must keep the window at the new bottom")
	assert.Equal(t, []string{"c", "d", "e"}, strings.Split(v.View(), "\n"))
}

func TestViewport_AppendLines_DoesNotAutoScrollWithoutFollowTail(t *testing.T) {
	// follow-tail is opt-in: a viewer that didn't ask for it keeps its
	// scroll position when content grows underneath, even when the user
	// happens to be sitting at the bottom.
	v := components.NewViewport()
	v.SetSize(40, 3)
	v.SetLines([]string{"a", "b", "c"})
	require.True(t, v.IsAtBottom())

	v.AppendLines([]string{"d", "e"})

	assert.Equal(t, 0, v.ScrollOffset(),
		"without SetFollowTail(true), append must not slide the window")
	assert.Equal(t, []string{"a", "b", "c"}, strings.Split(v.View(), "\n"))
}

func TestViewport_FollowTail_DoesNotDisturbScrolledReader(t *testing.T) {
	v := components.NewViewport()
	v.SetSize(40, 3)
	v.SetFollowTail(true)
	v.SetLines([]string{"a", "b", "c", "d", "e"})
	v.ScrollToTop()
	require.Equal(t, 0, v.ScrollOffset())

	v.AppendLines([]string{"f", "g"})

	assert.Equal(t, 0, v.ScrollOffset(), "user scrolled up — appended lines must not yank the view")
	assert.Equal(t, []string{"a", "b", "c"}, strings.Split(v.View(), "\n"))
}

func TestViewport_Reset_ClearsScrollState(t *testing.T) {
	v := components.NewViewport()
	v.SetSize(40, 3)
	v.SetLines([]string{"a", "b", "c", "d", "e"})
	v.ScrollBy(+2)
	require.Equal(t, 2, v.ScrollOffset())

	v.Reset()

	assert.Equal(t, 0, v.ScrollOffset())
	assert.Equal(t, []string{"a", "b", "c"}, strings.Split(v.View(), "\n"), "content survives Reset")
}

func TestViewport_HScrollBy_NoOpWhenWrapOn(t *testing.T) {
	// horizontal scroll is meaningless once wrap is on (every visual line
	// already fits the width by definition). Guarding here prevents stale
	// hScroll from leaking back when the user later toggles wrap off.
	v := components.NewViewport()
	v.SetSize(10, 3)
	v.SetLines([]string{"abcdefghij"})
	require.True(t, v.Wrap())

	v.HScrollBy(+5)

	assert.Equal(t, 0, v.HScrollOffset(), "HScrollBy must no-op while wrap is on")
}

func TestViewport_ClearCursor_DisablesFollowAndIsObservable(t *testing.T) {
	v := components.NewViewport()
	v.SetSize(10, 3)
	v.SetLines([]string{"a", "b", "c", "d", "e"})
	v.SetCursor(4)
	require.Equal(t, 4, v.Cursor())

	v.ClearCursor()

	assert.Equal(t, -1, v.Cursor())
	// EnsureCursorVisible must be a no-op once the cursor is cleared.
	v.ScrollToTop()
	v.EnsureCursorVisible()
	assert.Equal(t, 0, v.ScrollOffset(), "cleared cursor must not yank scroll back")
}

func TestViewport_NoWrap_HScroll_TruncatesAndShifts(t *testing.T) {
	v := components.NewViewport()
	v.SetSize(5, 1)
	v.SetWrap(false)
	v.SetLines([]string{"abcdefghij"})

	assert.Equal(t, "abcde", v.View(), "wrap off: line truncates to width")

	v.HScrollBy(+3)
	assert.Equal(t, "defgh", v.View(), "hScroll shifts the visible slice")
}

func TestViewport_Wrap_OnByDefault(t *testing.T) {
	v := components.NewViewport()
	assert.True(t, v.Wrap(), "wrap is on by default — matches the most useful mode for editing/reading")
}

func TestViewport_CursorHighlight_SurvivesHScroll(t *testing.T) {
	// arrange — ansi.TruncateLeft strips an opening SGR placed before the
	// panned window, so the highlight must be applied after truncation.
	v := components.NewViewport()
	v.SetSize(5, 2)
	v.SetWrap(false)
	v.SetLines([]string{"abcdefghij", "klmnopqrst"})
	v.SetCursor(1)
	v.SetCursorStyle(lipgloss.NewStyle().Background(lipgloss.Color("#3a3a3a")))
	v.HScrollBy(+3)

	// act
	out := v.View()
	lines := strings.Split(out, "\n")

	// assert
	require.Len(t, lines, 2)
	assert.NotContains(t, lines[0], "\x1b[48", "non-cursor row must not carry a background SGR")
	assert.Contains(t, lines[1], "\x1b[48", "cursor row must carry a background SGR after hScroll")
	assert.Contains(t, lines[1], "nopqr", "cursor row content shifts with hScroll")
}

func TestViewport_CursorHighlight_OptInOnly(t *testing.T) {
	// arrange
	v := components.NewViewport()
	v.SetSize(10, 2)
	v.SetWrap(false)
	v.SetLines([]string{"alpha", "beta"})
	v.SetCursor(0)

	// act
	out := v.View()

	// assert
	assert.Equal(t, "alpha\nbeta", out)
}

func TestWindowScrollBindings_HomeEndJump(t *testing.T) {
	v := components.NewViewport()
	v.SetSize(40, 3)
	v.SetLines([]string{"a", "b", "c", "d", "e"})
	bs := components.WindowScrollBindings(v)

	_, ok := keymap.Dispatch(bs, keyPressMsg("end"))
	require.True(t, ok)
	assert.Equal(t, 2, v.ScrollOffset())

	_, ok = keymap.Dispatch(bs, keyPressMsg("home"))
	require.True(t, ok)
	assert.Equal(t, 0, v.ScrollOffset())
}

func TestWindowScrollBindings_GIgnored(t *testing.T) {
	v := components.NewViewport()
	v.SetSize(40, 3)
	v.SetLines([]string{"a", "b", "c", "d", "e"})
	bs := components.WindowScrollBindings(v)

	_, ok := keymap.Dispatch(bs, keyPressMsg("g"))
	assert.False(t, ok, "g is not part of the viewport keymap")
	_, ok = keymap.Dispatch(bs, keyPressMsg("G"))
	assert.False(t, ok, "G is not part of the viewport keymap")
}

func TestWindowScrollBindings_UnknownKeyNotConsumed(t *testing.T) {
	v := components.NewViewport()
	bs := components.WindowScrollBindings(v)

	_, ok := keymap.Dispatch(bs, keyPressMsg("ctrl+s"))
	assert.False(t, ok)
}

func TestWindowScrollBindings_IgnoresViewportCursor(t *testing.T) {
	// the form path mounts a caret cursor on the textarea viewport so the
	// caret renders, but j/k in NORMAL must pan the window — not drag the
	// caret along. This is the contract that distinguishes WindowScrollBindings
	// from the cursor-aware ScrollBindings default.
	v := components.NewViewport()
	v.SetSize(40, 3)
	v.SetLines([]string{"a", "b", "c", "d", "e"})
	v.SetCursor(0)
	bs := components.WindowScrollBindings(v)

	_, ok := keymap.Dispatch(bs, keyPressMsg("j"))
	require.True(t, ok)
	assert.Equal(t, 1, v.ScrollOffset(), "j must scroll the window")
	assert.Equal(t, 0, v.Cursor(), "j must NOT move the viewport cursor")
}

func TestWrapLines_HardwrapsByWidth(t *testing.T) {
	got := components.WrapLines([]string{"abcdef", "xy"}, 3)
	assert.Equal(t, []string{"abc", "def", "xy"}, got)
}

func TestWrapLines_EmptyLineStaysEmpty(t *testing.T) {
	got := components.WrapLines([]string{"", "ab"}, 5)
	assert.Equal(t, []string{"", "ab"}, got)
}

func TestWrapLines_NoOpWhenWidthZero(t *testing.T) {
	got := components.WrapLines([]string{"abc", "def"}, 0)
	assert.Equal(t, []string{"abc", "def"}, got)
}

func TestCursorVisualLine_NoWrap_ReturnsLogicalIndex(t *testing.T) {
	logical := []string{"first", "second", "third"}
	assert.Equal(t, 1, components.CursorVisualLine(logical, 1, 3, 80, false))
}

func TestCursorVisualLine_WrapAccountsForWrappedRows(t *testing.T) {
	// "abcdef" wraps to ["abc","def"] at width 3.
	// Cursor on logical line 1 (=second logical) at col 0 → visual line 2
	// (after the 2 visual lines from the wrapped first logical line).
	logical := []string{"abcdef", "xyz"}
	got := components.CursorVisualLine(logical, 1, 0, 3, true)
	assert.Equal(t, 2, got)
}

func TestCursorVisualLine_WrapCursorMidLine(t *testing.T) {
	// "abcdefghi" wraps at width 3 to ["abc","def","ghi"].
	// Cursor at col 5 of logical line 0 → after "abcde" wraps to ["abc","de"],
	// so cursor is on visual line 1.
	logical := []string{"abcdefghi"}
	got := components.CursorVisualLine(logical, 0, 5, 3, true)
	assert.Equal(t, 1, got)
}

func TestCursorVisualLine_WrapCursorAtWidthBoundary(t *testing.T) {
	// "abcdef" wraps at width 3 to ["abc","def"]. Cursor at col=3 is
	// rendered AT rune[3]='d' — i.e. on the second visual line, not
	// trailing the first. Without the boundary bump, EnsureCursorVisible
	// would scroll to the wrong line.
	logical := []string{"abcdef"}
	got := components.CursorVisualLine(logical, 0, 3, 3, true)
	assert.Equal(t, 1, got)
}

func TestCursorVisualLine_WrapCursorAtEndOfWidthSizedLine(t *testing.T) {
	// "abc" with width 3, cursor at end (col=3). The trailing-cursor cell
	// rendered by renderLineWithCursor pushes total visual width to 4 cells,
	// which hardwraps to a second visual line. Cursor must land there.
	logical := []string{"abc"}
	got := components.CursorVisualLine(logical, 0, 3, 3, true)
	assert.Equal(t, 1, got)
}
