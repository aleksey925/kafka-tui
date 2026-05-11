package components_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
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

func TestViewport_HandleKey_GgChordScrollsToTop(t *testing.T) {
	v := components.NewViewport()
	v.SetSize(40, 3)
	v.SetLines([]string{"a", "b", "c", "d", "e"})
	v.ScrollToBottom()
	require.Equal(t, 2, v.ScrollOffset())

	require.True(t, v.HandleKey(keyPressMsg("g")), "first g must be consumed (arms chord)")
	assert.Equal(t, 2, v.ScrollOffset(), "first g does NOT scroll on its own")

	require.True(t, v.HandleKey(keyPressMsg("g")), "second g fires the chord")
	assert.Equal(t, 0, v.ScrollOffset())
}

func TestViewport_HandleKey_NonGDisarmsChord(t *testing.T) {
	v := components.NewViewport()
	v.SetSize(40, 3)
	v.SetLines([]string{"a", "b", "c", "d", "e"})

	v.HandleKey(keyPressMsg("g")) // arm
	v.HandleKey(keyPressMsg("j")) // disarm
	v.HandleKey(keyPressMsg("g")) // arm again
	v.HandleKey(keyPressMsg("g")) // fire

	assert.Equal(t, 0, v.ScrollOffset())
}

func TestViewport_HandleKey_WToggleWrap(t *testing.T) {
	v := components.NewViewport()
	require.True(t, v.Wrap())

	require.True(t, v.HandleKey(keyPressMsg("w")))
	assert.False(t, v.Wrap())

	require.True(t, v.HandleKey(keyPressMsg("w")))
	assert.True(t, v.Wrap())
}

func TestViewport_HandleKey_UnknownKeyNotConsumed(t *testing.T) {
	v := components.NewViewport()
	assert.False(t, v.HandleKey(keyPressMsg("ctrl+s")))
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
