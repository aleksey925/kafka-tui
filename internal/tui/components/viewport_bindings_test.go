package components_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
)

func TestScrollBindings_CursorlessMode_JKScrollsWindow(t *testing.T) {
	v := newViewport([]string{"a", "b", "c", "d", "e"})
	bs := components.ScrollBindings(v, nil)

	dispatch(t, bs, "j")
	assert.Equal(t, 1, v.ScrollOffset())
	assert.Equal(t, -1, v.Cursor(), "cursorless mode must not introduce a cursor")

	dispatch(t, bs, "k")
	assert.Equal(t, 0, v.ScrollOffset())
}

func TestScrollBindings_CursorMode_JKMovesCursor(t *testing.T) {
	v := newViewport([]string{"a", "b", "c", "d", "e"})
	v.SetCursor(0)
	bs := components.ScrollBindings(v, nil)

	dispatch(t, bs, "j")
	assert.Equal(t, 1, v.Cursor())

	dispatch(t, bs, "k")
	assert.Equal(t, 0, v.Cursor())
}

func TestScrollBindings_CursorMode_ClampsAtEdges(t *testing.T) {
	v := newViewport([]string{"a", "b", "c"})
	v.SetCursor(0)
	bs := components.ScrollBindings(v, nil)

	dispatch(t, bs, "k")
	assert.Equal(t, 0, v.Cursor(), "cursor clamps at 0")

	v.SetCursor(2)
	dispatch(t, bs, "j")
	assert.Equal(t, 2, v.Cursor(), "cursor clamps at last line")
}

func TestScrollBindings_PageDownUp_AliasesAndMode(t *testing.T) {
	v := newViewport([]string{"a", "b", "c", "d", "e", "f", "g", "h"})
	bs := components.ScrollBindings(v, nil)

	dispatch(t, bs, "ctrl+f")
	assert.Equal(t, 2, v.ScrollOffset(), "cursorless ctrl+f pages window")

	dispatch(t, bs, "ctrl+b")
	assert.Equal(t, 0, v.ScrollOffset())

	v.SetCursor(0)
	bs = components.ScrollBindings(v, nil)
	dispatch(t, bs, "pgdown")
	assert.Equal(t, 2, v.Cursor(), "cursor-mode pgdown moves cursor by pageStep")
}

func TestScrollBindings_HomeGoToTop(t *testing.T) {
	v := newViewport([]string{"a", "b", "c", "d", "e"})
	v.SetCursor(4)
	bs := components.ScrollBindings(v, nil)

	dispatch(t, bs, "home")
	assert.Equal(t, 0, v.Cursor())
}

func TestScrollBindings_EndGoToBottom(t *testing.T) {
	v := newViewport([]string{"a", "b", "c", "d", "e"})
	v.SetCursor(0)
	bs := components.ScrollBindings(v, nil)

	dispatch(t, bs, "end")
	assert.Equal(t, 4, v.Cursor())
}

func TestScrollBindings_HLOmittedWhenWrapOn(t *testing.T) {
	v := components.NewViewport()
	v.SetSize(40, 3)
	v.SetWrap(true)
	bs := components.ScrollBindings(v, nil)

	keys := bindingKeys(bs)
	assert.NotContains(t, keys, "h", "h must be hidden when wrap is on")
	assert.NotContains(t, keys, "l", "l must be hidden when wrap is on")
}

func TestScrollBindings_HLEmittedWhenWrapOff(t *testing.T) {
	v := components.NewViewport()
	v.SetWrap(false)
	v.SetSize(20, 3)
	v.SetLines([]string{"this single line is much longer than the twenty-cell viewport"})
	bs := components.ScrollBindings(v, nil)

	keys := bindingKeys(bs)
	assert.Contains(t, keys, "h")
	assert.Contains(t, keys, "l")

	dispatch(t, bs, "l")
	assert.Positive(t, v.HScrollOffset(), "l must pan right")
}

func TestScrollBindings_NoDuplicateKeysWithCursorMode(t *testing.T) {
	// guard against future drift: helper must satisfy keymap.Validate on
	// its own (no key listed twice across bindings).
	v := newViewport([]string{"a", "b", "c"})
	v.SetCursor(0)
	v.SetWrap(false)
	bs := components.ScrollBindings(v, nil)

	require.NoError(t, keymap.Validate(bs))
}

func TestScrollBindings_CallbackMode_RoutesAllVerticalKeysThroughOps(t *testing.T) {
	// callback-mode is used by screens whose cursor lives in a different
	// coordinate space than the viewport's visual lines (e.g. row index
	// when the rendered list interleaves section headers). The helper
	// must not touch v.SetCursor / v.ScrollBy directly when ops is set.
	v := newViewport([]string{"a", "b", "c", "d", "e"})
	moves := []int{}
	top, bottom := 0, 0
	cursor := &components.ScrollCursor{
		Move:     func(delta int) { moves = append(moves, delta) },
		PageStep: func() int { return 3 },
		ToTop:    func() { top++ },
		ToBottom: func() { bottom++ },
	}
	bs := components.ScrollBindings(v, cursor)

	dispatch(t, bs, "j")
	dispatch(t, bs, "k")
	dispatch(t, bs, "ctrl+f")
	dispatch(t, bs, "home")
	dispatch(t, bs, "end")

	assert.Equal(t, []int{+1, -1, +3}, moves, "j/k pass ±1; ctrl+f passes ±pageStep")
	assert.Equal(t, 1, top, "home routes through ops.ToTop")
	assert.Equal(t, 1, bottom, "end routes through ops.ToBottom")
	assert.Equal(t, -1, v.Cursor(), "callback mode must not introduce a viewport cursor")
}

func newViewport(lines []string) *components.Viewport {
	v := components.NewViewport()
	v.SetWrap(false)
	v.SetSize(40, 3)
	v.SetLines(lines)
	return v
}

func dispatch(t *testing.T, bs []keymap.Binding, key string) {
	t.Helper()
	_, ok := keymap.Dispatch(bs, keyPressMsg(key))
	require.True(t, ok, "binding for %q must dispatch", key)
}

func bindingKeys(bs []keymap.Binding) []string {
	out := make([]string, 0)
	for _, b := range bs {
		out = append(out, b.Keys...)
	}
	return out
}
