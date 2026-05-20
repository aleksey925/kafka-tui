package components_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

func TestForm_BindingsAdvertiseTabAndShiftTab(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "k", Label: "Key", Kind: components.FieldText},
	})

	keys := make(map[string]string)
	for _, b := range f.Bindings("Form") {
		require.Len(t, b.Keys, 1, "form nav bindings list one canonical key each")
		keys[b.Keys[0]] = b.Label
		assert.Equal(t, "Form", b.Category)
		assert.Nil(t, b.Handler, "advertise-only — Update handles dispatch")
	}
	assert.Contains(t, keys, "tab")
	assert.Contains(t, keys, "shift+tab")
}

func TestForm_TabFocusCyclesForwardAndBackward(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "key", Label: "Key", Kind: components.FieldText},
		{Key: "value", Label: "Value", Kind: components.FieldText},
		{Key: "compression", Label: "Compression", Kind: components.FieldDropdown,
			Value: "none", Options: []string{"none", "gzip", "snappy"}},
	})

	assert.Equal(t, 0, f.Focused())

	f, _ = f.Update(keyPressMsg("tab"))
	assert.Equal(t, 1, f.Focused())

	f, _ = f.Update(keyPressMsg("tab"))
	assert.Equal(t, 2, f.Focused())

	f, _ = f.Update(keyPressMsg("tab"))
	assert.Equal(t, 0, f.Focused()) // wraps

	f, _ = f.Update(keyPressMsg("shift+tab"))
	assert.Equal(t, 2, f.Focused())
}

func TestForm_TextFieldEditing(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "key", Label: "Key", Kind: components.FieldText},
	})
	for _, ch := range "abc" {
		f, _ = f.Update(keyPressRune(ch))
	}
	got, _ := f.Field("key")
	assert.Equal(t, "abc", got.Value)

	f, _ = f.Update(keyPressMsg("backspace"))
	got, _ = f.Field("key")
	assert.Equal(t, "ab", got.Value)
}

func TestForm_TextFieldReadlineShortcuts(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "k", Label: "Key", Kind: components.FieldText, Value: "hello world"},
	})
	f.FocusKey("k")

	// ctrl+a -> start, then insert "X" -> "Xhello world"
	f, _ = f.Update(keyPressMsg("ctrl+a"))
	f, _ = f.Update(keyPressRune('X'))
	got, _ := f.Field("k")
	assert.Equal(t, "Xhello world", got.Value)

	// ctrl+e -> end, then ctrl+w kills trailing "world"
	f, _ = f.Update(keyPressMsg("ctrl+e"))
	f, _ = f.Update(keyPressMsg("ctrl+w"))
	got, _ = f.Field("k")
	assert.Equal(t, "Xhello ", got.Value)

	// ctrl+u clears to start of line
	f, _ = f.Update(keyPressMsg("ctrl+u"))
	got, _ = f.Field("k")
	assert.Empty(t, got.Value)
}

func TestForm_TextFieldCtrlK(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "k", Label: "Key", Kind: components.FieldText, Value: "hello world"},
	})
	f.FocusKey("k")
	// move cursor to position 5 (after "hello")
	f, _ = f.Update(keyPressMsg("ctrl+a"))
	for range 5 {
		f, _ = f.Update(keyPressMsg("right"))
	}
	f, _ = f.Update(keyPressMsg("ctrl+k"))
	got, _ := f.Field("k")
	assert.Equal(t, "hello", got.Value)
}

func TestForm_TextFieldAltWordNav(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "k", Label: "Key", Kind: components.FieldText, Value: "foo bar baz"},
	})
	f.FocusKey("k")
	// alt+b from end jumps to start of "baz"; insert marker.
	f, _ = f.Update(keyPressMsg("alt+b"))
	f, _ = f.Update(keyPressRune('*'))
	got, _ := f.Field("k")
	assert.Equal(t, "foo bar *baz", got.Value)
}

func TestForm_TextareaCtrlU_KillsCurrentLineOnly(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "v", Label: "Value", Kind: components.FieldTextarea, Value: "first line\nsecond"},
	})
	f.FocusKey("v")
	// cursor lands at end of buffer (initialized in NewForm).
	f, _ = f.Update(keyPressMsg("ctrl+u"))
	got, _ := f.Field("v")
	assert.Equal(t, "first line\n", got.Value, "ctrl+u must stop at \\n")
}

func TestForm_TextareaAltBackspaceKillsWord(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "v", Label: "Value", Kind: components.FieldTextarea, Value: "hello world"},
	})
	f.FocusKey("v")
	f, _ = f.Update(keyPressMsg("alt+backspace"))
	got, _ := f.Field("v")
	assert.Equal(t, "hello ", got.Value)
}

func TestForm_PasteIntoTextFieldStripsNewlines(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "k", Label: "Key", Kind: components.FieldText, Value: "a"},
	})
	f.FocusKey("k")
	f, _ = f.Update(tea.PasteMsg{Content: "b\nc"})
	got, _ := f.Field("k")
	assert.Equal(t, "ab c", got.Value, "single-line field must replace \\n with space")
}

func TestForm_PasteIntoTextareaKeepsNewlines(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "v", Label: "Value", Kind: components.FieldTextarea, Value: "a"},
	})
	f.FocusKey("v")
	f, _ = f.Update(tea.PasteMsg{Content: "b\nc"})
	got, _ := f.Field("v")
	assert.Equal(t, "ab\nc", got.Value, "textarea must keep \\n")
}

func TestForm_PasteFiltersControlChars(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "k", Label: "Key", Kind: components.FieldText},
	})
	f.FocusKey("k")
	// pasting raw escape sequences must not survive — otherwise a later
	// View() would inject them into the rendered output.
	f, _ = f.Update(tea.PasteMsg{Content: "\x1b[31mhi\x1b[0m"})
	got, _ := f.Field("k")
	assert.NotContains(t, got.Value, "\x1b", "C0 controls must be filtered out")
}

func TestForm_PasteIntoDropdownIsNoop(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "d", Label: "D", Kind: components.FieldDropdown, Value: "a", Options: []string{"a", "b"}},
	})
	f.FocusKey("d")
	f, _ = f.Update(tea.PasteMsg{Content: "b"})
	got, _ := f.Field("d")
	assert.Equal(t, "a", got.Value, "paste must not change a non-text field")
}

func TestForm_PasteIntoEmptyListAppendsRow(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "h", Label: "Headers", Kind: components.FieldList},
	})
	f.FocusKey("h")
	f, _ = f.Update(tea.PasteMsg{Content: "k=v"})
	got, _ := f.Field("h")
	assert.Equal(t, []string{"k=v"}, got.List, "paste into empty list must create a row")
}

func TestForm_PasteMultilineIntoListSplitsRows(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "h", Label: "Headers", Kind: components.FieldList},
	})
	f.FocusKey("h")
	// the natural source: pasting from another tool that lists headers
	// one-per-line. Pre-split keeps validators (e.g. validateHeader) happy.
	f, _ = f.Update(tea.PasteMsg{Content: "a=1\nb=2\nc=3"})
	got, _ := f.Field("h")
	assert.Equal(t, []string{"a=1", "b=2", "c=3"}, got.List)
}

func TestForm_PasteMultilineIntoListPreservesSurroundingText(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "h", Label: "Headers", Kind: components.FieldList, List: []string{"prefix=tail"}},
	})
	f.FocusKey("h")
	// move the entry cursor to the boundary between "prefix=" and "tail" so
	// the multi-line paste lands at a known offset (initial cursor is at
	// end-of-entry, then 4× left → between '=' and 't').
	f, _ = f.Update(keyPressMsg("left"))
	f, _ = f.Update(keyPressMsg("left"))
	f, _ = f.Update(keyPressMsg("left"))
	f, _ = f.Update(keyPressMsg("left"))

	f, _ = f.Update(tea.PasteMsg{Content: "X\nY"})

	got, _ := f.Field("h")
	// "prefix=" + "X" stays in row 0; "Y" + "tail" becomes a new row.
	assert.Equal(t, []string{"prefix=X", "Ytail"}, got.List)
}

func TestForm_TextFieldEnterIgnoredSinglelineButAddsNewlineForTextarea(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "k", Label: "Key", Kind: components.FieldText},
		{Key: "v", Label: "Value", Kind: components.FieldTextarea},
	})
	for _, ch := range "ab" {
		f, _ = f.Update(keyPressRune(ch))
	}
	f, _ = f.Update(keyPressMsg("enter"))
	got, _ := f.Field("k")
	assert.Equal(t, "ab", got.Value) // newline NOT appended for FieldText

	f.FocusKey("v")
	for _, ch := range "first" {
		f, _ = f.Update(keyPressRune(ch))
	}
	f, _ = f.Update(keyPressMsg("enter"))
	for _, ch := range "second" {
		f, _ = f.Update(keyPressRune(ch))
	}
	got, _ = f.Field("v")
	assert.Equal(t, "first\nsecond", got.Value)
}

func TestForm_DropdownNavigatesWithJK(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "compression", Label: "Compression", Kind: components.FieldDropdown,
			Value: "none", Options: []string{"none", "gzip", "snappy", "lz4"}},
	})
	f, _ = f.Update(keyPressMsg("j"))
	got, _ := f.Field("compression")
	assert.Equal(t, "gzip", got.Value)

	f, _ = f.Update(keyPressMsg("j"))
	f, _ = f.Update(keyPressMsg("j"))
	got, _ = f.Field("compression")
	assert.Equal(t, "lz4", got.Value)

	f, _ = f.Update(keyPressMsg("j"))
	got, _ = f.Field("compression")
	assert.Equal(t, "none", got.Value) // wraps

	f, _ = f.Update(keyPressMsg("k"))
	got, _ = f.Field("compression")
	assert.Equal(t, "lz4", got.Value)
}

func TestForm_TextCursorArrowsAndMidStringInsert(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "k", Label: "Key", Kind: components.FieldText, Value: "abc"},
	})
	// cursor initialized to end of preset value
	assert.Equal(t, 3, f.CursorAt("k"))

	// move left twice → between 'a' and 'b'
	f, _ = f.Update(keyPressMsg("left"))
	f, _ = f.Update(keyPressMsg("left"))
	assert.Equal(t, 1, f.CursorAt("k"))

	// insert 'X' between 'a' and 'b'
	f, _ = f.Update(keyPressRune('X'))
	got, _ := f.Field("k")
	assert.Equal(t, "aXbc", got.Value)
	assert.Equal(t, 2, f.CursorAt("k"))

	// right to end-1, then home/end
	f, _ = f.Update(keyPressMsg("end"))
	assert.Equal(t, 4, f.CursorAt("k"))
	f, _ = f.Update(keyPressMsg("home"))
	assert.Equal(t, 0, f.CursorAt("k"))
}

func TestForm_TextCursorBackspaceAndDelete(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "k", Kind: components.FieldText, Value: "hello"},
	})
	// cursor at end → backspace removes 'o'
	f, _ = f.Update(keyPressMsg("backspace"))
	got, _ := f.Field("k")
	assert.Equal(t, "hell", got.Value)
	assert.Equal(t, 4, f.CursorAt("k"))

	// move to position 1, delete forward removes 'e'
	f, _ = f.Update(keyPressMsg("home"))
	f, _ = f.Update(keyPressMsg("right"))
	f, _ = f.Update(keyPressMsg("delete"))
	got, _ = f.Field("k")
	assert.Equal(t, "hll", got.Value)
	assert.Equal(t, 1, f.CursorAt("k"))

	// backspace from position 1 removes 'h'
	f, _ = f.Update(keyPressMsg("backspace"))
	got, _ = f.Field("k")
	assert.Equal(t, "ll", got.Value)
	assert.Equal(t, 0, f.CursorAt("k"))

	// further backspace at start is a no-op
	f, _ = f.Update(keyPressMsg("backspace"))
	got, _ = f.Field("k")
	assert.Equal(t, "ll", got.Value)
}

func TestForm_TextCursorIgnoresOutOfBoundsArrows(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "k", Kind: components.FieldText, Value: "ab"},
	})
	f, _ = f.Update(keyPressMsg("right"))
	assert.Equal(t, 2, f.CursorAt("k")) // already at end, stays

	f, _ = f.Update(keyPressMsg("home"))
	f, _ = f.Update(keyPressMsg("left"))
	assert.Equal(t, 0, f.CursorAt("k")) // already at start, stays
}

func TestForm_TextareaEnterInsertsAtCursor(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "v", Kind: components.FieldTextarea, Value: "abcd"},
	})
	// cursor at end, then move 2 left (between 'b' and 'c')
	f, _ = f.Update(keyPressMsg("left"))
	f, _ = f.Update(keyPressMsg("left"))
	f, _ = f.Update(keyPressMsg("enter"))
	got, _ := f.Field("v")
	assert.Equal(t, "ab\ncd", got.Value)
	assert.Equal(t, 3, f.CursorAt("v"))
}

func TestForm_TextareaUpDownPreservesColumn(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "v", Kind: components.FieldTextarea, Value: "abcdef\nghi\njklmno"},
	})
	// cursor at end (rune index 17). end of last line, col 6.
	assert.Equal(t, 17, f.CursorAt("v"))

	// up: previous line "ghi" has length 3, col clamps from 6 to 3.
	f, _ = f.Update(keyPressMsg("up"))
	// position = start of "ghi" (7) + 3 = 10
	assert.Equal(t, 10, f.CursorAt("v"))

	// up again: "abcdef" is long enough, col stays at 3.
	f, _ = f.Update(keyPressMsg("up"))
	assert.Equal(t, 3, f.CursorAt("v"))

	// up at top → no-op
	f, _ = f.Update(keyPressMsg("up"))
	assert.Equal(t, 3, f.CursorAt("v"))

	// down restores down to "ghi" col 3
	f, _ = f.Update(keyPressMsg("down"))
	assert.Equal(t, 10, f.CursorAt("v"))
}

func TestForm_TextareaHomeEndAreLineAware(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "v", Kind: components.FieldTextarea, Value: "abc\ndef\nghi"},
	})
	// cursor at end of value (11). go up to middle line.
	f, _ = f.Update(keyPressMsg("up"))
	// home: start of "def" (rune index 4)
	f, _ = f.Update(keyPressMsg("home"))
	assert.Equal(t, 4, f.CursorAt("v"))
	// end: just before the trailing \n of "def" (rune index 7)
	f, _ = f.Update(keyPressMsg("end"))
	assert.Equal(t, 7, f.CursorAt("v"))
}

func TestForm_TextSetValueResetsCursor(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "k", Kind: components.FieldText, Value: "abc"},
	})
	f, _ = f.Update(keyPressMsg("left"))
	f, _ = f.Update(keyPressMsg("left"))
	assert.Equal(t, 1, f.CursorAt("k"))

	f.SetValue("k", "hello")
	assert.Equal(t, 5, f.CursorAt("k")) // cursor pinned to end of new value
}

func TestForm_EmptyTextRendersDashWhenNotEditing(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "k", Label: "Key", Kind: components.FieldText},
		{Key: "v", Label: "Value", Kind: components.FieldTextarea},
	})
	f.SetEditing(false)
	out := f.View()
	// both fields are empty and not in caret mode → dash placeholder
	assert.Contains(t, out, "—")

	// once value is set, the dash is gone
	f.SetValue("k", "hello")
	out = f.View()
	assert.Contains(t, out, "hello")
	// the textarea is still empty so one dash remains; setting both removes all
	f.SetValue("v", "world")
	out = f.View()
	assert.NotContains(t, out, "—")
}

func TestForm_SetEditingHidesCursorBackground(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "k", Kind: components.FieldText, Value: "abcd"},
	})
	// Cursor renders as a reverse-video rune (foreground=bg, background=
	// accent). The accent-as-background SGR is the only place a background
	// color appears in this view, so its presence/absence is a reliable
	// proxy for "is the cursor drawn?".
	const cursorBgSGR = "48;2;209;138;69" // theme.Accent rgb as background
	editing := f.View()
	assert.Contains(t, editing, cursorBgSGR)

	f.SetEditing(false)
	plain := f.View()
	assert.NotContains(t, plain, cursorBgSGR)
}

func TestForm_SegmentedArrowKeysCycleValue(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "c", Label: "Compression", Kind: components.FieldSegmented,
			Value: "none", Options: []string{"none", "gzip", "snappy", "lz4"}},
	})
	// the inline control renders as `◂ value ▸` — a horizontal widget.
	// Only horizontal-motion keys (right/l, left/h) cycle it; vertical
	// keys (up/down, j/k) are reserved for field-nav at the screen
	// level and must NOT mutate the value here.
	f, _ = f.Update(keyPressMsg("right"))
	got, _ := f.Field("c")
	assert.Equal(t, "gzip", got.Value)

	f, _ = f.Update(keyPressMsg("l"))
	got, _ = f.Field("c")
	assert.Equal(t, "snappy", got.Value)

	f, _ = f.Update(keyPressMsg("right"))
	got, _ = f.Field("c")
	assert.Equal(t, "lz4", got.Value)

	f, _ = f.Update(keyPressMsg("right"))
	got, _ = f.Field("c")
	assert.Equal(t, "none", got.Value) // wraps

	f, _ = f.Update(keyPressMsg("left"))
	got, _ = f.Field("c")
	assert.Equal(t, "lz4", got.Value)

	f, _ = f.Update(keyPressMsg("h"))
	got, _ = f.Field("c")
	assert.Equal(t, "snappy", got.Value)
}

func TestForm_SegmentedInlineIgnoresVerticalKeys(t *testing.T) {
	// regression guard: j/k/up/down must not cycle the inline value.
	// They are claimed by field-nav at the screen level.
	f := components.NewForm([]components.Field{
		{Key: "c", Label: "Compression", Kind: components.FieldSegmented,
			Value: "none", Options: []string{"none", "gzip", "snappy"}},
	})
	for _, k := range []string{"down", "up", "j", "k"} {
		f, _ = f.Update(keyPressMsg(k))
		got, _ := f.Field("c")
		assert.Equal(t, "none", got.Value, "key %q must not cycle inline segmented", k)
	}
}

func TestForm_SegmentedEnterTogglesPopup(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "c", Label: "Compression", Kind: components.FieldSegmented,
			Value: "none", Options: []string{"none", "gzip", "snappy"}},
	})
	assert.False(t, f.PopupActive())

	f, _ = f.Update(keyPressMsg("enter"))
	assert.True(t, f.PopupActive())

	// arrow keys still cycle live while popup is open
	f, _ = f.Update(keyPressMsg("down"))
	got, _ := f.Field("c")
	assert.Equal(t, "gzip", got.Value)

	f, _ = f.Update(keyPressMsg("enter"))
	assert.False(t, f.PopupActive())
	got, _ = f.Field("c")
	assert.Equal(t, "gzip", got.Value) // confirmed
}

func TestForm_SegmentedEscRevertsAndClosesPopup(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "c", Label: "Compression", Kind: components.FieldSegmented,
			Value: "snappy", Options: []string{"none", "gzip", "snappy", "lz4"}},
	})
	f, _ = f.Update(keyPressMsg("enter"))
	f, _ = f.Update(keyPressMsg("down"))
	f, _ = f.Update(keyPressMsg("down"))
	got, _ := f.Field("c")
	assert.Equal(t, "none", got.Value) // wrapped past lz4

	f, _ = f.Update(keyPressMsg("esc"))
	assert.False(t, f.PopupActive())
	got, _ = f.Field("c")
	assert.Equal(t, "snappy", got.Value) // reverted
}

func TestForm_SegmentedTabClosesPopup(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "c", Label: "Compression", Kind: components.FieldSegmented,
			Value: "gzip", Options: []string{"none", "gzip"}},
		{Key: "k", Label: "Key", Kind: components.FieldText},
	})
	f, _ = f.Update(keyPressMsg("enter"))
	assert.True(t, f.PopupActive())

	f, _ = f.Update(keyPressMsg("tab"))
	assert.Equal(t, 1, f.Focused())
	// after focus moves, the segmented field's popup must be closed.
	f.FocusKey("c")
	assert.False(t, f.PopupActive())
}

func TestForm_SegmentedRendersCompactInlineWhenFocused(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "c", Label: "Compression", Kind: components.FieldSegmented,
			Value: "snappy", Options: []string{"none", "gzip", "snappy"}},
	})
	out := f.View()
	assert.Contains(t, out, "◂ snappy ▸")
}

func TestForm_SegmentedRendersListWhenPopupOpen(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "c", Label: "Compression", Kind: components.FieldSegmented,
			Value: "gzip", Options: []string{"none", "gzip", "snappy"}},
	})
	f, _ = f.Update(keyPressMsg("enter"))
	out := f.View()
	for _, opt := range []string{"none", "gzip", "snappy"} {
		assert.Contains(t, out, opt)
	}
	assert.Contains(t, out, "(•) gzip")
}

func TestForm_SegmentedRendersPlainWhenUnfocused(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "k", Label: "Key", Kind: components.FieldText},
		{Key: "c", Label: "Compression", Kind: components.FieldSegmented,
			Value: "gzip", Options: []string{"none", "gzip"}},
	})
	out := f.View()
	assert.Contains(t, out, "gzip")
	assert.NotContains(t, out, "◂") // unfocused does not show the slider chrome
}

func TestForm_ListAddTypeDelete(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "headers", Label: "Headers", Kind: components.FieldList},
	})
	// add/remove are no longer keystrokes at the form level — hosting
	// screens drive them via AppendListRow / RemoveListRow.
	f.AppendListRow()
	for _, ch := range "key1" {
		f, _ = f.Update(keyPressRune(ch))
	}
	f.AppendListRow()
	for _, ch := range "key2" {
		f, _ = f.Update(keyPressRune(ch))
	}

	got, _ := f.Field("headers")
	assert.Equal(t, []string{"key1", "key2"}, got.List)

	// move cursor up to "key1" and remove it
	f, _ = f.Update(keyPressMsg("up"))
	f.RemoveListRow()

	got, _ = f.Field("headers")
	assert.Equal(t, []string{"key2"}, got.List)
}

func TestForm_ListBackspaceEditsCurrentEntry(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "h", Label: "Headers", Kind: components.FieldList, List: []string{"abc"}},
	})
	f, _ = f.Update(keyPressMsg("backspace"))
	got, _ := f.Field("h")
	assert.Equal(t, []string{"ab"}, got.List)
}

func TestForm_ListBackspaceOnEmptyEntryRemovesIt(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "h", Kind: components.FieldList, List: []string{"a", "", "c"}},
	})
	// focus on second (empty) entry
	f, _ = f.Update(keyPressMsg("down"))
	f, _ = f.Update(keyPressMsg("backspace"))
	got, _ := f.Field("h")
	assert.Equal(t, []string{"a", "c"}, got.List)
}

func TestForm_ListInRowCursorEditing(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "h", Kind: components.FieldList, List: []string{"abcd"}},
	})
	// cursor initialized at end of "abcd"
	assert.Equal(t, 4, f.ListEntryCursor("h"))

	// move left twice, insert 'X' between 'b' and 'c'
	f, _ = f.Update(keyPressMsg("left"))
	f, _ = f.Update(keyPressMsg("left"))
	f, _ = f.Update(keyPressRune('X'))
	got, _ := f.Field("h")
	assert.Equal(t, []string{"abXcd"}, got.List)
	assert.Equal(t, 3, f.ListEntryCursor("h"))

	// home, then delete forward removes 'a'
	f, _ = f.Update(keyPressMsg("home"))
	f, _ = f.Update(keyPressMsg("delete"))
	got, _ = f.Field("h")
	assert.Equal(t, []string{"bXcd"}, got.List)
}

func TestForm_ListUpDownResetsEntryCursorToEnd(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "h", Kind: components.FieldList, List: []string{"abc", "defghi"}},
	})
	// move left twice on "abc"
	f, _ = f.Update(keyPressMsg("left"))
	f, _ = f.Update(keyPressMsg("left"))
	assert.Equal(t, 1, f.ListEntryCursor("h"))

	// move down to "defghi" — cursor lands at end (6)
	f, _ = f.Update(keyPressMsg("down"))
	assert.Equal(t, 6, f.ListEntryCursor("h"))
}

func TestForm_ListSetListResetsEntryCursor(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "h", Kind: components.FieldList, List: []string{"abc"}},
	})
	f, _ = f.Update(keyPressMsg("left"))
	assert.Equal(t, 2, f.ListEntryCursor("h"))

	f.SetList("h", []string{"hello"})
	assert.Equal(t, 5, f.ListEntryCursor("h"))
}

func TestForm_ListRendersAffordances(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "h", Kind: components.FieldList, List: []string{"a"}},
	})
	out := f.View()
	assert.Contains(t, out, "add row")
	assert.Contains(t, out, "remove row")
}

func TestForm_SetValueAndSetList(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "k", Label: "K", Kind: components.FieldText},
		{Key: "h", Label: "H", Kind: components.FieldList},
	})
	f.SetValue("k", "preset")
	f.SetList("h", []string{"a", "b"})

	got, _ := f.Field("k")
	assert.Equal(t, "preset", got.Value)

	got, _ = f.Field("h")
	assert.Equal(t, []string{"a", "b"}, got.List)
}

func TestForm_FocusKey(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "a", Kind: components.FieldText},
		{Key: "b", Kind: components.FieldText},
	})
	f.FocusKey("b")
	assert.Equal(t, 1, f.Focused())
	assert.Equal(t, "b", f.FocusedField().Key)
}

func TestForm_ViewIncludesAllLabels(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "key", Label: "Key", Kind: components.FieldText, Value: "k"},
		{Key: "value", Label: "Value", Kind: components.FieldTextarea, Value: "v"},
		{Key: "compression", Label: "Compression", Kind: components.FieldDropdown,
			Value: "gzip", Options: []string{"none", "gzip"}},
		{Key: "headers", Label: "Headers", Kind: components.FieldList, List: []string{"x"}},
	})
	out := f.View()
	for _, label := range []string{"Key", "Value", "Compression", "Headers"} {
		assert.Contains(t, out, label)
	}
	assert.Contains(t, out, "gzip")
	assert.Contains(t, out, "x")
}

func TestFieldKindString(t *testing.T) {
	assert.Equal(t, "text", components.FieldText.String())
	assert.Equal(t, "dropdown", components.FieldDropdown.String())
	assert.Equal(t, "list", components.FieldList.String())
	assert.Equal(t, "textarea", components.FieldTextarea.String())
	assert.Equal(t, "segmented", components.FieldSegmented.String())
}

func TestForm_FieldsReturnsDefensiveCopy(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "k", Label: "Key", Kind: components.FieldText, Value: "v"},
	})

	got := f.Fields()
	got[0].Value = "mutated"

	original, _ := f.Field("k")
	assert.Equal(t, "v", original.Value, "mutating returned slice must not affect form state")
}

func TestForm_FocusedListEntry(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "h", Label: "Headers", Kind: components.FieldList, List: []string{"a=1", "b=2"}},
	})

	val, idx, ok := f.FocusedListEntry()

	assert.True(t, ok)
	assert.Equal(t, "a=1", val)
	assert.Equal(t, 0, idx)
}

func TestForm_FocusedListEntry_NonListFieldReturnsFalse(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "k", Label: "Key", Kind: components.FieldText, Value: "v"},
	})

	_, _, ok := f.FocusedListEntry()
	assert.False(t, ok)
}

func TestForm_FocusedListEntry_EmptyListReturnsFalse(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "h", Label: "Headers", Kind: components.FieldList, List: nil},
	})

	_, _, ok := f.FocusedListEntry()
	assert.False(t, ok)
}

func TestForm_ValidateFocusedListEntry_RunsValidator(t *testing.T) {
	bad := errors.New("invalid")
	f := components.NewForm([]components.Field{
		{Key: "h", Label: "Headers", Kind: components.FieldList,
			List:      []string{"oops"},
			Validator: func(string) error { return bad },
		},
	})

	assert.ErrorIs(t, f.ValidateFocusedListEntry(), bad)
}

func TestForm_ValidateFocusedListEntry_EmptyEntryIsSkipped(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "h", Label: "Headers", Kind: components.FieldList,
			List:      []string{""},
			Validator: func(string) error { return errors.New("must not be called") },
		},
	})

	assert.NoError(t, f.ValidateFocusedListEntry())
}

func TestForm_ValidateFocusedListEntry_NoValidatorIsNoop(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "h", Label: "Headers", Kind: components.FieldList, List: []string{"x"}},
	})

	assert.NoError(t, f.ValidateFocusedListEntry())
}

func TestForm_RenderField_ReturnsFieldStringByKey(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "topic", Label: "Topic", Kind: components.FieldText, Value: "orders"},
	})

	out := f.RenderField("topic")

	assert.Contains(t, out, "Topic")
	assert.Contains(t, out, "orders")
}

func TestForm_RenderField_MissingKeyReturnsEmpty(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "k", Label: "K", Kind: components.FieldText},
	})

	assert.Empty(t, f.RenderField("missing"))
}

func TestForm_InsertAtCursor_TextField(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "k", Label: "K", Kind: components.FieldText, Value: "ab"},
	})

	f.InsertAtCursor("XY")

	got, _ := f.Field("k")
	assert.Equal(t, "abXY", got.Value)
}

func TestForm_InsertAtCursor_ListField(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "h", Label: "Headers", Kind: components.FieldList, List: []string{"abc"}},
	})

	f.InsertAtCursor("Z")

	got, _ := f.Field("h")
	assert.Equal(t, []string{"abcZ"}, got.List)
}

func TestForm_InsertAtCursor_NoopOnDropdown(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "c", Label: "Compression", Kind: components.FieldDropdown,
			Value: "none", Options: []string{"none", "gzip"}},
	})

	f.InsertAtCursor("anything")

	got, _ := f.Field("c")
	assert.Equal(t, "none", got.Value)
}

func TestForm_SetSegmentedPopup_OpenAndClose(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "c", Label: "Compression", Kind: components.FieldSegmented,
			Value: "none", Options: []string{"none", "gzip"}},
	})

	f.SetSegmentedPopup("c", true)
	assert.True(t, f.PopupActive())

	f.SetSegmentedPopup("c", false)
	assert.False(t, f.PopupActive())
}

func TestForm_SetFocusedSuffixAndEditing(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "k", Label: "Key", Kind: components.FieldText},
	})

	assert.True(t, f.Editing(), "form starts in editing mode")

	f.SetEditing(false)
	assert.False(t, f.Editing())

	// suffix appears next to focused field's label.
	f.SetFocusedSuffix("[EDIT]")
	assert.Contains(t, f.View(), "[EDIT]")
}

func TestForm_Reset_RestoresValuesAndPreservesInjectedOptions(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "partition", Label: "Partition", Kind: components.FieldSegmented,
			Options: []string{"auto"}, Value: "auto"},
		{Key: "key", Label: "Key", Kind: components.FieldText, Value: ""},
	})

	// simulate the async-load path: a screen calls SetOptions after some
	// real data arrives, then the user picks a non-default value.
	f.SetOptions("partition", []string{"auto", "0", "1", "2", "3"})
	f.SetValue("partition", "2")
	f.SetValue("key", "user-typed")

	f.Reset()

	partition, _ := f.Field("partition")
	assert.Equal(t,
		[]string{"auto", "0", "1", "2", "3"},
		partition.Options,
		"injected options must survive Reset — they are structure, not value",
	)
	assert.Equal(t, "auto", partition.Value, "Value returns to construction default")

	key, _ := f.Field("key")
	assert.Empty(t, key.Value, "text field returns to construction default")
}

func TestForm_Reset_ClearsListEntriesAndCursor(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "headers", Label: "Headers", Kind: components.FieldList},
	})
	f.FocusKey("headers")
	// add two rows so list/cursor state diverges from construction.
	f.AppendListRow()
	f.InsertAtCursor("first")
	f.AppendListRow()
	f.InsertAtCursor("second")

	f.Reset()

	got, _ := f.Field("headers")
	assert.Empty(t, got.List, "list rows must reset to the construction default (empty)")
	assert.Equal(t, 0, f.ListEntryCursor("headers"), "list entry cursor must reset to construction position")
}

func TestForm_Reset_MovesFocusBackToFirstField(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "a", Label: "A", Kind: components.FieldText},
		{Key: "b", Label: "B", Kind: components.FieldText},
		{Key: "c", Label: "C", Kind: components.FieldText},
	})
	f.FocusKey("c")
	require.Equal(t, 2, f.Focused())

	f.Reset()

	assert.Equal(t, 0, f.Focused())
}

func TestForm_Reset_RestoresTextCursorToValueEnd(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "k", Label: "K", Kind: components.FieldText, Value: "hello"},
	})
	f.FocusKey("k")
	// move cursor away from the construction position.
	for range 3 {
		f, _ = f.Update(keyPressMsg("left"))
	}
	require.Equal(t, 2, f.CursorAt("k"))

	f.Reset()

	assert.Equal(t, len("hello"), f.CursorAt("k"), "cursor returns to end of default value")
}

func TestForm_Reset_ClosesOpenSegmentedPopup(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "c", Label: "Compression", Kind: components.FieldSegmented,
			Options: []string{"none", "gzip"}, Value: "none"},
	})
	f.SetSegmentedPopup("c", true)
	require.True(t, f.PopupActive())

	f.Reset()

	assert.False(t, f.PopupActive(), "Reset returns to clean construction state — no popup")
}

func TestForm_Reset_LeavesEditingFlagAndSuffixToHost(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "k", Label: "K", Kind: components.FieldText},
	})
	f.SetEditing(false)
	f.SetFocusedSuffix("[EDIT]")

	f.Reset()

	// editing flag and suffix are host-owned (set by the screen's mode-restore
	// logic after clear). Reset must not stomp them.
	assert.False(t, f.Editing())
	assert.Contains(t, f.View(), "[EDIT]")
}

// TestForm_Reset_RewindsTextareaViewportScroll verifies the second half of
// the Form.Reset contract: not only Value/List/cursors restore, but per-field
// viewport scroll state zeros out too. Without this, a screen that scrolled
// a long textarea, hit ctrl+u, and refilled the same content would see the
// viewport stuck where it last sat.
func TestForm_Reset_RewindsTextareaViewportScroll(t *testing.T) {
	// build 50 distinct lines so we can see which window the viewport is on.
	var sb strings.Builder
	for i := range 50 {
		fmt.Fprintf(&sb, "LINE_%d\n", i)
	}
	bigDefault := strings.TrimRight(sb.String(), "\n")

	f := components.NewForm([]components.Field{
		{Key: "v", Label: "V", Kind: components.FieldTextarea, Value: bigDefault},
		{Key: "k", Label: "K", Kind: components.FieldText},
	})
	f.SetSize(40, 14)
	// editing=false → no cursor-follow on render, so the viewport's scrollTop
	// is observable directly. Otherwise SetCursor would pull it to the end of
	// content every frame and mask whatever Reset did.
	f.SetEditing(false)
	f.FocusKey("v")
	f.View() // populate the viewport with content

	f.HandleViewportKey(keyPressMsg("end")) // scroll to bottom
	bottom := f.View()
	require.Contains(t, bottom, "LINE_49", "end must scroll to the bottom")
	require.NotContains(t, bottom, "LINE_0", "top must have scrolled off")

	f.Reset()

	rewound := f.View()
	assert.Contains(t, rewound, "LINE_0", "Reset must rewind the viewport to scrollTop=0")
}

func TestForm_HandleViewportKey_BoundedFieldConsumesScrollKey(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "v", Label: "V", Kind: components.FieldTextarea, Value: strings.Repeat("x\n", 50)},
	})
	f.SetSize(40, 8)
	f.SetEditing(false)
	f.FocusKey("v")
	f.View() // populate viewport before key dispatch

	consumed := f.HandleViewportKey(keyPressMsg("j"))

	assert.True(t, consumed, "scroll keys on a focused textarea must be claimed by its viewport")
}

func TestForm_HandleViewportKey_NonBoundedFieldRejectsKey(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "k", Label: "K", Kind: components.FieldText},
	})
	f.FocusKey("k")

	consumed := f.HandleViewportKey(keyPressMsg("j"))

	assert.False(t, consumed, "non-bounded fields have no viewport — host must get the key back to handle elsewhere")
}

// TestForm_AllocateBodyHeights_HandlesSegmentedPopupExpansion: when a
// segmented field's popup opens, its natural body height grows (options +
// hint). The elastic textarea must absorb that extra height without
// overflowing the form's allotted area.
func TestForm_AllocateBodyHeights_HandlesSegmentedPopupExpansion(t *testing.T) {
	f := components.NewForm([]components.Field{
		{Key: "c", Label: "C", Kind: components.FieldSegmented,
			Options: []string{"a", "b", "c", "d"}, Value: "a"},
		{Key: "v", Label: "V", Kind: components.FieldTextarea, Value: strings.Repeat("X\n", 60)},
	})
	f.SetSize(40, 20)
	f.SetSegmentedPopup("c", true)

	out := f.View()
	totalLines := strings.Count(out, "\n") + 1
	assert.LessOrEqual(t, totalLines, 20,
		"opening the popup must not push the form past its allotted height — textarea shrinks to absorb")
	// popup itself must still be visible (the user is interacting with it).
	for _, opt := range []string{"a", "b", "c", "d"} {
		assert.Contains(t, out, opt, "popup option %q must render even when textarea is elastic", opt)
	}
}

func TestForm_WithFormStyles_OverridesPalette(t *testing.T) {
	custom := theme.DefaultStyles()
	f := components.NewForm(
		[]components.Field{{Key: "k", Label: "K", Kind: components.FieldText}},
		components.WithFormStyles(custom),
	)

	// the form must construct with the override applied without panicking.
	assert.NotEmpty(t, f.View())
}
