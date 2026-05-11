package lineedit

import (
	"testing"
	"unicode/utf8"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLineStart(t *testing.T) {
	tests := []struct {
		name   string
		text   string
		cursor int
		want   int
	}{
		{"empty", "", 0, 0},
		{"single line at start", "hello", 0, 0},
		{"single line at end", "hello", 5, 0},
		{"single line middle", "hello", 3, 0},
		{"multi line first line", "foo\nbar", 2, 0},
		{"multi line second line", "foo\nbar", 5, 4},
		{"multi line at newline", "foo\nbar", 3, 0},
		{"multi line right after newline", "foo\nbar", 4, 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, LineStart([]rune(tt.text), tt.cursor))
		})
	}
}

func TestLineEnd(t *testing.T) {
	tests := []struct {
		name   string
		text   string
		cursor int
		want   int
	}{
		{"empty", "", 0, 0},
		{"single line at start", "hello", 0, 5},
		{"single line at end", "hello", 5, 5},
		{"multi line first line", "foo\nbar", 0, 3},
		{"multi line at newline", "foo\nbar", 3, 3},
		{"multi line second line", "foo\nbar", 5, 7},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, LineEnd([]rune(tt.text), tt.cursor))
		})
	}
}

func TestWordBoundaryBack(t *testing.T) {
	tests := []struct {
		name   string
		text   string
		cursor int
		want   int
	}{
		{"empty", "", 0, 0},
		{"at start", "hello world", 0, 0},
		{"end of word", "hello world", 11, 6},
		{"middle of word", "hello world", 8, 6},
		{"after spaces", "hello   world", 13, 8},
		{"only spaces left", "   ", 3, 0},
		{"leading spaces consumed", "  foo  ", 7, 2},
		{"unicode word", "привет мир", 10, 7},
		{"path-like single token", "/usr/local/bin", 14, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, WordBoundaryBack([]rune(tt.text), tt.cursor))
		})
	}
}

func TestWordBoundaryForward(t *testing.T) {
	tests := []struct {
		name   string
		text   string
		cursor int
		want   int
	}{
		{"empty", "", 0, 0},
		{"at end", "hello world", 11, 11},
		{"at start", "hello world", 0, 5},
		{"middle of first word", "hello world", 2, 5},
		{"between words on space", "hello world", 5, 11},
		{"between words on second space", "hello  world", 6, 12},
		{"unicode word", "привет мир", 0, 6},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, WordBoundaryForward([]rune(tt.text), tt.cursor))
		})
	}
}

func TestKillToLineStart(t *testing.T) {
	got := KillToLineStart(State{Runes: []rune("hello world"), Cursor: 6})
	assert.Equal(t, "world", got.String())
	assert.Equal(t, 0, got.Cursor)
}

func TestKillToLineStart_multiline(t *testing.T) {
	// cursor sits between "bar" and " baz"; kill-to-line-start removes "bar"
	// but leaves the rest of the line and the previous line untouched.
	got := KillToLineStart(State{Runes: []rune("foo\nbar baz"), Cursor: 7, AllowNewline: true})
	assert.Equal(t, "foo\n baz", got.String())
	assert.Equal(t, 4, got.Cursor)
}

func TestKillToLineEnd(t *testing.T) {
	got := KillToLineEnd(State{Runes: []rune("hello world"), Cursor: 5})
	assert.Equal(t, "hello", got.String())
	assert.Equal(t, 5, got.Cursor)
}

func TestKillToLineEnd_multiline(t *testing.T) {
	got := KillToLineEnd(State{Runes: []rune("foo bar\nbaz"), Cursor: 3, AllowNewline: true})
	assert.Equal(t, "foo\nbaz", got.String())
	assert.Equal(t, 3, got.Cursor)
}

func TestKillWordBack(t *testing.T) {
	tests := []struct {
		name       string
		text       string
		cursor     int
		wantText   string
		wantCursor int
	}{
		{"end of word", "hello world", 11, "hello ", 6},
		{"with trailing space", "hello world ", 12, "hello ", 6},
		{"multiple spaces", "foo   bar", 9, "foo   ", 6},
		{"at start", "hello", 0, "hello", 0},
		{"path-like", "/usr/local/bin", 14, "", 0},
		{"unicode", "привет мир", 10, "привет ", 7},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := KillWordBack(State{Runes: []rune(tt.text), Cursor: tt.cursor})
			assert.Equal(t, tt.wantText, got.String())
			assert.Equal(t, tt.wantCursor, got.Cursor)
		})
	}
}

func TestMoveLine_up(t *testing.T) {
	// "foo\nbarbaz" — cursor at column 5 of "barbaz" → moving up keeps column.
	got := MoveLine([]rune("foo\nbarbaz"), 9, -1)
	assert.Equal(t, 3, got, "should clamp to end of shorter previous line")
}

func TestMoveLine_down(t *testing.T) {
	got := MoveLine([]rune("foobar\nqux"), 5, +1)
	assert.Equal(t, 10, got, "should clamp to end of shorter next line")
}

func TestApply_textInsertion(t *testing.T) {
	s := State{Runes: []rune("hello"), Cursor: 5}
	s, ok := Apply(s, key("!", "!"))
	require.True(t, ok)
	assert.Equal(t, "hello!", s.String())
	assert.Equal(t, 6, s.Cursor)
}

func TestApply_backspace(t *testing.T) {
	s := State{Runes: []rune("hello"), Cursor: 5}
	s, ok := Apply(s, namedKey("backspace"))
	require.True(t, ok)
	assert.Equal(t, "hell", s.String())
	assert.Equal(t, 4, s.Cursor)
}

func TestApply_delete(t *testing.T) {
	s := State{Runes: []rune("hello"), Cursor: 0}
	s, ok := Apply(s, namedKey("delete"))
	require.True(t, ok)
	assert.Equal(t, "ello", s.String())
	assert.Equal(t, 0, s.Cursor)
}

func TestApply_ctrlA_ctrlE_singleLine(t *testing.T) {
	s := State{Runes: []rune("hello"), Cursor: 3}
	s, ok := Apply(s, ctrl("a"))
	require.True(t, ok)
	assert.Equal(t, 0, s.Cursor)

	s, ok = Apply(s, ctrl("e"))
	require.True(t, ok)
	assert.Equal(t, 5, s.Cursor)
}

func TestApply_ctrlA_ctrlE_multiline(t *testing.T) {
	s := State{Runes: []rune("foo\nbar baz"), Cursor: 7, AllowNewline: true}
	s, ok := Apply(s, ctrl("a"))
	require.True(t, ok)
	assert.Equal(t, 4, s.Cursor)

	s, ok = Apply(s, ctrl("e"))
	require.True(t, ok)
	assert.Equal(t, 11, s.Cursor)
}

func TestApply_ctrlU(t *testing.T) {
	s := State{Runes: []rune("hello world"), Cursor: 6}
	s, ok := Apply(s, ctrl("u"))
	require.True(t, ok)
	assert.Equal(t, "world", s.String())
	assert.Equal(t, 0, s.Cursor)
}

func TestApply_ctrlK(t *testing.T) {
	s := State{Runes: []rune("hello world"), Cursor: 5}
	s, ok := Apply(s, ctrl("k"))
	require.True(t, ok)
	assert.Equal(t, "hello", s.String())
	assert.Equal(t, 5, s.Cursor)
}

func TestApply_ctrlW(t *testing.T) {
	s := State{Runes: []rune("hello world"), Cursor: 11}
	s, ok := Apply(s, ctrl("w"))
	require.True(t, ok)
	assert.Equal(t, "hello ", s.String())
	assert.Equal(t, 6, s.Cursor)
}

func TestApply_altBackspace(t *testing.T) {
	s := State{Runes: []rune("hello world"), Cursor: 11}
	s, ok := Apply(s, alt(tea.KeyBackspace))
	require.True(t, ok)
	assert.Equal(t, "hello ", s.String())
}

func TestApply_altB_altF(t *testing.T) {
	s := State{Runes: []rune("hello world foo"), Cursor: 15}
	s, ok := Apply(s, alt('b'))
	require.True(t, ok)
	assert.Equal(t, 12, s.Cursor)

	s, ok = Apply(s, alt('f'))
	require.True(t, ok)
	assert.Equal(t, 15, s.Cursor)
}

func TestApply_enter_textareaInsertsNewline(t *testing.T) {
	s := State{Runes: []rune("hello"), Cursor: 5, AllowNewline: true}
	s, ok := Apply(s, namedKey("enter"))
	require.True(t, ok)
	assert.Equal(t, "hello\n", s.String())
	assert.Equal(t, 6, s.Cursor)
}

func TestApply_enter_singleLineDeclines(t *testing.T) {
	s := State{Runes: []rune("hello"), Cursor: 5}
	_, ok := Apply(s, namedKey("enter"))
	assert.False(t, ok, "single-line caller decides what enter means")
}

func TestApply_up_singleLineDeclines(t *testing.T) {
	s := State{Runes: []rune("hello"), Cursor: 5}
	_, ok := Apply(s, namedKey("up"))
	assert.False(t, ok)
}

func TestApply_appendOnlySemantics(t *testing.T) {
	// caller treats the buffer as append-only by keeping Cursor == len.
	// ctrl+u should clear all, ctrl+w should kill the trailing word.
	s := State{Runes: []rune("foo bar"), Cursor: 7}

	cleared, ok := Apply(s, ctrl("u"))
	require.True(t, ok)
	assert.Empty(t, cleared.String())
	assert.Equal(t, 0, cleared.Cursor)

	wiped, ok := Apply(s, ctrl("w"))
	require.True(t, ok)
	assert.Equal(t, "foo ", wiped.String())
	assert.Equal(t, 4, wiped.Cursor)
}

func TestApply_unknownKeyDeclines(t *testing.T) {
	s := State{Runes: []rune("hello"), Cursor: 5}
	_, ok := Apply(s, namedKey("tab"))
	assert.False(t, ok)
	_, ok = Apply(s, namedKey("esc"))
	assert.False(t, ok)
}

func TestInsertText_SingleLineDropsNewlinesAndTabs(t *testing.T) {
	s := State{Runes: []rune("a"), Cursor: 1}
	got := InsertText(s, "b\nc\td")
	// \n and \t become single spaces; cursor advances by the inserted rune length.
	assert.Equal(t, "ab c d", got.String())
	assert.Equal(t, 6, got.Cursor)
}

func TestInsertText_TextareaKeepsNewlinesAndTabs(t *testing.T) {
	s := State{Runes: []rune("a"), Cursor: 1, AllowNewline: true}
	got := InsertText(s, "b\nc\td")
	assert.Equal(t, "ab\nc\td", got.String())
	assert.Equal(t, 6, got.Cursor)
}

func TestInsertText_FiltersControlChars(t *testing.T) {
	// pasting raw escape sequences from a terminal copy must not survive — if
	// they do, the next View() would emit them into the rendered output and
	// could leave the terminal in an inconsistent state.
	s := State{}
	got := InsertText(s, "\x1b[31mred\x1b[0m")
	assert.Equal(t, "[31mred[0m", got.String())
	// only the escape rune itself is filtered; surrounding printable chars stay.
}

func TestInsertText_DropsDelAndOtherC0(t *testing.T) {
	s := State{}
	got := InsertText(s, "a\x00b\x07c\x7fd")
	assert.Equal(t, "abcd", got.String())
}

func TestInsertText_NormalisesCRLF(t *testing.T) {
	// Windows clipboards emit \r\n; we keep one separator (or space in
	// single-line mode), never the \r itself.
	multi := InsertText(State{AllowNewline: true}, "a\r\nb")
	assert.Equal(t, "a\nb", multi.String())

	single := InsertText(State{}, "a\r\nb")
	assert.Equal(t, "a b", single.String())
}

func TestInsertText_EmptyTextIsNoop(t *testing.T) {
	s := State{Runes: []rune("abc"), Cursor: 2}
	got := InsertText(s, "")
	assert.Equal(t, "abc", got.String())
	assert.Equal(t, 2, got.Cursor)
}

func TestInsertText_ClampsCursorBeforeInsert(t *testing.T) {
	s := State{Runes: []rune("ab"), Cursor: 99}
	got := InsertText(s, "X")
	assert.Equal(t, "abX", got.String())
	assert.Equal(t, 3, got.Cursor)
}

func TestPublicHelpers_TolerateOutOfRangeCursor(t *testing.T) {
	// pinning the public contract: helpers used outside Apply must not panic
	// when handed a cursor past the buffer (e.g. stale snapshot, raw caller
	// that hasn't clamped).
	r := []rune("abc")
	assert.NotPanics(t, func() {
		LineStart(r, 99)
		LineEnd(r, 99)
		WordBoundaryBack(r, 99)
		WordBoundaryForward(r, 99)
		MoveLine(r, 99, -1)
		MoveLine(r, 99, +1)
		KillToLineStart(State{Runes: r, Cursor: 99})
		KillToLineEnd(State{Runes: r, Cursor: 99})
		KillWordBack(State{Runes: r, Cursor: 99})
	})
}

func TestApply_clampsOutOfRangeCursor(t *testing.T) {
	s := State{Runes: []rune("ab"), Cursor: 99}
	s, ok := Apply(s, namedKey("backspace"))
	require.True(t, ok)
	assert.Equal(t, "a", s.String())
	assert.Equal(t, 1, s.Cursor)
}

// ----- key constructors -----

func namedKey(name string) tea.KeyPressMsg {
	switch name {
	case "enter":
		return tea.KeyPressMsg{Code: tea.KeyEnter}
	case "esc":
		return tea.KeyPressMsg{Code: tea.KeyEscape}
	case "tab":
		return tea.KeyPressMsg{Code: tea.KeyTab}
	case "backspace":
		return tea.KeyPressMsg{Code: tea.KeyBackspace}
	case "delete":
		return tea.KeyPressMsg{Code: tea.KeyDelete}
	case "up":
		return tea.KeyPressMsg{Code: tea.KeyUp}
	case "down":
		return tea.KeyPressMsg{Code: tea.KeyDown}
	case "left":
		return tea.KeyPressMsg{Code: tea.KeyLeft}
	case "right":
		return tea.KeyPressMsg{Code: tea.KeyRight}
	case "home":
		return tea.KeyPressMsg{Code: tea.KeyHome}
	case "end":
		return tea.KeyPressMsg{Code: tea.KeyEnd}
	}
	panic("unknown named key: " + name)
}

func key(code, text string) tea.KeyPressMsg {
	r, _ := utf8.DecodeRuneInString(code)
	return tea.KeyPressMsg{Code: r, Text: text}
}

func ctrl(letter string) tea.KeyPressMsg {
	r, _ := utf8.DecodeRuneInString(letter)
	return tea.KeyPressMsg{Code: r, Mod: tea.ModCtrl}
}

// alt builds an alt-modified key. No Text is set: real alt-modified keystrokes
// come from terminals as escape sequences with no printable text, and Key.String
// returns Text verbatim when present — keeping Text empty here lets String
// resolve to "alt+<code>".
func alt(code rune) tea.KeyPressMsg {
	return tea.KeyPressMsg{Code: code, Mod: tea.ModAlt}
}
