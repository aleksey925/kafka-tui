// Package lineedit provides readline-style line-editing primitives shared by
// all text inputs in the TUI (form fields, the `/` filter, the `:` command
// bar). Word boundaries follow readline's unix-word-rubout (whitespace-only),
// and kill operations (ctrl+u / ctrl+k / ctrl+w) act on the current line —
// they stop at `\n`, not at the buffer edge.
//
// State is plain data (runes + cursor); helpers are pure functions; the
// dispatcher [Apply] turns a [tea.KeyPressMsg] into a state update so call
// sites don't have to repeat the switch.
package lineedit

import (
	"unicode"

	tea "charm.land/bubbletea/v2"
)

// State is the input buffer plus a cursor position measured in runes.
//
// AllowNewline switches semantics that depend on whether `\n` is a valid
// character: enter inserts `\n` only when true, and up/down only navigate
// across visual lines when true.
type State struct {
	Runes        []rune
	Cursor       int
	AllowNewline bool
}

// FromString builds a State from a string with the cursor placed at the end.
func FromString(s string, allowNewline bool) State {
	r := []rune(s)
	return State{Runes: r, Cursor: len(r), AllowNewline: allowNewline}
}

// String returns the buffer as a string.
func (s State) String() string { return string(s.Runes) }

// Clamp ensures Cursor is within [0, len(Runes)].
func (s State) Clamp() State {
	if s.Cursor < 0 {
		s.Cursor = 0
	}
	if s.Cursor > len(s.Runes) {
		s.Cursor = len(s.Runes)
	}
	return s
}

// RuneLen returns the number of runes in s. Provided as a single source of
// truth so call sites that track rune-based cursor offsets don't each ship
// their own copy.
func RuneLen(s string) int { return len([]rune(s)) }

// clampCursor returns cur projected into [0, len(runes)] so callers can pass
// raw cursor values without risking out-of-bounds indexing inside the helpers.
func clampCursor(runes []rune, cur int) int {
	if cur < 0 {
		return 0
	}
	if cur > len(runes) {
		return len(runes)
	}
	return cur
}

// LineStart returns the index of the first rune of the line containing cur.
// cur is clamped to [0, len(runes)] before use.
func LineStart(runes []rune, cur int) int {
	i := clampCursor(runes, cur)
	for i > 0 && runes[i-1] != '\n' {
		i--
	}
	return i
}

// LineEnd returns the index of the `\n` ending the line containing cur (or
// len(runes) if it's the last line). cur is clamped to [0, len(runes)].
func LineEnd(runes []rune, cur int) int {
	i := clampCursor(runes, cur)
	for i < len(runes) && runes[i] != '\n' {
		i++
	}
	return i
}

// WordBoundaryBack returns the start of the word ending at cur, using the
// readline unix-word-rubout rule: skip whitespace runes, then skip
// non-whitespace runes. cur is clamped to [0, len(runes)].
func WordBoundaryBack(runes []rune, cur int) int {
	i := clampCursor(runes, cur)
	for i > 0 && unicode.IsSpace(runes[i-1]) {
		i--
	}
	for i > 0 && !unicode.IsSpace(runes[i-1]) {
		i--
	}
	return i
}

// WordBoundaryForward returns the position past the word starting at cur,
// using the same rule as [WordBoundaryBack]: skip whitespace, then skip
// non-whitespace. cur is clamped to [0, len(runes)].
func WordBoundaryForward(runes []rune, cur int) int {
	n := len(runes)
	i := clampCursor(runes, cur)
	for i < n && unicode.IsSpace(runes[i]) {
		i++
	}
	for i < n && !unicode.IsSpace(runes[i]) {
		i++
	}
	return i
}

// MoveLine moves cur by delta lines (delta = -1 up, +1 down), preserving the
// visual column. cur is clamped to [0, len(runes)]. Callers gate up/down by
// AllowNewline themselves (see [Apply]); MoveLine always honors line breaks
// present in runes.
func MoveLine(runes []rune, cur, delta int) int {
	cur = clampCursor(runes, cur)
	curStart := LineStart(runes, cur)
	col := cur - curStart
	if delta < 0 {
		if curStart == 0 {
			return cur
		}
		prevEnd := curStart - 1
		prevStart := LineStart(runes, prevEnd)
		prevLen := prevEnd - prevStart
		if col > prevLen {
			col = prevLen
		}
		return prevStart + col
	}
	curEnd := LineEnd(runes, cur)
	if curEnd >= len(runes) {
		return cur
	}
	nextStart := curEnd + 1
	nextEnd := LineEnd(runes, nextStart)
	nextLen := nextEnd - nextStart
	if col > nextLen {
		col = nextLen
	}
	return nextStart + col
}

// killRange removes runes[from:to] and clamps the cursor.
func killRange(s State, from, to int) State {
	if from < 0 {
		from = 0
	}
	if to > len(s.Runes) {
		to = len(s.Runes)
	}
	if from >= to {
		return s
	}
	s.Runes = append(s.Runes[:from], s.Runes[to:]...)
	switch {
	case s.Cursor >= to:
		s.Cursor -= to - from
	case s.Cursor > from:
		s.Cursor = from
	}
	return s
}

// KillToLineStart removes runes from the start of the current line up to the
// cursor. The cursor moves to the start of the line.
func KillToLineStart(s State) State {
	return killRange(s, LineStart(s.Runes, s.Cursor), s.Cursor)
}

// KillToLineEnd removes runes from the cursor to the end of the current line
// (not consuming the trailing `\n`). The cursor is unchanged.
func KillToLineEnd(s State) State {
	return killRange(s, s.Cursor, LineEnd(s.Runes, s.Cursor))
}

// KillWordBack removes the word before the cursor (see [WordBoundaryBack]).
// When the cursor sits on a word, the partial prefix is also removed.
func KillWordBack(s State) State {
	return killRange(s, WordBoundaryBack(s.Runes, s.Cursor), s.Cursor)
}

// insertText inserts the given text at the cursor and advances the cursor by
// its rune length.
func insertText(s State, text string) State {
	if text == "" {
		return s
	}
	in := []rune(text)
	merged := make([]rune, 0, len(s.Runes)+len(in))
	merged = append(merged, s.Runes[:s.Cursor]...)
	merged = append(merged, in...)
	merged = append(merged, s.Runes[s.Cursor:]...)
	s.Runes = merged
	s.Cursor += len(in)
	return s
}

// Apply dispatches readline-style edit keys against s. When handled is true
// the caller should adopt the returned state; when false the key has no
// edit-level meaning and the caller is free to interpret it (e.g. enter on a
// single-line field, tab on a non-textarea, esc, etc.).
//
// `enter` is handled only when AllowNewline is true (inserts `\n`). `tab` and
// `esc` are never handled here.
func Apply(s State, key tea.KeyPressMsg) (State, bool) {
	s = s.Clamp()
	if next, ok := applyNav(s, key); ok {
		return next, true
	}
	if next, ok := applyKill(s, key); ok {
		return next, true
	}
	if key.String() == "enter" {
		if !s.AllowNewline {
			return s, false
		}
		return insertText(s, "\n"), true
	}
	if t := key.Text; t != "" {
		return insertText(s, t), true
	}
	return s, false
}

// applyNav handles cursor-movement keys.
func applyNav(s State, key tea.KeyPressMsg) (State, bool) {
	n := len(s.Runes)
	switch key.String() {
	case "left":
		if s.Cursor > 0 {
			s.Cursor--
		}
	case "right":
		if s.Cursor < n {
			s.Cursor++
		}
	case "home", "ctrl+a":
		if s.AllowNewline {
			s.Cursor = LineStart(s.Runes, s.Cursor)
		} else {
			s.Cursor = 0
		}
	case "end", "ctrl+e":
		if s.AllowNewline {
			s.Cursor = LineEnd(s.Runes, s.Cursor)
		} else {
			s.Cursor = n
		}
	case "up":
		if !s.AllowNewline {
			return s, false
		}
		s.Cursor = MoveLine(s.Runes, s.Cursor, -1)
	case "down":
		if !s.AllowNewline {
			return s, false
		}
		s.Cursor = MoveLine(s.Runes, s.Cursor, +1)
	case "alt+b":
		s.Cursor = WordBoundaryBack(s.Runes, s.Cursor)
	case "alt+f":
		s.Cursor = WordBoundaryForward(s.Runes, s.Cursor)
	default:
		return s, false
	}
	return s, true
}

// applyKill handles deletion keys (single char + readline kill family).
func applyKill(s State, key tea.KeyPressMsg) (State, bool) {
	n := len(s.Runes)
	switch key.String() {
	case "backspace":
		if s.Cursor > 0 {
			s = killRange(s, s.Cursor-1, s.Cursor)
		}
	case "delete":
		if s.Cursor < n {
			s = killRange(s, s.Cursor, s.Cursor+1)
		}
	case "ctrl+u":
		s = KillToLineStart(s)
	case "ctrl+k":
		s = KillToLineEnd(s)
	case "ctrl+w", "alt+backspace":
		s = KillWordBack(s)
	default:
		return s, false
	}
	return s, true
}
