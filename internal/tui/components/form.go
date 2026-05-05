package components

import (
	"fmt"
	"slices"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// FieldKind enumerates form field types.
type FieldKind int

const (
	FieldText FieldKind = iota
	FieldDropdown
	// FieldList is a dynamic list of strings (Add/Remove).
	FieldList
	FieldTextarea
	// FieldSegmented is a compact single-choice picker rendered as
	// "◂ value ▸". Enter opens a popup with the full vertical option list;
	// esc in popup reverts.
	FieldSegmented
)

// Field describes one input on the form.
type Field struct {
	Key     string
	Label   string
	Kind    FieldKind
	Value   string   // text/textarea: current text; dropdown: selected option
	Options []string // dropdown choices
	List    []string // list-mode entries

	// Validator, when non-nil, is called for each non-empty list entry
	// during render and on commit-and-continue. Empty entries are skipped.
	Validator func(entry string) error

	listCursor      int
	listEntryCursor int
	textCursor      int

	popupOpen     bool
	popupOriginal string // FieldSegmented: value at the moment popup opened
}

// Form is a vertical stack of Fields navigated with tab / shift+tab.
//
// `editing` controls cursor visibility on text-like fields. Hosting screens
// implementing modal editing toggle this so the caret is hidden in command mode.
type Form struct {
	fields  []Field
	focus   int
	editing bool

	focusedSuffix string

	styles theme.Styles
}

func NewForm(fields []Field, opts ...FormOption) *Form {
	f := &Form{
		fields:  append([]Field(nil), fields...),
		styles:  theme.DefaultStyles(),
		editing: true,
	}
	for i := range f.fields {
		f.fields[i].textCursor = runeLen(f.fields[i].Value)
		if f.fields[i].Kind == FieldList && len(f.fields[i].List) > 0 {
			f.fields[i].listEntryCursor = runeLen(f.fields[i].List[f.fields[i].listCursor])
		}
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

type FormOption func(*Form)

func WithFormStyles(s theme.Styles) FormOption {
	return func(f *Form) { f.styles = s }
}

func (f *Form) Focused() int { return f.focus }

func (f *Form) FocusedField() Field { return f.fields[f.focus] }

func (f *Form) CursorAt(key string) int {
	for _, fld := range f.fields {
		if fld.Key == key {
			return fld.textCursor
		}
	}
	return 0
}

func (f *Form) Field(key string) (Field, bool) {
	for _, fld := range f.fields {
		if fld.Key == key {
			return fld, true
		}
	}
	return Field{}, false
}

func (f *Form) Fields() []Field {
	out := make([]Field, len(f.fields))
	copy(out, f.fields)
	return out
}

// SetValue updates the value of a text/textarea/dropdown field. List fields
// use SetList instead. The cursor is moved to the end of the new value.
func (f *Form) SetValue(key, value string) {
	for i := range f.fields {
		if f.fields[i].Key == key {
			f.fields[i].Value = value
			f.fields[i].textCursor = runeLen(value)
			return
		}
	}
}

// SetOptions replaces the Options of a FieldDropdown / FieldSegmented field.
// If the current Value is no longer in the new option set, it falls back to
// the first option (or "" when the list is empty).
func (f *Form) SetOptions(key string, opts []string) {
	for i := range f.fields {
		if f.fields[i].Key != key {
			continue
		}
		fld := &f.fields[i]
		if fld.Kind != FieldDropdown && fld.Kind != FieldSegmented {
			return
		}
		fld.Options = append([]string(nil), opts...)
		if len(opts) == 0 {
			fld.Value = ""
			return
		}
		if !slices.Contains(opts, fld.Value) {
			fld.Value = opts[0]
		}
		return
	}
}

func (f *Form) SetList(key string, entries []string) {
	for i := range f.fields {
		if f.fields[i].Key == key && f.fields[i].Kind == FieldList {
			f.fields[i].List = append([]string(nil), entries...)
			f.fields[i].listCursor = 0
			if len(entries) > 0 {
				f.fields[i].listEntryCursor = runeLen(entries[0])
			} else {
				f.fields[i].listEntryCursor = 0
			}
			return
		}
	}
}

// FocusedListEntry returns the value and index of the focused row in the
// focused FieldList, or false when the field is not a list / has no rows.
func (f *Form) FocusedListEntry() (string, int, bool) {
	if len(f.fields) == 0 {
		return "", 0, false
	}
	fld := &f.fields[f.focus]
	if fld.Kind != FieldList || len(fld.List) == 0 {
		return "", 0, false
	}
	idx := fld.listCursor
	if idx < 0 || idx >= len(fld.List) {
		return "", idx, false
	}
	return fld.List[idx], idx, true
}

// ValidateFocusedListEntry runs the focused list field's Validator on the
// focused entry. Returns nil for empty entries, no validator, or non-list fields.
func (f *Form) ValidateFocusedListEntry() error {
	if len(f.fields) == 0 {
		return nil
	}
	fld := &f.fields[f.focus]
	if fld.Kind != FieldList || fld.Validator == nil {
		return nil
	}
	idx := fld.listCursor
	if idx < 0 || idx >= len(fld.List) {
		return nil
	}
	entry := fld.List[idx]
	if entry == "" {
		return nil
	}
	return fld.Validator(entry)
}

func (f *Form) ListEntryCursor(key string) int {
	for _, fld := range f.fields {
		if fld.Key == key {
			return fld.listEntryCursor
		}
	}
	return 0
}

func (f *Form) FocusKey(key string) {
	for i := range f.fields {
		if f.fields[i].Key == key {
			f.focus = i
			return
		}
	}
}

func (f *Form) FocusNext() {
	if len(f.fields) == 0 {
		return
	}
	f.closeFocusedPopup()
	f.focus = (f.focus + 1) % len(f.fields)
}

func (f *Form) FocusPrev() {
	if len(f.fields) == 0 {
		return
	}
	f.closeFocusedPopup()
	f.focus = (f.focus - 1 + len(f.fields)) % len(f.fields)
}

// SetEditing toggles whether text-like fields render their caret.
func (f *Form) SetEditing(on bool) { f.editing = on }

// SetFocusedSuffix sets a short tag rendered next to the focused field's
// label (e.g. "[EDIT]"). Empty string hides it.
func (f *Form) SetFocusedSuffix(s string) { f.focusedSuffix = s }

func (f *Form) Editing() bool { return f.editing }

// PopupActive reports whether the focused field has a modal popup open
// (currently only FieldSegmented).
func (f *Form) PopupActive() bool {
	if len(f.fields) == 0 {
		return false
	}
	return f.fields[f.focus].popupOpen
}

// closeFocusedPopup drops popup state on the focused field; the value has
// already been live-updated as the user navigated.
func (f *Form) closeFocusedPopup() {
	if len(f.fields) == 0 {
		return
	}
	fld := &f.fields[f.focus]
	fld.popupOpen = false
	fld.popupOriginal = ""
}

func (f *Form) Update(msg tea.Msg) (*Form, tea.Cmd) {
	key, ok := msg.(tea.KeyPressMsg)
	if !ok {
		return f, nil
	}
	switch key.String() {
	case "tab":
		f.FocusNext()
		return f, nil
	case "shift+tab":
		f.FocusPrev()
		return f, nil
	}
	if len(f.fields) == 0 {
		return f, nil
	}
	fld := &f.fields[f.focus]
	switch fld.Kind {
	case FieldText:
		updateText(fld, key, false)
	case FieldTextarea:
		updateText(fld, key, true)
	case FieldDropdown:
		updateDropdown(fld, key)
	case FieldSegmented:
		updateSegmented(fld, key)
	case FieldList:
		updateList(fld, key)
	}
	return f, nil
}

// RenderField renders a single field by key. Returns "" if missing. Used by
// screens that own their own layout instead of stacking via View().
func (f *Form) RenderField(key string) string {
	for i, fld := range f.fields {
		if fld.Key == key {
			return f.renderField(fld, i == f.focus)
		}
	}
	return ""
}

// InsertAtCursor inserts text at the focused field's cursor position.
// Supported on FieldText, FieldTextarea, and FieldList (writes into the
// focused entry). No-op on other kinds.
func (f *Form) InsertAtCursor(text string) {
	if len(f.fields) == 0 || text == "" {
		return
	}
	fld := &f.fields[f.focus]
	switch fld.Kind {
	case FieldText, FieldTextarea:
		runes := []rune(fld.Value)
		clampCursor(&fld.textCursor, len(runes))
		fld.Value = string(runes[:fld.textCursor]) + text + string(runes[fld.textCursor:])
		fld.textCursor += len([]rune(text))
	case FieldList:
		i := fld.listCursor
		if i < 0 || i >= len(fld.List) {
			return
		}
		runes := []rune(fld.List[i])
		clampCursor(&fld.listEntryCursor, len(runes))
		fld.List[i] = string(runes[:fld.listEntryCursor]) + text + string(runes[fld.listEntryCursor:])
		fld.listEntryCursor += len([]rune(text))
	case FieldDropdown, FieldSegmented:
	}
}

// AppendListRow adds a new empty entry to the focused list field, moves
// the row cursor onto it, and resets the entry cursor. No-op when the
// focused field is not a list.
func (f *Form) AppendListRow() bool {
	if len(f.fields) == 0 {
		return false
	}
	fld := &f.fields[f.focus]
	if fld.Kind != FieldList {
		return false
	}
	fld.List = append(fld.List, "")
	fld.listCursor = len(fld.List) - 1
	fld.listEntryCursor = 0
	return true
}

// RemoveListRow removes the focused row. Returns true when something was
// removed.
func (f *Form) RemoveListRow() bool {
	if len(f.fields) == 0 {
		return false
	}
	fld := &f.fields[f.focus]
	if fld.Kind != FieldList || len(fld.List) == 0 {
		return false
	}
	listRemoveCurrent(fld)
	return true
}

// SetSegmentedPopup forces the popup state of a FieldSegmented value.
func (f *Form) SetSegmentedPopup(key string, open bool) {
	for i := range f.fields {
		if f.fields[i].Key == key && f.fields[i].Kind == FieldSegmented {
			if open && !f.fields[i].popupOpen {
				f.fields[i].popupOriginal = f.fields[i].Value
			}
			f.fields[i].popupOpen = open
			return
		}
	}
}

func (f *Form) View() string {
	if len(f.fields) == 0 {
		return ""
	}
	parts := make([]string, 0, len(f.fields))
	for i, fld := range f.fields {
		focused := i == f.focus
		parts = append(parts, f.renderField(fld, focused))
	}
	return strings.Join(parts, "\n")
}

func (f *Form) renderField(fld Field, focused bool) string {
	label := fld.Label
	if focused {
		label = "▸ " + label
	} else {
		label = "  " + label
	}
	labelStyle := f.styles.HintLabel
	if focused {
		labelStyle = f.styles.HintKey
	}
	header := labelStyle.Render(label)
	if focused && f.focusedSuffix != "" {
		header += " " + f.styles.HintKey.Render(f.focusedSuffix)
	}

	body := ""
	caretOn := focused && f.editing
	switch fld.Kind {
	case FieldText:
		body = renderTextValue(f.styles, fld.Value, fld.textCursor, focused, caretOn, false)
	case FieldTextarea:
		body = renderTextValue(f.styles, fld.Value, fld.textCursor, focused, caretOn, true)
	case FieldDropdown:
		body = renderDropdown(f.styles, fld, focused)
	case FieldSegmented:
		body = renderSegmented(f.styles, fld, focused)
	case FieldList:
		body = renderList(f.styles, fld, focused, caretOn)
	}
	return header + "\n" + body
}

func renderTextValue(s theme.Styles, value string, cursor int, focused, caretOn, multiline bool) string {
	runes := []rune(value)
	if cursor > len(runes) {
		cursor = len(runes)
	}
	if cursor < 0 {
		cursor = 0
	}
	if !multiline {
		if !focused || !caretOn {
			if value == "" {
				return "    " + s.HintLabel.Render("—")
			}
			return "    " + s.Command.Render(value)
		}
		return "    " + renderLineWithCursor(s, s.Command, "", runes, cursor)
	}
	if !focused || !caretOn {
		if value == "" {
			return "    " + s.HintLabel.Render("—")
		}
		lines := strings.Split(value, "\n")
		for i, line := range lines {
			lines[i] = "    " + line
		}
		return s.Command.Render(strings.Join(lines, "\n"))
	}
	lineIdx := 0
	lineStartIdx := 0
	for i := range cursor {
		if runes[i] == '\n' {
			lineIdx++
			lineStartIdx = i + 1
		}
	}
	col := cursor - lineStartIdx
	lines := strings.Split(value, "\n")
	out := make([]string, len(lines))
	for i, line := range lines {
		if i != lineIdx {
			out[i] = "    " + s.Command.Render(line)
			continue
		}
		lr := []rune(line)
		col = min(col, len(lr))
		out[i] = "    " + renderLineWithCursor(s, s.Command, "", lr, col)
	}
	return strings.Join(out, "\n")
}

// renderLineWithCursor renders a line with a reverse-video block cursor at
// col. If col == len(runes), a trailing space stands in for "past EOL".
// `prefix` is rendered with `surround` style and concatenated before the
// "before" segment.
func renderLineWithCursor(s theme.Styles, surround lipgloss.Style, prefix string, runes []rune, col int) string {
	before := string(runes[:col])
	var underCursor, after string
	if col >= len(runes) {
		underCursor = " "
		after = ""
	} else {
		underCursor = string(runes[col])
		after = string(runes[col+1:])
	}
	return surround.Render(prefix+before) + s.Cursor.Render(underCursor) + surround.Render(after)
}

func renderDropdown(s theme.Styles, fld Field, focused bool) string {
	if !focused {
		return "    " + s.Command.Render(fld.Value)
	}
	if len(fld.Options) == 0 {
		return "    " + s.StatusInfo.Render("(no options)")
	}
	parts := make([]string, 0, len(fld.Options))
	for _, opt := range fld.Options {
		marker := "( ) "
		style := s.Command
		if opt == fld.Value {
			marker = "(•) "
			style = s.CommandHL
		}
		parts = append(parts, "    "+style.Render(marker+opt))
	}
	return strings.Join(parts, "\n")
}

func renderSegmented(s theme.Styles, fld Field, focused bool) string {
	if !focused {
		return "    " + s.Command.Render(fld.Value)
	}
	if len(fld.Options) == 0 {
		return "    " + s.StatusInfo.Render("(no options)")
	}
	if !fld.popupOpen {
		body := "◂ " + fld.Value + " ▸"
		hint := s.HintLabel.Render("  (←/→ cycle, enter for list)")
		return "    " + s.CommandHL.Render(body) + hint
	}
	parts := make([]string, 0, len(fld.Options)+1)
	for _, opt := range fld.Options {
		marker := "( ) "
		style := s.Command
		if opt == fld.Value {
			marker = "(•) "
			style = s.CommandHL
		}
		parts = append(parts, "    "+style.Render(marker+opt))
	}
	parts = append(parts, "    "+s.HintLabel.Render("enter confirm  esc cancel"))
	return strings.Join(parts, "\n")
}

func renderList(s theme.Styles, fld Field, focused, caretOn bool) string {
	if len(fld.List) == 0 {
		hint := "    " + s.StatusInfo.Render("(empty)")
		if focused {
			hint += "\n    " + s.HintLabel.Render("press enter to add a row")
		}
		return hint
	}
	parts := make([]string, 0, len(fld.List)+1)
	for i, entry := range fld.List {
		isCurrent := focused && i == fld.listCursor
		prefix := "  - "
		style := s.Command
		if isCurrent {
			prefix = "  ▸ "
			style = s.CommandHL
		}
		var marker string
		if entry != "" && fld.Validator != nil {
			if err := fld.Validator(entry); err != nil {
				marker = "  " + s.StatusErr.Render("! "+err.Error())
			}
		}
		if isCurrent && caretOn {
			runes := []rune(entry)
			c := min(fld.listEntryCursor, len(runes))
			c = max(c, 0)
			parts = append(parts, "  "+renderLineWithCursor(s, style, prefix, runes, c)+marker)
			continue
		}
		parts = append(parts, "  "+style.Render(prefix+entry)+marker)
	}
	if focused {
		parts = append(parts, "    "+s.HintLabel.Render("enter or ctrl+n — add row    ctrl+x — remove row    backspace on empty — remove"))
	}
	return strings.Join(parts, "\n")
}

func updateText(fld *Field, key tea.KeyPressMsg, allowNewline bool) {
	runes := []rune(fld.Value)
	clampCursor(&fld.textCursor, len(runes))
	if textNavigate(fld, runes, key, allowNewline) {
		return
	}
	textEdit(fld, runes, key, allowNewline)
}

func textNavigate(fld *Field, runes []rune, key tea.KeyPressMsg, allowNewline bool) bool {
	n := len(runes)
	switch key.String() {
	case "left":
		if fld.textCursor > 0 {
			fld.textCursor--
		}
	case "right":
		if fld.textCursor < n {
			fld.textCursor++
		}
	case "home":
		if allowNewline {
			fld.textCursor = lineStart(runes, fld.textCursor)
		} else {
			fld.textCursor = 0
		}
	case "end":
		if allowNewline {
			fld.textCursor = lineEnd(runes, fld.textCursor)
		} else {
			fld.textCursor = n
		}
	case "up":
		if !allowNewline {
			return false
		}
		fld.textCursor = moveLine(runes, fld.textCursor, -1)
	case "down":
		if !allowNewline {
			return false
		}
		fld.textCursor = moveLine(runes, fld.textCursor, +1)
	default:
		return false
	}
	return true
}

func textEdit(fld *Field, runes []rune, key tea.KeyPressMsg, allowNewline bool) {
	n := len(runes)
	switch key.String() {
	case "backspace":
		if fld.textCursor > 0 {
			fld.Value = string(runes[:fld.textCursor-1]) + string(runes[fld.textCursor:])
			fld.textCursor--
		}
	case "delete":
		if fld.textCursor < n {
			fld.Value = string(runes[:fld.textCursor]) + string(runes[fld.textCursor+1:])
		}
	case "enter":
		if allowNewline {
			fld.Value = string(runes[:fld.textCursor]) + "\n" + string(runes[fld.textCursor:])
			fld.textCursor++
		}
	default:
		if t := key.Text; t != "" {
			fld.Value = string(runes[:fld.textCursor]) + t + string(runes[fld.textCursor:])
			fld.textCursor += len([]rune(t))
		}
	}
}

func clampCursor(cursor *int, n int) {
	if *cursor > n {
		*cursor = n
	}
	if *cursor < 0 {
		*cursor = 0
	}
}

func lineStart(runes []rune, cursor int) int {
	i := cursor
	for i > 0 && runes[i-1] != '\n' {
		i--
	}
	return i
}

func lineEnd(runes []rune, cursor int) int {
	i := cursor
	for i < len(runes) && runes[i] != '\n' {
		i++
	}
	return i
}

// moveLine moves cursor by delta lines preserving the visual column.
// delta = -1 up, +1 down.
func moveLine(runes []rune, cursor, delta int) int {
	curStart := lineStart(runes, cursor)
	col := cursor - curStart
	if delta < 0 {
		if curStart == 0 {
			return cursor
		}
		prevEnd := curStart - 1
		prevStart := lineStart(runes, prevEnd)
		prevLen := prevEnd - prevStart
		if col > prevLen {
			col = prevLen
		}
		return prevStart + col
	}
	curEnd := lineEnd(runes, cursor)
	if curEnd >= len(runes) {
		return cursor
	}
	nextStart := curEnd + 1
	nextEnd := lineEnd(runes, nextStart)
	nextLen := nextEnd - nextStart
	if col > nextLen {
		col = nextLen
	}
	return nextStart + col
}

func runeLen(s string) int { return len([]rune(s)) }

func updateSegmented(fld *Field, key tea.KeyPressMsg) {
	if len(fld.Options) == 0 {
		return
	}
	idx := indexOf(fld.Options, fld.Value)
	switch key.String() {
	case "right", "down", "j", "l":
		if idx < 0 {
			idx = 0
		} else {
			idx = (idx + 1) % len(fld.Options)
		}
		fld.Value = fld.Options[idx]
	case "left", "up", "k", "h":
		if idx < 0 {
			idx = 0
		} else {
			idx = (idx - 1 + len(fld.Options)) % len(fld.Options)
		}
		fld.Value = fld.Options[idx]
	case "enter":
		if fld.popupOpen {
			fld.popupOpen = false
			fld.popupOriginal = ""
		} else {
			fld.popupOpen = true
			fld.popupOriginal = fld.Value
		}
	case "esc":
		if fld.popupOpen {
			fld.Value = fld.popupOriginal
			fld.popupOpen = false
			fld.popupOriginal = ""
		}
	}
}

func updateDropdown(fld *Field, key tea.KeyPressMsg) {
	if len(fld.Options) == 0 {
		return
	}
	idx := indexOf(fld.Options, fld.Value)
	switch key.String() {
	case "j", "down", "right":
		if idx < 0 {
			idx = 0
		} else {
			idx = (idx + 1) % len(fld.Options)
		}
		fld.Value = fld.Options[idx]
	case "k", "up", "left":
		if idx < 0 {
			idx = 0
		} else {
			idx = (idx - 1 + len(fld.Options)) % len(fld.Options)
		}
		fld.Value = fld.Options[idx]
	}
}

// updateList handles list-mode editing with a per-entry rune cursor.
// Up/down move between rows; backspace on an empty entry removes the row.
func updateList(fld *Field, key tea.KeyPressMsg) {
	if listStructural(fld, key) {
		return
	}
	i := fld.listCursor
	if i < 0 || i >= len(fld.List) {
		return
	}
	runes := []rune(fld.List[i])
	clampCursor(&fld.listEntryCursor, len(runes))
	if listEntryNavigate(fld, runes, key) {
		return
	}
	listEntryEdit(fld, runes, key)
}

func listStructural(fld *Field, key tea.KeyPressMsg) bool {
	switch key.String() {
	case "down":
		if len(fld.List) > 0 {
			fld.listCursor = (fld.listCursor + 1) % len(fld.List)
			fld.listEntryCursor = runeLen(fld.List[fld.listCursor])
		}
	case "up":
		if len(fld.List) > 0 {
			fld.listCursor = (fld.listCursor - 1 + len(fld.List)) % len(fld.List)
			fld.listEntryCursor = runeLen(fld.List[fld.listCursor])
		}
	default:
		return false
	}
	return true
}

func listEntryNavigate(fld *Field, runes []rune, key tea.KeyPressMsg) bool {
	switch key.String() {
	case "left":
		if fld.listEntryCursor > 0 {
			fld.listEntryCursor--
		}
	case "right":
		if fld.listEntryCursor < len(runes) {
			fld.listEntryCursor++
		}
	case "home":
		fld.listEntryCursor = 0
	case "end":
		fld.listEntryCursor = len(runes)
	default:
		return false
	}
	return true
}

// listEntryEdit handles edit keys inside the focused entry. Backspace on an
// empty entry removes the row.
func listEntryEdit(fld *Field, runes []rune, key tea.KeyPressMsg) {
	i := fld.listCursor
	n := len(runes)
	switch key.String() {
	case "backspace":
		if n == 0 {
			listRemoveCurrent(fld)
			return
		}
		if fld.listEntryCursor > 0 {
			fld.List[i] = string(runes[:fld.listEntryCursor-1]) + string(runes[fld.listEntryCursor:])
			fld.listEntryCursor--
		}
	case "delete":
		if fld.listEntryCursor < n {
			fld.List[i] = string(runes[:fld.listEntryCursor]) + string(runes[fld.listEntryCursor+1:])
		}
	default:
		if t := key.Text; t != "" {
			fld.List[i] = string(runes[:fld.listEntryCursor]) + t + string(runes[fld.listEntryCursor:])
			fld.listEntryCursor += len([]rune(t))
		}
	}
}

func listRemoveCurrent(fld *Field) {
	i := fld.listCursor
	if i < 0 || i >= len(fld.List) {
		return
	}
	fld.List = append(fld.List[:i], fld.List[i+1:]...)
	if fld.listCursor >= len(fld.List) {
		fld.listCursor = max(0, len(fld.List)-1)
	}
	if len(fld.List) == 0 {
		fld.listEntryCursor = 0
	} else {
		fld.listEntryCursor = runeLen(fld.List[fld.listCursor])
	}
}

func indexOf(items []string, target string) int {
	for i, v := range items {
		if v == target {
			return i
		}
	}
	return -1
}

func (k FieldKind) String() string {
	switch k {
	case FieldText:
		return "text"
	case FieldDropdown:
		return "dropdown"
	case FieldList:
		return "list"
	case FieldTextarea:
		return "textarea"
	case FieldSegmented:
		return "segmented"
	}
	return fmt.Sprintf("kind(%d)", int(k))
}
