package components

import (
	"fmt"
	"slices"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/lineedit"
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
//
// `defaults` is a snapshot of the value-side state (Value / List / cursors)
// captured at construction time. [Form.Reset] restores from it without touching
// Options injected later via [Form.SetOptions] — see the comment on Reset for
// the contract.
//
// `viewports` are bounded scrollable regions for textarea / list fields,
// lazily allocated per field key. They survive Reset (scroll position is
// cleared, content is owned by the field) so a screen that holds the same
// Form pointer keeps its bounded rendering across clears.
type Form struct {
	fields   []Field
	defaults []Field
	focus    int
	editing  bool

	focusedSuffix string

	width, height int
	viewports     map[string]*Viewport

	styles theme.Styles
}

func NewForm(fields []Field, opts ...FormOption) *Form {
	f := &Form{
		fields:  append([]Field(nil), fields...),
		styles:  theme.DefaultStyles(),
		editing: true,
	}
	for i := range f.fields {
		f.fields[i].textCursor = lineedit.RuneLen(f.fields[i].Value)
		if f.fields[i].Kind == FieldList && len(f.fields[i].List) > 0 {
			f.fields[i].listEntryCursor = lineedit.RuneLen(f.fields[i].List[f.fields[i].listCursor])
		}
	}
	f.defaults = snapshotDefaults(f.fields)
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// snapshotDefaults captures the value-side state of each field at construction
// time. List is deep-copied so later in-place mutations (AppendListRow,
// listRemoveCurrent) can't bleed back into the snapshot.
func snapshotDefaults(fields []Field) []Field {
	out := make([]Field, len(fields))
	for i, fld := range fields {
		out[i] = Field{
			Value:           fld.Value,
			List:            append([]string(nil), fld.List...),
			textCursor:      fld.textCursor,
			listCursor:      fld.listCursor,
			listEntryCursor: fld.listEntryCursor,
		}
	}
	return out
}

// Reset restores every field's Value, List and cursor positions to the state
// captured at construction time and moves focus back to the first field.
// Options injected later via [Form.SetOptions] survive — they are field
// structure, not value, so a screen that loaded async data (e.g. partition
// lists) does not lose it on reset. Kind, Label, Key and Validator are also
// preserved. Any open segmented popup is closed.
//
// The host-owned visual state (editing flag, focused-suffix) is intentionally
// not touched: the host re-applies it as part of its own mode-restore logic
// (see [Form.SetEditing] / [Form.SetFocusedSuffix]).
func (f *Form) Reset() {
	for i := range f.fields {
		d := f.defaults[i]
		f.fields[i].Value = d.Value
		f.fields[i].List = append([]string(nil), d.List...)
		f.fields[i].textCursor = d.textCursor
		f.fields[i].listCursor = d.listCursor
		f.fields[i].listEntryCursor = d.listEntryCursor
		f.fields[i].popupOpen = false
		f.fields[i].popupOriginal = ""
	}
	for _, v := range f.viewports {
		v.Reset()
	}
	f.focus = 0
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
			f.fields[i].textCursor = lineedit.RuneLen(value)
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
				f.fields[i].listEntryCursor = lineedit.RuneLen(entries[0])
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

// Bindings returns the navigation keystrokes Update() recognizes, as
// advertise-only entries (no Handler) so screens embedding the form
// merge them into their own help/hints from one place. Mirrors
// [Menu.Bindings]. Passing category="" hides every entry from help.
func (f *Form) Bindings(category string) []keymap.Binding {
	return []keymap.Binding{
		{Keys: []string{"tab"}, Label: "next field", Category: category},
		{Keys: []string{"shift+tab"}, Label: "previous field", Category: category},
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

// SetSize records the area the form has been allotted by its host. Bounded
// fields (textarea, list) use it to derive their visible-row counts; any
// non-positive component (default zero, or an explicit 0 / negative) reverts
// to natural-height rendering. Hosts should call this from their own SetSize
// so the form re-flows on terminal resize.
func (f *Form) SetSize(w, h int) {
	f.width = w
	f.height = h
}

func (f *Form) Width() int { return f.width }

func (f *Form) Height() int { return f.height }

// fieldViewport returns (creating if needed) the bounded scroller backing a
// textarea or list field. Returns nil for non-bounded kinds so callers can
// gate on it.
func (f *Form) fieldViewport(key string, kind FieldKind) *Viewport {
	if kind != FieldTextarea && kind != FieldList {
		return nil
	}
	if f.viewports == nil {
		f.viewports = make(map[string]*Viewport)
	}
	v, ok := f.viewports[key]
	if !ok {
		v = NewViewport()
		f.viewports[key] = v
	}
	return v
}

// HandleViewportKey forwards a scroll-class key to the focused field's
// viewport, returning true when the key was consumed. Hosts call this in
// NORMAL mode after their own bindings have had a chance to claim the key —
// it lets the user pan around a long textarea / headers list without entering
// INSERT. No-op (returns false) when the focused field is not bounded.
//
// Uses [WindowScrollBindings] so the field's caret / row cursor (set on the
// viewport for rendering purposes) does not get dragged along by j/k —
// list-row navigation is an INSERT-mode concern, not a NORMAL pan action.
// Wrap toggling (w) is not exposed: textareas pre-wrap inside
// [buildTextareaVisualLines] and list rows are pre-built to width, so
// flipping the viewport's wrap flag would not change anything the user
// sees on these fields.
func (f *Form) HandleViewportKey(key tea.KeyPressMsg) bool {
	if len(f.fields) == 0 {
		return false
	}
	fld := f.fields[f.focus]
	v := f.fieldViewport(fld.Key, fld.Kind)
	if v == nil {
		return false
	}
	_, ok := keymap.Dispatch(WindowScrollBindings(v), key)
	return ok
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

// pasteIntoFocused inserts content into the focused text-like field via
// [lineedit.InsertText]. Non-text fields (segmented / dropdown) are silently
// skipped — paste has no meaning there. Single-line fields strip newlines to
// spaces (handled inside InsertText via [lineedit.State.AllowNewline]).
func (f *Form) pasteIntoFocused(content string) {
	if len(f.fields) == 0 || content == "" {
		return
	}
	fld := &f.fields[f.focus]
	switch fld.Kind {
	case FieldText:
		state := lineedit.InsertText(lineedit.State{
			Runes:  []rune(fld.Value),
			Cursor: fld.textCursor,
		}, content)
		fld.Value = state.String()
		fld.textCursor = state.Cursor
	case FieldTextarea:
		state := lineedit.InsertText(lineedit.State{
			Runes:        []rune(fld.Value),
			Cursor:       fld.textCursor,
			AllowNewline: true,
		}, content)
		fld.Value = state.String()
		fld.textCursor = state.Cursor
	case FieldList:
		// list semantics: one entry per line. Multi-line paste must split
		// into separate rows rather than collapse into a single entry —
		// otherwise validators that parse "key=value" silently swallow
		// every line after the first.
		pasteIntoList(fld, content)
	case FieldDropdown, FieldSegmented:
		// paste has no meaning on option pickers — silently drop.
	}
}

// pasteIntoList splits multi-line content along `\n` and threads it through
// the focused entry: the first chunk is inserted at the entry cursor (keeping
// the existing prefix/suffix), and remaining chunks become new entries
// immediately after. Per-chunk sanitization (control chars, stray \r/\t)
// still goes through [lineedit.InsertText] in single-line mode.
func pasteIntoList(fld *Field, content string) {
	if fld.Kind != FieldList || content == "" {
		return
	}
	lines := strings.Split(content, "\n")
	if len(fld.List) == 0 {
		fld.List = append(fld.List, "")
		fld.listCursor = 0
		fld.listEntryCursor = 0
	}
	// inject the first chunk into the current entry at cursor; this keeps
	// the prefix and suffix that already lived in the row.
	i := fld.listCursor
	state := lineedit.InsertText(lineedit.State{
		Runes:  []rune(fld.List[i]),
		Cursor: fld.listEntryCursor,
	}, lines[0])
	first := state.String()
	cursorAfterFirst := state.Cursor

	if len(lines) == 1 {
		fld.List[i] = first
		fld.listEntryCursor = cursorAfterFirst
		return
	}

	// multi-line: split the current entry at the cursor. Everything before
	// stays in the original row (extended with line[0]); everything after
	// rides on the last new row. Lines in between become standalone rows.
	tail := string([]rune(first)[cursorAfterFirst:])
	fld.List[i] = string([]rune(first)[:cursorAfterFirst])
	extras := make([]string, len(lines)-1)
	for j, line := range lines[1:] {
		extras[j] = lineedit.InsertText(lineedit.State{}, line).String()
	}
	extras[len(extras)-1] += tail

	newList := make([]string, 0, len(fld.List)+len(extras))
	newList = append(newList, fld.List[:i+1]...)
	newList = append(newList, extras...)
	newList = append(newList, fld.List[i+1:]...)
	fld.List = newList
	fld.listCursor = i + len(extras)
	// cursor lands at the boundary between the last pasted line and the
	// preserved tail — i.e. exactly where the user would continue typing.
	fld.listEntryCursor = lineedit.RuneLen(extras[len(extras)-1]) - lineedit.RuneLen(tail)
}

// closeFocusedPopup clears the modal popup flag. No commit is needed —
// updateSegmented writes the chosen option into fld.Value on every arrow,
// so the selection is already persisted by the time the popup closes.
func (f *Form) closeFocusedPopup() {
	if len(f.fields) == 0 {
		return
	}
	fld := &f.fields[f.focus]
	fld.popupOpen = false
	fld.popupOriginal = ""
}

func (f *Form) Update(msg tea.Msg) (*Form, tea.Cmd) {
	if paste, ok := msg.(tea.PasteMsg); ok {
		f.pasteIntoFocused(paste.Content)
		return f, nil
	}
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
// screens that own their own layout instead of stacking via View() — most
// notably fullscreen field views in the produce screen, where the focused
// field gets the entire form area.
func (f *Form) RenderField(key string) string {
	for i, fld := range f.fields {
		if fld.Key == key {
			// in standalone-field mode the body gets everything except the
			// 1-line label; 0 falls back to natural rendering when unsized.
			bodyHeight := 0
			if f.height > 1 {
				bodyHeight = f.height - 1
			}
			return f.renderField(fld, i == f.focus, bodyHeight)
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
	bodyHeights := f.allocateBodyHeights()
	parts := make([]string, 0, len(f.fields))
	for i, fld := range f.fields {
		focused := i == f.focus
		parts = append(parts, f.renderField(fld, focused, bodyHeights[i]))
	}
	return strings.Join(parts, "\n")
}

// allocateBodyHeights distributes the form's allotted height across its
// fields. Returns 0 for every entry when SetSize hasn't been called — that's
// the signal to fall back to natural unbounded rendering (legacy behavior).
//
// Policy when sized: textarea is the elastic field and gets the remainder.
// Lists are bounded by max(3, totalHeight/3) visible rows so a long
// headers list can't crowd out the textarea. Fixed-row fields (text,
// dropdown, segmented) take their natural footprint.
func (f *Form) allocateBodyHeights() []int {
	out := make([]int, len(f.fields))
	if f.height <= 0 || f.width <= 0 {
		return out
	}
	// one label line per field; everything else is "body".
	available := max(f.height-len(f.fields), 0)

	listCap := max(3, f.height/3)
	elasticIdx := -1
	for i, fld := range f.fields {
		switch fld.Kind {
		case FieldTextarea:
			elasticIdx = i
		case FieldList:
			visible := len(fld.List)
			if visible == 0 {
				visible = 1 // "(empty)" placeholder
				if i == f.focus {
					visible++ // + "press enter to add a row" hint
				}
			} else {
				if visible > listCap {
					visible = listCap
				}
				if i == f.focus {
					visible++ // shortcut hint line below the rows
				}
			}
			out[i] = visible
			available -= visible
		default:
			h := naturalBodyHeight(fld)
			out[i] = h
			available -= h
		}
	}
	if elasticIdx >= 0 {
		h := available
		const minTextareaHeight = 3
		if h < minTextareaHeight {
			h = minTextareaHeight // clip rather than refuse to render
		}
		out[elasticIdx] = h
	}
	return out
}

func naturalBodyHeight(fld Field) int {
	switch fld.Kind {
	case FieldDropdown:
		if len(fld.Options) == 0 {
			return 1
		}
		return len(fld.Options)
	case FieldSegmented:
		if fld.popupOpen {
			return len(fld.Options) + 1 // options + hint
		}
		return 1
	case FieldText, FieldTextarea, FieldList:
		return 1
	}
	return 1
}

func (f *Form) renderField(fld Field, focused bool, bodyHeight int) string {
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
		body = f.renderTextarea(fld, focused, caretOn, bodyHeight)
	case FieldDropdown:
		body = renderDropdown(f.styles, fld, focused)
	case FieldSegmented:
		body = renderSegmented(f.styles, fld, focused)
	case FieldList:
		body = f.renderListField(fld, focused, caretOn, bodyHeight)
	}
	return header + "\n" + body
}

// renderTextarea is the viewport-aware textarea body renderer. When the form
// hasn't been sized (bodyHeight == 0) it delegates to the legacy unbounded
// renderer so screens that don't propagate SetSize keep their old behavior.
// Otherwise content is wrapped to width-4 (leaving room for the indent
// prefix) and sliced through a per-field viewport: in INSERT the cursor's
// visual line is auto-followed, in NORMAL the scroll position persists so
// the user's reading spot survives mode flips.
func (f *Form) renderTextarea(fld Field, focused, caretOn bool, bodyHeight int) string {
	if bodyHeight <= 0 {
		return renderTextValue(f.styles, fld.Value, fld.textCursor, focused, caretOn, true)
	}
	if (!focused || !caretOn) && fld.Value == "" {
		return "    " + f.styles.HintLabel.Render("—")
	}
	lines, cursorLine := buildTextareaVisualLines(f.styles, fld.Value, fld.textCursor, caretOn, f.width)
	if len(lines) <= bodyHeight {
		return strings.Join(lines, "\n")
	}
	v := f.fieldViewport(fld.Key, FieldTextarea)
	v.SetSize(f.width, bodyHeight)
	v.SetLines(lines)
	if caretOn {
		v.SetCursor(cursorLine)
	} else {
		v.ClearCursor()
	}
	return v.View()
}

// renderListField is the viewport-aware list body renderer. Same fallback
// rule as renderTextarea — unsized form uses the legacy unbounded path so
// existing screens keep their behavior. When sized, rows are sliced through
// a viewport that follows listCursor; the focused-only shortcut hint sits
// below the viewport's window so the user always sees it.
func (f *Form) renderListField(fld Field, focused, caretOn bool, bodyHeight int) string {
	if bodyHeight <= 0 {
		return renderList(f.styles, fld, focused, caretOn)
	}
	if len(fld.List) == 0 {
		hint := "    " + f.styles.StatusInfo.Render("(empty)")
		if focused {
			hint += "\n    " + f.styles.HintLabel.Render("press enter to add a row")
		}
		return hint
	}
	rows := buildListRowLines(f.styles, fld, focused, caretOn)
	rowsHeight := bodyHeight
	if focused {
		rowsHeight = max(bodyHeight-1, 1) // reserve a line for the shortcut hint
	}
	v := f.fieldViewport(fld.Key, FieldList)
	v.SetSize(f.width, rowsHeight)
	v.SetLines(rows)
	if focused {
		v.SetCursor(fld.listCursor)
	} else {
		v.ClearCursor()
	}
	out := v.View()
	if focused {
		out += "\n    " + f.styles.HintLabel.Render("enter or ctrl+n — add row    ctrl+x — remove row    backspace on empty — remove")
	}
	return out
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

// buildTextareaVisualLines builds the wrapped, indented, ANSI-styled visual
// lines that back a viewport-bounded textarea body. Returns the visual line
// of the cursor (or -1 when caretOn is false). When width is non-positive,
// wrapping is skipped — the caller falls back to legacy rendering anyway in
// that case, but this keeps the helper safe to call standalone.
func buildTextareaVisualLines(s theme.Styles, value string, runeCursor int, caretOn bool, width int) ([]string, int) {
	runes := []rune(value)
	if runeCursor > len(runes) {
		runeCursor = len(runes)
	}
	if runeCursor < 0 {
		runeCursor = 0
	}

	cursorLineIdx, cursorCol := 0, 0
	if caretOn {
		lineStart := 0
		for i := range runeCursor {
			if runes[i] == '\n' {
				cursorLineIdx++
				lineStart = i + 1
			}
		}
		cursorCol = runeCursor - lineStart
	}

	logical := strings.Split(value, "\n")
	styledLogical := make([]string, len(logical))
	for i, line := range logical {
		if caretOn && i == cursorLineIdx {
			lr := []rune(line)
			c := min(cursorCol, len(lr))
			styledLogical[i] = renderLineWithCursor(s, s.Command, "", lr, c)
		} else {
			styledLogical[i] = s.Command.Render(line)
		}
	}

	contentWidth := width - 4
	visual := WrapLines(styledLogical, contentWidth)
	for i := range visual {
		visual[i] = "    " + visual[i]
	}

	cursorVisual := -1
	if caretOn {
		cursorVisual = CursorVisualLine(logical, cursorLineIdx, cursorCol, contentWidth, true)
	}
	return visual, cursorVisual
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
	parts := buildListRowLines(s, fld, focused, caretOn)
	if focused {
		parts = append(parts, "    "+s.HintLabel.Render("enter or ctrl+n — add row    ctrl+x — remove row    backspace on empty — remove"))
	}
	return strings.Join(parts, "\n")
}

// buildListRowLines renders each list entry into a single styled visual line,
// including the row marker and any validator error suffix. The focused-only
// shortcut hint is NOT included — callers append it after the viewport's
// window so it always sits below the visible rows.
func buildListRowLines(s theme.Styles, fld Field, focused, caretOn bool) []string {
	parts := make([]string, 0, len(fld.List))
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
	return parts
}

func updateText(fld *Field, key tea.KeyPressMsg, allowNewline bool) {
	state, _ := lineedit.Apply(lineedit.State{
		Runes:        []rune(fld.Value),
		Cursor:       fld.textCursor,
		AllowNewline: allowNewline,
	}, key)
	fld.Value = state.String()
	fld.textCursor = state.Cursor
}

func clampCursor(cursor *int, n int) {
	if *cursor > n {
		*cursor = n
	}
	if *cursor < 0 {
		*cursor = 0
	}
}

func updateSegmented(fld *Field, key tea.KeyPressMsg) {
	if len(fld.Options) == 0 {
		return
	}
	s := key.String()
	if s == "enter" {
		if fld.popupOpen {
			fld.popupOpen = false
			fld.popupOriginal = ""
		} else {
			fld.popupOpen = true
			fld.popupOriginal = fld.Value
		}
		return
	}
	if s == "esc" && fld.popupOpen {
		fld.Value = fld.popupOriginal
		fld.popupOpen = false
		fld.popupOriginal = ""
		return
	}
	// inline renders as `◂ value ▸` — a horizontal control. We accept
	// only horizontal-motion keys there so j/k stay free for field-nav
	// and so the keypress direction matches the visual orientation.
	// In the popup the same control becomes a vertical list, so j/k and
	// up/down become natural and we accept the full motion set.
	next := []string{"right", "l"}
	prev := []string{"left", "h"}
	if fld.popupOpen {
		next = append(next, "down", "j")
		prev = append(prev, "up", "k")
	}
	idx := indexOf(fld.Options, fld.Value)
	switch {
	case slices.Contains(next, s):
		if idx < 0 {
			idx = 0
		} else {
			idx = (idx + 1) % len(fld.Options)
		}
		fld.Value = fld.Options[idx]
	case slices.Contains(prev, s):
		if idx < 0 {
			idx = 0
		} else {
			idx = (idx - 1 + len(fld.Options)) % len(fld.Options)
		}
		fld.Value = fld.Options[idx]
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
// All in-entry text editing is delegated to lineedit.
func updateList(fld *Field, key tea.KeyPressMsg) {
	if listStructural(fld, key) {
		return
	}
	i := fld.listCursor
	if i < 0 || i >= len(fld.List) {
		return
	}
	// backspace on an empty entry is structural — remove the row instead of
	// no-oping inside lineedit.
	if key.String() == "backspace" && fld.List[i] == "" {
		listRemoveCurrent(fld)
		return
	}
	state, _ := lineedit.Apply(lineedit.State{
		Runes:  []rune(fld.List[i]),
		Cursor: fld.listEntryCursor,
	}, key)
	fld.List[i] = state.String()
	fld.listEntryCursor = state.Cursor
}

func listStructural(fld *Field, key tea.KeyPressMsg) bool {
	switch key.String() {
	case "down":
		if len(fld.List) > 0 {
			fld.listCursor = (fld.listCursor + 1) % len(fld.List)
			fld.listEntryCursor = lineedit.RuneLen(fld.List[fld.listCursor])
		}
	case "up":
		if len(fld.List) > 0 {
			fld.listCursor = (fld.listCursor - 1 + len(fld.List)) % len(fld.List)
			fld.listEntryCursor = lineedit.RuneLen(fld.List[fld.listCursor])
		}
	default:
		return false
	}
	return true
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
		fld.listEntryCursor = lineedit.RuneLen(fld.List[fld.listCursor])
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
