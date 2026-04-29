package components

import (
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// FieldKind enumerates form field types.
type FieldKind int

const (
	// FieldText is a single-line text input.
	FieldText FieldKind = iota
	// FieldDropdown is a single-choice picker over Options.
	FieldDropdown
	// FieldList is a dynamic list of strings (Add/Remove). Used for headers.
	FieldList
	// FieldTextarea is a multi-line text editor.
	FieldTextarea
)

// Field describes one input on the form. Most fields use a subset of these
// attributes; the Form treats them uniformly.
type Field struct {
	Key     string // unique field identifier
	Label   string
	Kind    FieldKind
	Value   string   // text/textarea: current text; dropdown: selected option
	Options []string // dropdown choices
	List    []string // list-mode entries

	listCursor int // for FieldList: which entry is focused
}

// Form is a vertical stack of Fields navigated with tab / shift+tab. Each
// field implements its own per-character editing.
type Form struct {
	fields []Field
	focus  int

	styles theme.Styles
}

// NewForm constructs a Form with the given fields.
func NewForm(fields []Field, opts ...FormOption) *Form {
	f := &Form{
		fields: append([]Field(nil), fields...),
		styles: theme.DefaultStyles(),
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// FormOption configures a Form.
type FormOption func(*Form)

// WithFormStyles overrides the theme styles.
func WithFormStyles(s theme.Styles) FormOption {
	return func(f *Form) { f.styles = s }
}

// Focused returns the index of the currently-focused field.
func (f *Form) Focused() int { return f.focus }

// FocusedField returns a copy of the currently-focused field.
func (f *Form) FocusedField() Field { return f.fields[f.focus] }

// Field returns a copy of the field with the given key.
func (f *Form) Field(key string) (Field, bool) {
	for _, fld := range f.fields {
		if fld.Key == key {
			return fld, true
		}
	}
	return Field{}, false
}

// Fields returns a defensive copy of all fields.
func (f *Form) Fields() []Field {
	out := make([]Field, len(f.fields))
	copy(out, f.fields)
	return out
}

// SetValue updates the Value of the field with the given key (text/textarea
// fields) or sets the dropdown selection. List fields use SetList instead.
func (f *Form) SetValue(key, value string) {
	for i := range f.fields {
		if f.fields[i].Key == key {
			f.fields[i].Value = value
			return
		}
	}
}

// SetList replaces the list entries of the field with the given key.
func (f *Form) SetList(key string, entries []string) {
	for i := range f.fields {
		if f.fields[i].Key == key && f.fields[i].Kind == FieldList {
			f.fields[i].List = append([]string(nil), entries...)
			f.fields[i].listCursor = 0
			return
		}
	}
}

// FocusKey moves focus to the field with the given key.
func (f *Form) FocusKey(key string) {
	for i := range f.fields {
		if f.fields[i].Key == key {
			f.focus = i
			return
		}
	}
}

// FocusNext advances focus to the next field, wrapping.
func (f *Form) FocusNext() {
	if len(f.fields) == 0 {
		return
	}
	f.focus = (f.focus + 1) % len(f.fields)
}

// FocusPrev moves focus to the previous field, wrapping.
func (f *Form) FocusPrev() {
	if len(f.fields) == 0 {
		return
	}
	f.focus = (f.focus - 1 + len(f.fields)) % len(f.fields)
}

// Update routes a key message to the focused field. tab / shift+tab change
// focus; everything else is delegated to the active field.
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
	case FieldList:
		updateList(fld, key)
	}
	return f, nil
}

// View renders the whole form.
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

	body := ""
	switch fld.Kind {
	case FieldText:
		body = renderTextValue(f.styles, fld.Value, focused, false)
	case FieldTextarea:
		body = renderTextValue(f.styles, fld.Value, focused, true)
	case FieldDropdown:
		body = renderDropdown(f.styles, fld, focused)
	case FieldList:
		body = renderList(f.styles, fld, focused)
	}
	return header + "\n" + body
}

func renderTextValue(s theme.Styles, value string, focused, multiline bool) string {
	caret := ""
	if focused {
		caret = "▌"
	}
	v := value
	if multiline {
		// indent every line for visual grouping
		lines := strings.Split(v, "\n")
		for i, line := range lines {
			lines[i] = "    " + line
		}
		v = strings.Join(lines, "\n")
		return s.Command.Render(v) + s.CommandHL.Render(caret)
	}
	return "    " + s.Command.Render(v) + s.CommandHL.Render(caret)
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

func renderList(s theme.Styles, fld Field, focused bool) string {
	if len(fld.List) == 0 {
		return "    " + s.StatusInfo.Render("(empty — ctrl+a to add)")
	}
	parts := make([]string, 0, len(fld.List))
	for i, entry := range fld.List {
		prefix := "  - "
		style := s.Command
		if focused && i == fld.listCursor {
			prefix = "  ▸ "
			style = s.CommandHL
		}
		parts = append(parts, "  "+style.Render(prefix+entry))
	}
	if focused {
		parts = append(parts, "    "+s.HintLabel.Render("ctrl+a add  ctrl+d delete"))
	}
	return strings.Join(parts, "\n")
}

// updateText handles single-line and multi-line text editing.
func updateText(fld *Field, key tea.KeyPressMsg, allowNewline bool) {
	switch key.String() {
	case "backspace":
		if n := len(fld.Value); n > 0 {
			// remove the last UTF-8 rune to be safe
			r := []rune(fld.Value)
			fld.Value = string(r[:len(r)-1])
		}
	case "enter":
		if allowNewline {
			fld.Value += "\n"
		}
	default:
		if t := key.Text; t != "" {
			fld.Value += t
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

// updateList handles list-mode editing. Arrow keys move the row cursor;
// ctrl+a inserts a new entry; ctrl+d removes the current one. Every other
// printable character (including "a", "d", "j", "k") is appended to the
// focused entry's text — list editing must not steal letters that show up in
// header names like "Authorization".
func updateList(fld *Field, key tea.KeyPressMsg) {
	switch key.String() {
	case "down":
		if len(fld.List) > 0 {
			fld.listCursor = (fld.listCursor + 1) % len(fld.List)
		}
	case "up":
		if len(fld.List) > 0 {
			fld.listCursor = (fld.listCursor - 1 + len(fld.List)) % len(fld.List)
		}
	case "ctrl+a":
		fld.List = append(fld.List, "")
		fld.listCursor = len(fld.List) - 1
	case "ctrl+d":
		if i := fld.listCursor; i >= 0 && i < len(fld.List) {
			fld.List = append(fld.List[:i], fld.List[i+1:]...)
			if fld.listCursor >= len(fld.List) {
				fld.listCursor = max(0, len(fld.List)-1)
			}
		}
	case "backspace":
		if i := fld.listCursor; i >= 0 && i < len(fld.List) {
			r := []rune(fld.List[i])
			if len(r) > 0 {
				fld.List[i] = string(r[:len(r)-1])
			}
		}
	default:
		if t := key.Text; t != "" {
			if i := fld.listCursor; i >= 0 && i < len(fld.List) {
				fld.List[i] += t
			}
		}
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

// String of FieldKind helps tests produce readable diagnostics.
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
	}
	return fmt.Sprintf("kind(%d)", int(k))
}
