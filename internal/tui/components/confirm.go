package components

import (
	"strings"

	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// ConfirmResult is what Confirm.Update returns when the user answers.
type ConfirmResult int

const (
	ConfirmPending ConfirmResult = iota
	ConfirmYes
	ConfirmNo
)

// Confirm is a centered yes/no modal dialog. Message is the trailing
// sentence (e.g. "This cannot be undone."); fields carry the labeled
// context (Topic, Group, From/To) that identifies what is about to happen.
// Long context values wrap onto their own lines so the modal box never
// stretches to the full length of an identifier (see the shared
// [renderModal]).
type Confirm struct {
	Title   string
	Message string
	YesKey  string // defaults to "y"
	NoKey   string // defaults to "n"

	fields []modalField
	result ConfirmResult
	styles theme.Styles
}

func NewConfirm(title, message string, opts ...ConfirmOption) *Confirm {
	c := &Confirm{
		Title:   title,
		Message: message,
		YesKey:  "y",
		NoKey:   "n",
		styles:  theme.DefaultStyles(),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

type ConfirmOption func(*Confirm)

func WithConfirmStyles(s theme.Styles) ConfirmOption {
	return func(c *Confirm) { c.styles = s }
}

// WithConfirmField appends a labeled context line (e.g. "Topic", name) shown
// above the message. The value wraps when it would overflow the modal width.
func WithConfirmField(label, value string) ConfirmOption {
	return func(c *Confirm) { c.fields = append(c.fields, modalField{Label: label, Value: value}) }
}

func (c *Confirm) Result() ConfirmResult { return c.result }

func (c *Confirm) Reset() { c.result = ConfirmPending }

// Bindings advertises the confirm's keymap for help / hints while it's
// open. yesLabel describes what the y answer commits to (e.g. "save",
// "clone"); the no/esc rows are always "cancel".
func (c *Confirm) Bindings(category, yesLabel string) []keymap.Binding {
	return []keymap.Binding{
		{Keys: []string{c.YesKey}, Label: yesLabel, Category: category, Hint: true},
		{Keys: []string{c.NoKey, "esc"}, Label: "cancel", Category: category, Hint: true},
	}
}

func (c *Confirm) Update(msg tea.Msg) (*Confirm, tea.Cmd) {
	key, ok := msg.(tea.KeyPressMsg)
	if !ok {
		return c, nil
	}
	switch strings.ToLower(key.String()) {
	case strings.ToLower(c.YesKey):
		c.result = ConfirmYes
	case strings.ToLower(c.NoKey), "esc":
		c.result = ConfirmNo
	}
	return c, nil
}

// View renders the confirm dialog as a centered modal. width and height
// are the body-area dimensions to center within; pass 0 for either axis
// to skip placement on that axis.
func (c *Confirm) View(width, height int) string {
	hint := HintLine(c.styles,
		Hint{Key: c.YesKey, Label: "yes"},
		Hint{Key: c.NoKey, Label: "no"},
	)
	return renderModal(c.styles, c.Title, c.fields, c.Message, hint, width, height)
}
