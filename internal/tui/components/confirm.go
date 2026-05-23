package components

import (
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// modalContentWidth is the inner width shared by every confirm modal so
// hints fit on a single row and titles can be centered against a known
// canvas. The value comfortably fits the longest hint line in the app
// ("y send  k send & keep  esc cancel") with breathing room on both
// sides.
const modalContentWidth = 44

// ConfirmResult is what Confirm.Update returns when the user answers.
type ConfirmResult int

const (
	ConfirmPending ConfirmResult = iota
	ConfirmYes
	ConfirmNo
)

// Confirm is a centered yes/no modal dialog.
type Confirm struct {
	Title   string
	Message string
	YesKey  string // defaults to "y"
	NoKey   string // defaults to "n"

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
	hint := c.styles.HintKey.Render(c.YesKey) + c.styles.HintLabel.Render(" yes  ") +
		c.styles.HintKey.Render(c.NoKey) + c.styles.HintLabel.Render(" no")

	body := []string{}
	if c.Title != "" {
		body = append(body, lipgloss.PlaceHorizontal(modalContentWidth, lipgloss.Center, c.styles.HelpTitle.Render(c.Title)))
	}
	if c.Message != "" {
		body = append(body, "", c.styles.Command.Render(c.Message))
	}
	body = append(body, "", lipgloss.PlaceHorizontal(modalContentWidth, lipgloss.Center, hint))

	box := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		Padding(0, 2).
		Foreground(c.styles.Palette.Foreground).
		Render(strings.Join(body, "\n"))

	placed := box
	if width > 0 {
		placed = lipgloss.PlaceHorizontal(width, lipgloss.Center, placed)
	}
	if height > 0 {
		placed = lipgloss.PlaceVertical(height, lipgloss.Center, placed)
	}
	return placed
}
