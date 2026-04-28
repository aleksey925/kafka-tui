package components

import (
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// ConfirmResult is what Confirm.Update returns when the user answers.
type ConfirmResult int

const (
	// ConfirmPending means the user has not answered yet.
	ConfirmPending ConfirmResult = iota
	// ConfirmYes means the user pressed `y`.
	ConfirmYes
	// ConfirmNo means the user pressed `n` or Esc.
	ConfirmNo
)

// Confirm is a centered yes/no modal dialog (specification §7.12).
//
// Screens own the data flow: when their action requires confirmation they
// instantiate Confirm, route key messages, and check the Result on each
// frame.
type Confirm struct {
	Title   string
	Message string
	YesKey  string // defaults to "y"
	NoKey   string // defaults to "n"

	result ConfirmResult
	styles theme.Styles
}

// NewConfirm constructs a confirm dialog with the given title and message.
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

// ConfirmOption configures Confirm.
type ConfirmOption func(*Confirm)

// WithConfirmStyles overrides the theme styles.
func WithConfirmStyles(s theme.Styles) ConfirmOption {
	return func(c *Confirm) { c.styles = s }
}

// Result returns the latest user answer (or ConfirmPending).
func (c *Confirm) Result() ConfirmResult { return c.result }

// Reset clears the answer (so the same instance can be reused).
func (c *Confirm) Reset() { c.result = ConfirmPending }

// Update routes a key message into the confirm dialog.
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

// View renders the confirm dialog. The width parameter controls how wide the
// modal box is; pass 0 for an automatic sizing based on content.
func (c *Confirm) View(width int) string {
	hint := c.styles.HintKey.Render(c.YesKey) + c.styles.HintLabel.Render(" yes  ") +
		c.styles.HintKey.Render(c.NoKey) + c.styles.HintLabel.Render(" no")

	body := []string{}
	if c.Title != "" {
		body = append(body, c.styles.HelpTitle.Render(c.Title))
	}
	if c.Message != "" {
		body = append(body, c.styles.Command.Render(c.Message))
	}
	body = append(body, "", hint)

	box := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		Padding(0, 2).
		Foreground(c.styles.Palette.Foreground).
		Render(strings.Join(body, "\n"))

	if width <= 0 {
		return box
	}
	return lipgloss.PlaceHorizontal(width, lipgloss.Center, box)
}
