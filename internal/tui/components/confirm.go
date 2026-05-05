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

// View renders the confirm dialog. Pass width=0 for automatic sizing.
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
