package components

import (
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// SendConfirmResult is what SendConfirm.Update returns when the user answers.
type SendConfirmResult int

const (
	SendConfirmPending SendConfirmResult = iota
	SendConfirmYesClose
	SendConfirmYesKeep
	SendConfirmNo
)

// SendConfirm is a centered modal that gates an irreversible produce.
// It carries two confirm variants (close form / keep open) plus cancel,
// shows the cluster + topic context so the user reads where the record
// is going before committing, and binds no default cursor / no enter
// key — every answer must come from an explicit y/k/n/esc keystroke.
type SendConfirm struct {
	Cluster string
	Topic   string

	result SendConfirmResult
	styles theme.Styles
}

func NewSendConfirm(cluster, topic string, opts ...SendConfirmOption) *SendConfirm {
	c := &SendConfirm{
		Cluster: cluster,
		Topic:   topic,
		styles:  theme.DefaultStyles(),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

type SendConfirmOption func(*SendConfirm)

func WithSendConfirmStyles(s theme.Styles) SendConfirmOption {
	return func(c *SendConfirm) { c.styles = s }
}

func (c *SendConfirm) Result() SendConfirmResult { return c.result }

func (c *SendConfirm) Reset() { c.result = SendConfirmPending }

// Update routes a keypress. Enter is intentionally unbound — a reflexive
// enter must not fire send.
func (c *SendConfirm) Update(msg tea.Msg) (*SendConfirm, tea.Cmd) {
	key, ok := msg.(tea.KeyPressMsg)
	if !ok {
		return c, nil
	}
	switch strings.ToLower(key.String()) {
	case "y":
		c.result = SendConfirmYesClose
	case "k":
		c.result = SendConfirmYesKeep
	case "n", "esc":
		c.result = SendConfirmNo
	}
	return c, nil
}

// Bindings advertises the keymap for help and hints. Category="" hides
// the entries from help while keeping Hint=true rows in the bottom bar.
func (c *SendConfirm) Bindings(category string) []keymap.Binding {
	return []keymap.Binding{
		{Keys: []string{"y"}, Label: "send", Category: category, Hint: true},
		{Keys: []string{"k"}, Label: "send & keep", Category: category, Hint: true},
		{Keys: []string{"esc", "n"}, Label: "cancel", Category: category, Hint: true},
	}
}

// View renders the modal centered within width/height; pass 0 to skip
// placement on that axis.
func (c *SendConfirm) View(width, height int) string {
	title := lipgloss.PlaceHorizontal(modalContentWidth, lipgloss.Center, c.styles.HelpTitle.Render("Send"))
	body := []string{title, ""}
	if c.Cluster != "" {
		body = append(body, c.styles.Command.Render("Cluster:  "+c.Cluster))
	}
	if c.Topic != "" {
		body = append(body, c.styles.Command.Render("Topic:    "+c.Topic))
	}
	hint := c.styles.HintKey.Render("y") + c.styles.HintLabel.Render(" send  ") +
		c.styles.HintKey.Render("k") + c.styles.HintLabel.Render(" send & keep  ") +
		c.styles.HintKey.Render("esc") + c.styles.HintLabel.Render(" cancel")
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
