package components

import (
	"strconv"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// MenuItem is a single row in a [Menu]. Hint is optional text shown to the
// right of the label.
type MenuItem struct {
	Label string
	Hint  string
}

// Menu is a popup list with digit shortcuts and arrow/Tab navigation. The
// menu does not close itself — the host inspects the result flags
// ([Selected] / [Canceled]) after each Update.
type Menu struct {
	title  string
	items  []MenuItem
	cursor int

	confirmed bool
	canceled  bool

	styles theme.Styles
}

func NewMenu(items []MenuItem, opts ...MenuOption) *Menu {
	m := &Menu{
		items:  append([]MenuItem(nil), items...),
		styles: theme.DefaultStyles(),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

type MenuOption func(*Menu)

func WithMenuStyles(s theme.Styles) MenuOption {
	return func(m *Menu) { m.styles = s }
}

func WithMenuTitle(title string) MenuOption {
	return func(m *Menu) { m.title = title }
}

// WithMenuCursor pre-positions the cursor (clamped).
func WithMenuCursor(i int) MenuOption {
	return func(m *Menu) {
		if i < 0 {
			i = 0
		}
		if i >= len(m.items) {
			i = len(m.items) - 1
		}
		if i < 0 {
			i = 0
		}
		m.cursor = i
	}
}

func (m *Menu) Cursor() int { return m.cursor }

// SetCursor moves the cursor to i (clamped).
func (m *Menu) SetCursor(i int) {
	if len(m.items) == 0 {
		m.cursor = 0
		return
	}
	if i < 0 {
		i = 0
	}
	if i >= len(m.items) {
		i = len(m.items) - 1
	}
	m.cursor = i
}

func (m *Menu) Items() []MenuItem {
	out := make([]MenuItem, len(m.items))
	copy(out, m.items)
	return out
}

// Selected returns (index, item, true) when the user pressed enter on the
// current cursor since the last Reset.
func (m *Menu) Selected() (int, MenuItem, bool) {
	if !m.confirmed || m.cursor < 0 || m.cursor >= len(m.items) {
		return 0, MenuItem{}, false
	}
	return m.cursor, m.items[m.cursor], true
}

func (m *Menu) Canceled() bool { return m.canceled }

func (m *Menu) Reset() {
	m.confirmed = false
	m.canceled = false
}

// Update routes a key message. Digit shortcuts jump to that index AND
// confirm the selection in a single keystroke.
func (m *Menu) Update(msg tea.Msg) (*Menu, tea.Cmd) {
	key, ok := msg.(tea.KeyPressMsg)
	if !ok {
		return m, nil
	}
	if len(m.items) == 0 {
		if key.String() == "esc" {
			m.canceled = true
		}
		return m, nil
	}
	switch key.String() {
	case "down", "j", "tab":
		m.cursor = (m.cursor + 1) % len(m.items)
	case "up", "k", "shift+tab":
		m.cursor = (m.cursor - 1 + len(m.items)) % len(m.items)
	case "home":
		m.cursor = 0
	case "end":
		m.cursor = len(m.items) - 1
	case "enter":
		m.confirmed = true
	case "esc":
		m.canceled = true
	default:
		if t := key.Text; len(t) == 1 && t[0] >= '1' && t[0] <= '9' {
			idx, err := strconv.Atoi(t)
			if err == nil && idx >= 1 && idx <= len(m.items) {
				m.cursor = idx - 1
				m.confirmed = true
			}
		}
	}
	return m, nil
}

// Bindings returns the keystrokes Update() recognizes as advertise-only
// entries (no Handler) so screens can merge them into their own help/hints.
// Passing category="" hides every entry from the help overlay; Hint=true
// entries (enter, esc) still appear in the bottom hints bar.
func (m *Menu) Bindings(category string) []keymap.Binding {
	bs := []keymap.Binding{
		{Keys: []string{"j", "down", "tab"}, Label: "next item", Category: category},
		{Keys: []string{"k", "up", "shift+tab"}, Label: "previous item", Category: category},
		{Keys: []string{"home"}, Label: "first item", Category: category},
		{Keys: []string{"end"}, Label: "last item", Category: category},
		{Keys: []string{"enter"}, Label: "select", Category: category, Hint: true},
		{Keys: []string{"esc"}, Label: "cancel", Category: category, Hint: true},
	}
	// only list digits 1..N where N = min(9, len(items)) so help stays honest.
	n := min(len(m.items), 9)
	if n > 0 {
		digits := make([]string, n)
		for i := range n {
			digits[i] = strconv.Itoa(i + 1)
		}
		bs = append(bs, keymap.Binding{
			Keys:     digits,
			Label:    "jump to item by index",
			Category: category,
		})
	}
	return bs
}

// View renders the menu body. width<=0 means natural width.
func (m *Menu) View(width int) string {
	parts := make([]string, 0, len(m.items)+2)
	if m.title != "" {
		parts = append(parts, m.styles.HelpTitle.Render(m.title), "")
	}
	for i, it := range m.items {
		focused := i == m.cursor
		prefix := "  "
		labelStyle := m.styles.Command
		if focused {
			prefix = "▸ "
			labelStyle = m.styles.CommandHL
		}
		digit := m.styles.HintKey.Render(strconv.Itoa(i + 1))
		row := prefix + digit + ". " + labelStyle.Render(it.Label)
		if it.Hint != "" {
			row += " " + m.styles.HintLabel.Render(it.Hint)
		}
		parts = append(parts, row)
	}
	body := strings.Join(parts, "\n")
	box := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		Padding(0, 2).
		Foreground(m.styles.Palette.Foreground).
		Render(body)
	if width <= 0 {
		return box
	}
	return lipgloss.PlaceHorizontal(width, lipgloss.Center, box)
}
