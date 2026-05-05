package components_test

import (
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
)

func TestMenu_Navigation_ArrowsAndWrap(t *testing.T) {
	// arrange
	m := components.NewMenu([]components.MenuItem{{Label: "a"}, {Label: "b"}, {Label: "c"}})

	// act + assert
	_, _ = m.Update(menuKey("down"))
	assert.Equal(t, 1, m.Cursor())

	_, _ = m.Update(menuKey("up"))
	assert.Equal(t, 0, m.Cursor())

	// wrap
	_, _ = m.Update(menuKey("up"))
	assert.Equal(t, 2, m.Cursor())
}

func TestMenu_DigitJumpsAndConfirms(t *testing.T) {
	m := components.NewMenu([]components.MenuItem{
		{Label: "one"}, {Label: "two"}, {Label: "three"},
	})

	_, _ = m.Update(menuRune('2'))

	idx, item, ok := m.Selected()
	require.True(t, ok)
	assert.Equal(t, 1, idx)
	assert.Equal(t, "two", item.Label)
}

func TestMenu_EnterConfirmsCurrent(t *testing.T) {
	m := components.NewMenu(
		[]components.MenuItem{{Label: "a"}, {Label: "b"}},
		components.WithMenuCursor(1),
	)
	_, _ = m.Update(menuKey("enter"))
	idx, _, ok := m.Selected()
	require.True(t, ok)
	assert.Equal(t, 1, idx)
}

func TestMenu_EscCancels(t *testing.T) {
	m := components.NewMenu([]components.MenuItem{{Label: "a"}})
	_, _ = m.Update(menuKey("esc"))
	assert.True(t, m.Canceled())
	_, _, ok := m.Selected()
	assert.False(t, ok)
}

func TestMenu_View_ContainsItemsAndDigits(t *testing.T) {
	m := components.NewMenu([]components.MenuItem{
		{Label: "alpha", Hint: "(1)"},
		{Label: "beta"},
	})

	out := m.View(0)
	assert.Contains(t, out, "alpha")
	assert.Contains(t, out, "beta")
	assert.Contains(t, out, "1")
	assert.Contains(t, out, "2")
	// hint surfaces too
	assert.Contains(t, out, "(1)")
}

func TestMenu_Reset_ClearsFlags(t *testing.T) {
	m := components.NewMenu([]components.MenuItem{{Label: "a"}})
	_, _ = m.Update(menuKey("enter"))
	require.True(t, hasSelection(m))
	m.Reset()
	assert.False(t, hasSelection(m))
}

func hasSelection(m *components.Menu) bool {
	_, _, ok := m.Selected()
	return ok
}

func menuKey(name string) tea.KeyPressMsg {
	switch name {
	case "enter":
		return tea.KeyPressMsg{Code: tea.KeyEnter}
	case "esc":
		return tea.KeyPressMsg{Code: tea.KeyEscape}
	case "down":
		return tea.KeyPressMsg{Code: tea.KeyDown}
	case "up":
		return tea.KeyPressMsg{Code: tea.KeyUp}
	case "tab":
		return tea.KeyPressMsg{Code: tea.KeyTab}
	}
	if len(name) == 1 {
		r := rune(name[0])
		return tea.KeyPressMsg{Code: r, Text: string(r)}
	}
	return tea.KeyPressMsg{}
}

func menuRune(r rune) tea.KeyPressMsg {
	return tea.KeyPressMsg{Code: r, Text: string(r)}
}
