package tui

import (
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/tui/layout"
)

// fakeScreen is a stand-in screen used to drive the host's raw-input dispatch
// path without booting a real produce form.
type fakeScreen struct {
	rawInput bool
	keys     []string
}

func (s *fakeScreen) Init() tea.Cmd { return nil }
func (s *fakeScreen) Update(msg tea.Msg) tea.Cmd {
	if k, ok := msg.(tea.KeyPressMsg); ok {
		s.keys = append(s.keys, k.String())
	}
	return nil
}
func (s *fakeScreen) View() string               { return "" }
func (s *fakeScreen) SetSize(_, _ int)           {}
func (s *fakeScreen) KeyHints() []layout.KeyHint { return nil }
func (s *fakeScreen) WantsRawInput() bool        { return s.rawInput }

func keyMsg(s string) tea.KeyPressMsg {
	if len(s) == 1 && s[0] >= ' ' && s[0] < 0x7f {
		return tea.KeyPressMsg{Code: rune(s[0]), Text: s}
	}
	return tea.KeyPressMsg{Code: tea.KeyExtended, Text: s}
}

func TestHandleNormalKey_RawInputBypassesGlobalShortcuts(t *testing.T) {
	m := New(Options{Initial: ScreenTopics, Width: 80, Height: 24})
	fake := &fakeScreen{rawInput: true}
	m.active = fake

	for _, key := range []string{":", "/", "?", "ctrl+r"} {
		_, _ = m.Update(keyMsg(key))
	}

	assert.Equal(t, []string{":", "/", "?", "ctrl+r"}, fake.keys,
		"raw-input screen must receive every key as a literal")
	assert.Equal(t, ModeNormal, m.mode, "global modes must not activate")
	assert.True(t, m.autoRefresh, "auto-refresh toggle must not fire")
}

func TestHandleNormalKey_NonRawScreenStillSeesGlobalShortcuts(t *testing.T) {
	m := New(Options{Initial: ScreenTopics, Width: 80, Height: 24})
	fake := &fakeScreen{rawInput: false}
	m.active = fake

	_, _ = m.Update(keyMsg(":"))

	assert.Equal(t, ModeCommand, m.mode)
	assert.Empty(t, fake.keys, "global shortcut must not reach screen")
}

func TestHandleNormalKey_CtrlCQuitsEvenInRawInput(t *testing.T) {
	m := New(Options{Initial: ScreenTopics, Width: 80, Height: 24})
	fake := &fakeScreen{rawInput: true}
	m.active = fake

	_, cmd := m.Update(keyMsg("ctrl+c"))

	assert.True(t, m.quit)
	assert.NotNil(t, cmd)
	assert.Empty(t, fake.keys, "ctrl+c must not be forwarded to the screen")
}
