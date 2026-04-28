package tui

import (
	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/clusters"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/configsrc"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/groups"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/logs"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/messages"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/produce"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/topics"
)

// screen is the uniform surface every hosted screen exposes to the root
// model. Concrete screens differ in their `Update` return shape (each one
// returns its own `*Model`) and in their `Action` types — the per-screen
// adapters below normalise those differences so the host can dispatch via
// a single field.
type screen interface {
	Init() tea.Cmd
	Update(tea.Msg) tea.Cmd
	View() string
	SetSize(w, h int)
	KeyHints() []layout.KeyHint
}

type clustersScreen struct{ m *clusters.Model }

func (s *clustersScreen) Init() tea.Cmd { return s.m.Init() }
func (s *clustersScreen) Update(msg tea.Msg) tea.Cmd {
	updated, cmd := s.m.Update(msg)
	s.m = updated
	return cmd
}
func (s *clustersScreen) View() string               { return s.m.View() }
func (s *clustersScreen) SetSize(w, h int)           { s.m.SetSize(w, h) }
func (s *clustersScreen) KeyHints() []layout.KeyHint { return s.m.KeyHints() }

type topicsScreen struct{ m *topics.Model }

func (s *topicsScreen) Init() tea.Cmd { return s.m.Init() }
func (s *topicsScreen) Update(msg tea.Msg) tea.Cmd {
	updated, cmd := s.m.Update(msg)
	s.m = updated
	return cmd
}
func (s *topicsScreen) View() string               { return s.m.View() }
func (s *topicsScreen) SetSize(w, h int)           { s.m.SetSize(w, h) }
func (s *topicsScreen) KeyHints() []layout.KeyHint { return s.m.KeyHints() }

type messagesScreen struct{ m *messages.Model }

func (s *messagesScreen) Init() tea.Cmd { return s.m.Init() }
func (s *messagesScreen) Update(msg tea.Msg) tea.Cmd {
	updated, cmd := s.m.Update(msg)
	s.m = updated
	return cmd
}
func (s *messagesScreen) View() string               { return s.m.View() }
func (s *messagesScreen) SetSize(w, h int)           { s.m.SetSize(w, h) }
func (s *messagesScreen) KeyHints() []layout.KeyHint { return s.m.KeyHints() }

type produceScreen struct{ m *produce.Model }

func (s *produceScreen) Init() tea.Cmd { return s.m.Init() }
func (s *produceScreen) Update(msg tea.Msg) tea.Cmd {
	updated, cmd := s.m.Update(msg)
	s.m = updated
	return cmd
}
func (s *produceScreen) View() string               { return s.m.View() }
func (s *produceScreen) SetSize(w, h int)           { s.m.SetSize(w, h) }
func (s *produceScreen) KeyHints() []layout.KeyHint { return s.m.KeyHints() }

type groupsScreen struct{ m *groups.Model }

func (s *groupsScreen) Init() tea.Cmd { return s.m.Init() }
func (s *groupsScreen) Update(msg tea.Msg) tea.Cmd {
	updated, cmd := s.m.Update(msg)
	s.m = updated
	return cmd
}
func (s *groupsScreen) View() string               { return s.m.View() }
func (s *groupsScreen) SetSize(w, h int)           { s.m.SetSize(w, h) }
func (s *groupsScreen) KeyHints() []layout.KeyHint { return s.m.KeyHints() }

type logsScreen struct{ m *logs.Model }

func (s *logsScreen) Init() tea.Cmd { return s.m.Init() }
func (s *logsScreen) Update(msg tea.Msg) tea.Cmd {
	updated, cmd := s.m.Update(msg)
	s.m = updated
	return cmd
}
func (s *logsScreen) View() string               { return s.m.View() }
func (s *logsScreen) SetSize(w, h int)           { s.m.SetSize(w, h) }
func (s *logsScreen) KeyHints() []layout.KeyHint { return s.m.KeyHints() }

type configsrcScreen struct{ m *configsrc.Model }

func (s *configsrcScreen) Init() tea.Cmd { return s.m.Init() }
func (s *configsrcScreen) Update(msg tea.Msg) tea.Cmd {
	updated, cmd := s.m.Update(msg)
	s.m = updated
	return cmd
}
func (s *configsrcScreen) View() string               { return s.m.View() }
func (s *configsrcScreen) SetSize(w, h int)           { s.m.SetSize(w, h) }
func (s *configsrcScreen) KeyHints() []layout.KeyHint { return s.m.KeyHints() }

type topicConfigsScreen struct{ m *topics.ConfigsModel }

func (s *topicConfigsScreen) Init() tea.Cmd { return s.m.Init() }
func (s *topicConfigsScreen) Update(msg tea.Msg) tea.Cmd {
	updated, cmd := s.m.Update(msg)
	s.m = updated
	return cmd
}
func (s *topicConfigsScreen) View() string               { return s.m.View() }
func (s *topicConfigsScreen) SetSize(w, h int)           { s.m.SetSize(w, h) }
func (s *topicConfigsScreen) KeyHints() []layout.KeyHint { return s.m.KeyHints() }
