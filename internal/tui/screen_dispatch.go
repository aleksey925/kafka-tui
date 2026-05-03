package tui

import (
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/components"
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
	// WantsRawInput reports whether the screen is currently editing free-form
	// text (e.g. a form field) and wants every key — including normally global
	// shortcuts like `:`, `/`, `?`, `ctrl+r` — routed straight to it. ctrl+c
	// stays global regardless.
	WantsRawInput() bool
	// LatestFlash returns the freshest live toast from the screen's queue
	// (after pruning expired entries). Returns false when nothing is live.
	// The host promotes the result to the global flash bar.
	LatestFlash() (components.Toast, bool)
	// Title is rendered in the top-left of the body frame (e.g.
	// "Topics[42]"). Empty hides the slot.
	Title() string
	// Breadcrumb is rendered in the top-right of the body frame, typically
	// the selected row identifier. Empty hides it.
	Breadcrumb() string
	// RefreshInterval is the configured auto-refresh tick for this screen
	// (0 when the screen has no auto-refresh). The host uses it to drive
	// the chrome's Refresh: indicator and the ctrl+r toggle.
	RefreshInterval() time.Duration
	// SetRefreshPaused puts the screen's refresh ticker on pause without
	// stopping it; flipping back to false resumes the regular cadence.
	// No-op for screens without auto-refresh.
	SetRefreshPaused(bool)
	// LastRefresh returns the wall-clock time of the most recent successful
	// load, or zero when no load has completed (or the screen has no
	// refresh concept). Drives the chrome's "X ago" indicator.
	LastRefresh() time.Time
	// SupportsRefresh reports whether the screen is conceptually live
	// data that could be refreshed (true: topics, groups, clusters) vs
	// a static view (false: a single message, a form, a one-shot
	// snapshot). The chrome shows "—" instead of "off" for the latter
	// to make it clear refresh isn't applicable here.
	SupportsRefresh() bool
}

type clustersScreen struct{ m *clusters.Model }

func (s *clustersScreen) Init() tea.Cmd { return s.m.Init() }
func (s *clustersScreen) Update(msg tea.Msg) tea.Cmd {
	updated, cmd := s.m.Update(msg)
	s.m = updated
	return cmd
}
func (s *clustersScreen) View() string                          { return s.m.View() }
func (s *clustersScreen) SetSize(w, h int)                      { s.m.SetSize(w, h) }
func (s *clustersScreen) KeyHints() []layout.KeyHint            { return s.m.KeyHints() }
func (s *clustersScreen) WantsRawInput() bool                   { return false }
func (s *clustersScreen) LatestFlash() (components.Toast, bool) { return s.m.LatestFlash() }
func (s *clustersScreen) Title() string                         { return s.m.Title() }
func (s *clustersScreen) Breadcrumb() string                    { return s.m.Breadcrumb() }
func (s *clustersScreen) RefreshInterval() time.Duration        { return s.m.RefreshInterval() }
func (s *clustersScreen) SetRefreshPaused(p bool)               { s.m.SetRefreshPaused(p) }
func (s *clustersScreen) LastRefresh() time.Time                { return s.m.LastRefresh() }
func (s *clustersScreen) SupportsRefresh() bool                 { return true }

type topicsScreen struct{ m *topics.Model }

func (s *topicsScreen) Init() tea.Cmd { return s.m.Init() }
func (s *topicsScreen) Update(msg tea.Msg) tea.Cmd {
	updated, cmd := s.m.Update(msg)
	s.m = updated
	return cmd
}
func (s *topicsScreen) View() string                          { return s.m.View() }
func (s *topicsScreen) SetSize(w, h int)                      { s.m.SetSize(w, h) }
func (s *topicsScreen) KeyHints() []layout.KeyHint            { return s.m.KeyHints() }
func (s *topicsScreen) WantsRawInput() bool                   { return s.m.WantsRawInput() }
func (s *topicsScreen) LatestFlash() (components.Toast, bool) { return s.m.LatestFlash() }
func (s *topicsScreen) Title() string                         { return s.m.Title() }
func (s *topicsScreen) Breadcrumb() string                    { return s.m.Breadcrumb() }
func (s *topicsScreen) RefreshInterval() time.Duration        { return s.m.RefreshInterval() }
func (s *topicsScreen) SetRefreshPaused(p bool)               { s.m.SetRefreshPaused(p) }
func (s *topicsScreen) LastRefresh() time.Time                { return s.m.LastRefresh() }
func (s *topicsScreen) SupportsRefresh() bool                 { return true }

type messagesScreen struct{ m *messages.Model }

func (s *messagesScreen) Init() tea.Cmd { return s.m.Init() }
func (s *messagesScreen) Update(msg tea.Msg) tea.Cmd {
	updated, cmd := s.m.Update(msg)
	s.m = updated
	return cmd
}
func (s *messagesScreen) View() string                          { return s.m.View() }
func (s *messagesScreen) SetSize(w, h int)                      { s.m.SetSize(w, h) }
func (s *messagesScreen) KeyHints() []layout.KeyHint            { return s.m.KeyHints() }
func (s *messagesScreen) WantsRawInput() bool                   { return false }
func (s *messagesScreen) LatestFlash() (components.Toast, bool) { return s.m.LatestFlash() }
func (s *messagesScreen) Title() string                         { return s.m.Title() }
func (s *messagesScreen) Breadcrumb() string                    { return s.m.Breadcrumb() }
func (s *messagesScreen) RefreshInterval() time.Duration        { return 0 }
func (s *messagesScreen) SetRefreshPaused(bool)                 {}
func (s *messagesScreen) LastRefresh() time.Time                { return time.Time{} }
func (s *messagesScreen) SupportsRefresh() bool                 { return false }

type produceScreen struct{ m *produce.Model }

func (s *produceScreen) Init() tea.Cmd { return s.m.Init() }
func (s *produceScreen) Update(msg tea.Msg) tea.Cmd {
	updated, cmd := s.m.Update(msg)
	s.m = updated
	return cmd
}
func (s *produceScreen) View() string                          { return s.m.View() }
func (s *produceScreen) SetSize(w, h int)                      { s.m.SetSize(w, h) }
func (s *produceScreen) KeyHints() []layout.KeyHint            { return s.m.KeyHints() }
func (s *produceScreen) WantsRawInput() bool                   { return s.m.WantsRawInput() }
func (s *produceScreen) LatestFlash() (components.Toast, bool) { return s.m.LatestFlash() }
func (s *produceScreen) Title() string                         { return s.m.Title() }
func (s *produceScreen) Breadcrumb() string                    { return s.m.Breadcrumb() }
func (s *produceScreen) RefreshInterval() time.Duration        { return 0 }
func (s *produceScreen) SetRefreshPaused(bool)                 {}
func (s *produceScreen) LastRefresh() time.Time                { return time.Time{} }
func (s *produceScreen) SupportsRefresh() bool                 { return false }

type groupsScreen struct{ m *groups.Model }

func (s *groupsScreen) Init() tea.Cmd { return s.m.Init() }
func (s *groupsScreen) Update(msg tea.Msg) tea.Cmd {
	updated, cmd := s.m.Update(msg)
	s.m = updated
	return cmd
}
func (s *groupsScreen) View() string                          { return s.m.View() }
func (s *groupsScreen) SetSize(w, h int)                      { s.m.SetSize(w, h) }
func (s *groupsScreen) KeyHints() []layout.KeyHint            { return s.m.KeyHints() }
func (s *groupsScreen) WantsRawInput() bool                   { return s.m.WantsRawInput() }
func (s *groupsScreen) LatestFlash() (components.Toast, bool) { return s.m.LatestFlash() }
func (s *groupsScreen) Title() string                         { return s.m.Title() }
func (s *groupsScreen) Breadcrumb() string                    { return s.m.Breadcrumb() }
func (s *groupsScreen) RefreshInterval() time.Duration        { return s.m.RefreshInterval() }
func (s *groupsScreen) SetRefreshPaused(p bool)               { s.m.SetRefreshPaused(p) }
func (s *groupsScreen) LastRefresh() time.Time                { return s.m.LastRefresh() }
func (s *groupsScreen) SupportsRefresh() bool                 { return true }

type logsScreen struct{ m *logs.Model }

func (s *logsScreen) Init() tea.Cmd { return s.m.Init() }
func (s *logsScreen) Update(msg tea.Msg) tea.Cmd {
	updated, cmd := s.m.Update(msg)
	s.m = updated
	return cmd
}
func (s *logsScreen) View() string                          { return s.m.View() }
func (s *logsScreen) SetSize(w, h int)                      { s.m.SetSize(w, h) }
func (s *logsScreen) KeyHints() []layout.KeyHint            { return s.m.KeyHints() }
func (s *logsScreen) WantsRawInput() bool                   { return false }
func (s *logsScreen) LatestFlash() (components.Toast, bool) { return s.m.LatestFlash() }
func (s *logsScreen) Title() string                         { return s.m.Title() }
func (s *logsScreen) Breadcrumb() string                    { return s.m.Breadcrumb() }
func (s *logsScreen) RefreshInterval() time.Duration        { return 0 }
func (s *logsScreen) SetRefreshPaused(bool)                 {}
func (s *logsScreen) LastRefresh() time.Time                { return time.Time{} }
func (s *logsScreen) SupportsRefresh() bool                 { return false }

type configsrcScreen struct{ m *configsrc.Model }

func (s *configsrcScreen) Init() tea.Cmd { return s.m.Init() }
func (s *configsrcScreen) Update(msg tea.Msg) tea.Cmd {
	updated, cmd := s.m.Update(msg)
	s.m = updated
	return cmd
}
func (s *configsrcScreen) View() string                          { return s.m.View() }
func (s *configsrcScreen) SetSize(w, h int)                      { s.m.SetSize(w, h) }
func (s *configsrcScreen) KeyHints() []layout.KeyHint            { return s.m.KeyHints() }
func (s *configsrcScreen) WantsRawInput() bool                   { return false }
func (s *configsrcScreen) LatestFlash() (components.Toast, bool) { return components.Toast{}, false }
func (s *configsrcScreen) Title() string                         { return s.m.Title() }
func (s *configsrcScreen) Breadcrumb() string                    { return s.m.Breadcrumb() }
func (s *configsrcScreen) RefreshInterval() time.Duration        { return 0 }
func (s *configsrcScreen) SetRefreshPaused(bool)                 {}
func (s *configsrcScreen) LastRefresh() time.Time                { return time.Time{} }
func (s *configsrcScreen) SupportsRefresh() bool                 { return false }

type topicConfigsScreen struct{ m *topics.ConfigsModel }

func (s *topicConfigsScreen) Init() tea.Cmd { return s.m.Init() }
func (s *topicConfigsScreen) Update(msg tea.Msg) tea.Cmd {
	updated, cmd := s.m.Update(msg)
	s.m = updated
	return cmd
}
func (s *topicConfigsScreen) View() string                          { return s.m.View() }
func (s *topicConfigsScreen) SetSize(w, h int)                      { s.m.SetSize(w, h) }
func (s *topicConfigsScreen) KeyHints() []layout.KeyHint            { return s.m.KeyHints() }
func (s *topicConfigsScreen) WantsRawInput() bool                   { return false }
func (s *topicConfigsScreen) LatestFlash() (components.Toast, bool) { return s.m.LatestFlash() }
func (s *topicConfigsScreen) Title() string                         { return s.m.Title() }
func (s *topicConfigsScreen) Breadcrumb() string                    { return s.m.Breadcrumb() }
func (s *topicConfigsScreen) RefreshInterval() time.Duration        { return 0 }
func (s *topicConfigsScreen) SetRefreshPaused(bool)                 {}
func (s *topicConfigsScreen) LastRefresh() time.Time                { return time.Time{} }
func (s *topicConfigsScreen) SupportsRefresh() bool                 { return false }
