package tui

import (
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

// routeActiveAction reads the active screen's pending Action and reacts
// to it (push/pop/replace/connect/quit).
func (m *Model) routeActiveAction() tea.Cmd {
	switch s := m.active.(type) {
	case *clusters.Model:
		return m.routeClustersAction(s)
	case *topics.Model:
		return m.routeTopicsAction(s)
	case *messages.Model:
		return m.routeMessagesAction(s)
	case *produce.Model:
		return m.routeProduceAction(s)
	case *groups.Model:
		return m.routeGroupsAction(s)
	case *logs.Model:
		return m.routeLogsAction(s)
	case *configsrc.Model:
		return m.routeConfigSrcAction(s)
	case *topics.ConfigsModel:
		return m.routeTopicConfigsAction(s)
	case *topics.ConfigEditModel:
		return m.routeTopicConfigEditAction(s)
	}
	return nil
}

func (m *Model) routeClustersAction(s *clusters.Model) tea.Cmd {
	a := s.ConsumeAction()
	if a.Quit {
		m.quit = true
		return tea.Quit
	}
	if a.Connect != "" {
		return m.connectCluster(a.Connect)
	}
	if a.Reload {
		m.reloadClusters(s)
	}
	return nil
}

func (m *Model) routeTopicsAction(s *topics.Model) tea.Cmd {
	a := s.ConsumeAction()
	switch {
	case a.Quit:
		return m.popOrReplaceToClusters()
	case a.Messages != "":
		m.lastTopic = a.Messages
		m.navTopic = a.Messages
		return m.pushScreenCmd(ScreenMessages)
	case a.Configs != "":
		m.lastTopic = a.Configs
		m.navTopic = a.Configs
		// fresh entry into the configs screen — drop a stale focus seed
		// from a previous topic / edit session so the cursor lands at
		// the top, not on a key that may not exist here.
		m.lastConfigKey = ""
		return m.pushScreenCmd(ScreenTopicConfigs)
	case a.Groups != "":
		m.lastTopic = a.Groups
		m.navGroupFilter = a.Groups
		return m.pushScreenCmd(ScreenGroups)
	case a.Produce != "":
		m.lastTopic = a.Produce
		m.navTopic = a.Produce
		m.navPrefill = nil
		return m.pushScreenCmd(ScreenProduce)
	}
	return nil
}

func (m *Model) routeMessagesAction(s *messages.Model) tea.Cmd {
	a := s.ConsumeAction()
	switch {
	case a.Back:
		m.popScreen()
		return m.activeInit()
	case a.Groups != "":
		m.lastTopic = a.Groups
		m.navGroupFilter = a.Groups
		return m.pushScreenCmd(ScreenGroups)
	case a.Produce != "":
		m.lastTopic = a.Produce
		m.navTopic = a.Produce
		m.navPrefill = a.PrefillFromMessage
		return m.pushScreenCmd(ScreenProduce)
	}
	return nil
}

func (m *Model) routeProduceAction(s *produce.Model) tea.Cmd {
	a := s.ConsumeAction()
	if a.Back {
		pending := pendingProduceToast(s, a)
		m.popScreen()
		if pending != nil {
			if q, ok := activeToastQueue(m.active); ok {
				q.Push(pending.Level, pending.Message)
			}
		}
		return m.activeInit()
	}
	return nil
}

// pendingProduceToast forwards the produce screen's success toast to the
// screen the user lands on after the form closes.
func pendingProduceToast(s *produce.Model, a produce.Action) *components.Toast {
	if a.Sent == nil {
		return nil
	}
	t, ok := s.LatestFlash()
	if !ok {
		return nil
	}
	return &t
}

func (m *Model) routeGroupsAction(s *groups.Model) tea.Cmd {
	a := s.ConsumeAction()
	switch {
	case a.Back:
		return m.popOrReplaceToHome()
	case a.Topic != "":
		m.lastTopic = a.Topic
		m.navTopic = a.Topic
		return m.pushScreenCmd(ScreenMessages)
	}
	return nil
}

func (m *Model) routeLogsAction(s *logs.Model) tea.Cmd {
	a := s.ConsumeAction()
	if a.Back {
		return m.popOrReplaceToHome()
	}
	return nil
}

func (m *Model) routeConfigSrcAction(s *configsrc.Model) tea.Cmd {
	a := s.ConsumeAction()
	if a.Back {
		return m.popOrReplaceToHome()
	}
	return nil
}

func (m *Model) routeTopicConfigsAction(s *topics.ConfigsModel) tea.Cmd {
	a := s.ConsumeAction()
	if a.Back {
		m.popScreen()
		return m.activeInit()
	}
	if a.Edit != "" {
		m.navTopic = s.Topic()
		m.navConfigKey = a.Edit
		m.navConfigValue = currentValueForKey(s, a.Edit)
		// remember the focused key so popScreen() restores the cursor
		// when the user returns from the edit screen.
		m.lastConfigKey = a.Edit
		return m.pushScreenCmd(ScreenTopicConfigEdit)
	}
	return nil
}

// currentValueForKey is small enough to inline, but lives next to the
// router so the configs screen public surface stays minimal.
func currentValueForKey(s *topics.ConfigsModel, key string) string {
	for _, c := range s.Configs() {
		if c.Key == key {
			return c.Value
		}
	}
	return ""
}

func (m *Model) routeTopicConfigEditAction(s *topics.ConfigEditModel) tea.Cmd {
	a := s.ConsumeAction()
	if !a.Back {
		return nil
	}
	pending := pendingEditToast(s, a)
	m.popScreen()
	if pending != nil {
		if q, ok := activeToastQueue(m.active); ok {
			q.Push(pending.Level, pending.Message)
		}
	}
	return m.activeInit()
}

// pendingEditToast forwards the edit screen's success toast to the
// configs screen the user lands on after the form closes.
func pendingEditToast(s *topics.ConfigEditModel, a topics.ConfigEditAction) *components.Toast {
	if !a.Saved {
		return nil
	}
	t, ok := s.LatestFlash()
	if !ok {
		return nil
	}
	return &t
}

// popOrReplaceToClusters pops to expose the clusters list, or replaces
// when there's nothing below.
func (m *Model) popOrReplaceToClusters() tea.Cmd {
	if m.router.Depth() > 1 {
		m.popScreen()
		return m.activeInit()
	}
	return m.replaceScreen(ScreenClusters, "")
}

// popOrReplaceToHome pops to the underlying screen, or replaces with the
// natural "home" — topics when a cluster is connected, clusters otherwise.
// Used by screens reachable via `:` commands (groups, logs, config sources)
// so esc at depth=1 lands somewhere usable instead of "(no screen active)".
func (m *Model) popOrReplaceToHome() tea.Cmd {
	if m.router.Depth() > 1 {
		m.popScreen()
		return m.activeInit()
	}
	if m.client != nil {
		return m.replaceScreen(ScreenTopics, "")
	}
	return m.replaceScreen(ScreenClusters, "")
}

// closeActive releases background resources held by the active screen
// and clears the pointer. The screen's current `/` filter is captured into
// lastFilters first so a subsequent push/pop/replace can restore it on the
// next instance — without this, popping back to topics after browsing a
// message lands on an unfiltered list and the user has to re-type the query.
func (m *Model) closeActive() {
	if m.active != nil {
		if id := m.router.Active(); id != "" {
			m.lastFilters[id] = screenActiveFilter(m.active)
		}
		closeScreen(m.active)
		m.active = nil
	}
	m.flash = layout.Flash{}
}

// pushScreen pushes id and instantiates the screen. The caller is expected
// to invoke Init separately (used at startup).
func (m *Model) pushScreen(id ScreenID) {
	m.closeActive()
	m.router.Push(id)
	m.instantiate(id)
	m.applySize()
	m.syncRefreshStatus()
}

// pushScreenCmd is the runtime variant that returns the screen's Init cmd.
func (m *Model) pushScreenCmd(id ScreenID) tea.Cmd {
	m.closeActive()
	m.router.Push(id)
	m.instantiate(id)
	m.applySize()
	m.syncRefreshStatus()
	return m.activeInit()
}

func (m *Model) replaceScreen(id ScreenID, arg string) tea.Cmd {
	if id == ScreenClusters && arg != "" {
		// `:cluster <name>` connects to a known cluster directly.
		return m.connectCluster(arg)
	}
	m.closeActive()
	m.router.Replace(id)
	m.instantiate(id)
	m.applySize()
	m.syncRefreshStatus()
	return m.activeInit()
}

// popScreen pops and re-instantiates the underlying screen. We drop
// instances when leaving them to release Kafka follow sessions, table
// state, etc.
func (m *Model) popScreen() {
	m.closeActive()
	m.router.Pop()
	if id := m.router.Active(); id != "" {
		m.instantiate(id)
	}
	m.applySize()
	m.syncRefreshStatus()
}
