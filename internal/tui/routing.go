// Routing — the host translates the active screen's [Action] into
// router transitions (push / pop / replace) and lifecycle ops, plus
// the closeActive hook that releases background resources before a
// screen is dropped.

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

// routeActiveAction reads the active screen's pending Action and reacts to
// it (push/pop/replace/connect/quit). Returns any tea.Cmd produced.
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
		// On send & close the produce screen pushes a success toast right
		// before signalling Back; without forwarding it to the underlying
		// screen's queue the user would never see the confirmation. Failed
		// sends keep the form open, so their toast surfaces locally.
		var pending *components.Toast
		if a.Sent != nil {
			if t, ok := s.LatestFlash(); ok {
				pending = &t
			}
		}
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

func (m *Model) routeGroupsAction(s *groups.Model) tea.Cmd {
	a := s.ConsumeAction()
	switch {
	case a.Back:
		m.popScreen()
		return m.activeInit()
	case a.Topic != "":
		m.lastTopic = a.Topic
		m.navTopic = a.Topic
		return m.pushScreenCmd(ScreenMessages)
	case len(a.TopicsForGroup) > 0:
		m.navTopicsFilter = a.TopicsForGroup
		return m.pushScreenCmd(ScreenTopics)
	}
	return nil
}

func (m *Model) routeLogsAction(s *logs.Model) tea.Cmd {
	a := s.ConsumeAction()
	if a.Back {
		m.popScreen()
		return m.activeInit()
	}
	return nil
}

func (m *Model) routeConfigSrcAction(s *configsrc.Model) tea.Cmd {
	a := s.ConsumeAction()
	if a.Back {
		m.popScreen()
		return m.activeInit()
	}
	return nil
}

func (m *Model) routeTopicConfigsAction(s *topics.ConfigsModel) tea.Cmd {
	a := s.ConsumeAction()
	if a.Back {
		m.popScreen()
		return m.activeInit()
	}
	return nil
}

// popOrReplaceToClusters pops the topics screen to expose the clusters list
// underneath; if there's nothing below, it replaces with the clusters screen.
func (m *Model) popOrReplaceToClusters() tea.Cmd {
	if m.router.Depth() > 1 {
		m.popScreen()
		return m.activeInit()
	}
	return m.replaceScreen(ScreenClusters, "")
}

// closeActive releases any background resources held by the current
// active screen (clone goroutine, follow session, etc.) and clears the
// pointer so a fresh instance can be wired in.
func (m *Model) closeActive() {
	if m.active != nil {
		closeScreen(m.active)
		m.active = nil
	}
	m.flash = layout.Flash{}
}

// pushScreen pushes id onto the router stack and constructs the screen
// instance. Used at startup. Discards the resulting Init cmd because the
// caller is expected to invoke [Init] separately.
func (m *Model) pushScreen(id ScreenID) {
	m.closeActive()
	m.router.Push(id)
	m.instantiate(id)
	m.applySize()
	m.syncRefreshStatus()
}

// pushScreenCmd is the runtime variant: pushes a screen, instantiates it, and
// returns its Init cmd to be batched with the host's reply.
func (m *Model) pushScreenCmd(id ScreenID) tea.Cmd {
	m.closeActive()
	m.router.Push(id)
	m.instantiate(id)
	m.applySize()
	m.syncRefreshStatus()
	return m.activeInit()
}

// replaceScreen swaps the active screen for a new id, freeing the previous
// instance.
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

// popScreen pops the router stack and frees the previously active screen.
// The new active screen (one level up) is re-instantiated from the bootstrap
// data, since we drop instances when leaving them to release Kafka follow
// sessions, table state, etc.
func (m *Model) popScreen() {
	m.closeActive()
	m.router.Pop()
	if id := m.router.Active(); id != "" {
		m.instantiate(id)
	}
	m.applySize()
	m.syncRefreshStatus()
}
