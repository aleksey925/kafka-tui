package tui

import (
	"github.com/aleksey925/kafka-tui/internal/config"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/clusters"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/configsrc"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/groups"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/logs"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/messages"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/produce"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/topics"
)

// requiresClient reports whether id can only be instantiated once a Kafka
// client is connected. Kept in sync with the `m.client != nil` guards in
// [Model.instantiate].
func requiresClient(id ScreenID) bool {
	switch id {
	case ScreenTopics, ScreenTopicConfigs, ScreenTopicConfigEdit,
		ScreenMessages, ScreenProduce, ScreenGroups:
		return true
	case ScreenClusters, ScreenLogs, ScreenConfigSrc, ScreenHelpOverlay:
		return false
	}
	return false
}

// instantiate constructs the screen for id. When boot is nil or required
// deps are absent, the active screen is left nil and renderBody falls back
// to the placeholder.
func (m *Model) instantiate(id ScreenID) {
	if m.boot == nil {
		return
	}
	// after a pop the nav seeds are empty; fall back to lastTopic so
	// topic-bound screens recreate against the correct topic.
	if m.navTopic == "" {
		m.navTopic = m.lastTopic
	}
	defer m.clearNavSeeds()
	defer m.restoreState(id)
	switch id {
	case ScreenClusters:
		m.active = m.newClusters()
	case ScreenTopics:
		if m.client != nil {
			m.active = m.newTopics()
		}
	case ScreenTopicConfigs:
		if m.client != nil {
			m.active = m.newTopicConfigs()
		}
	case ScreenTopicConfigEdit:
		if m.client != nil {
			m.active = m.newTopicConfigEdit()
		}
	case ScreenMessages:
		if m.client != nil {
			m.active = m.newMessages()
		}
	case ScreenProduce:
		if m.client != nil {
			m.active = m.newProduce()
		}
	case ScreenGroups:
		if m.client != nil {
			m.active = m.newGroups()
		}
	case ScreenLogs:
		m.active = m.newLogs()
	case ScreenConfigSrc:
		m.active = m.newConfigSrc()
	case ScreenHelpOverlay:
		// help is a chrome overlay, not a routed screen.
	}
}

func (m *Model) clearNavSeeds() {
	m.navTopic = ""
	m.navPrefill = nil
	m.navGroupFilter = ""
	m.navConfigKey = ""
	m.navConfigValue = ""
}

// restoreState re-applies the Stateful snapshot captured by closeActive
// on the previous instance of this screen. No-op when the screen failed
// to construct, when no state was saved, or when the screen doesn't
// implement Stateful.
func (m *Model) restoreState(id ScreenID) {
	if m.active == nil {
		return
	}
	blob, ok := m.sessionState[id]
	if !ok {
		return
	}
	screenRestore(m.active, blob)
}

func (m *Model) newClusters() *clusters.Model {
	b := m.boot
	var invalid []config.InvalidCluster
	if b.Loaded != nil {
		invalid = b.Loaded.InvalidClusters
	}
	return clusters.New(clusters.Options{
		Clusters:          b.Clusters,
		InvalidClusters:   invalid,
		CLIName:           b.CLIName,
		AutoSelectCluster: b.AutoSelectCluster,
		GlobalPath:        b.GlobalPath,
		ProjectPath:       b.ProjectPath,
		Pinger:            b.Pinger,
		Editor:            b.Editor,
		StartupWarnings:   b.StartupWarnings,
		Now:               m.now,
		Styles:            m.styles,
	})
}

func (m *Model) newTopics() *topics.Model {
	cfg := m.boot.Loaded.Config
	return topics.New(topics.Options{
		Service:          m.client,
		ReadOnly:         m.clusterRO,
		Columns:          cfg.Topics.Columns,
		FocusTopic:       m.lastTopic,
		RefreshIntervals: m.boot.RefreshIntervals,
		Now:              m.now,
		Styles:           m.styles,
	})
}

func (m *Model) newTopicConfigs() *topics.ConfigsModel {
	return topics.NewConfigsModel(topics.ConfigsOptions{
		Service:  m.client,
		Topic:    m.navTopic,
		ReadOnly: m.clusterRO,
		FocusKey: m.lastConfigKey,
		Now:      m.now,
		Styles:   m.styles,
	})
}

func (m *Model) newTopicConfigEdit() *topics.ConfigEditModel {
	return topics.NewConfigEditModel(topics.ConfigEditOptions{
		Service:      m.client,
		Topic:        m.navTopic,
		Key:          m.navConfigKey,
		CurrentValue: m.navConfigValue,
		Now:          m.now,
		Styles:       m.styles,
	})
}

func (m *Model) newMessages() *messages.Model {
	cfg := m.boot.Loaded.Config
	return messages.New(messages.Options{
		Service:   m.client,
		Topic:     m.navTopic,
		Cluster:   m.activeClu,
		ReadOnly:  m.clusterRO,
		Columns:   cfg.Messages.Columns,
		Clipboard: m.boot.Clipboard,
		ViewState: m.boot.MessagesViewState,
		Now:       m.now,
		Styles:    m.styles,
	})
}

func (m *Model) newProduce() *produce.Model {
	return produce.New(produce.Options{
		Service:            m.client,
		Topic:              m.navTopic,
		Cluster:            m.activeClu,
		ReadOnly:           m.clusterRO,
		Pager:              m.boot.Pager,
		PrefillFromMessage: m.navPrefill,
		Now:                m.now,
		Styles:             m.styles,
	})
}

func (m *Model) newGroups() *groups.Model {
	return groups.New(groups.Options{
		Service:          m.client,
		ReadOnly:         m.clusterRO,
		FilterTopic:      m.navGroupFilter,
		RefreshIntervals: m.boot.RefreshIntervals,
		Now:              m.now,
		Styles:           m.styles,
	})
}

func (m *Model) newLogs() *logs.Model {
	return logs.New(logs.Options{
		Path:   m.boot.LogPath,
		Now:    m.now,
		Styles: m.styles,
	})
}

func (m *Model) newConfigSrc() *configsrc.Model {
	return configsrc.New(configsrc.Options{
		Sources: m.boot.Loaded.Sources,
		Styles:  m.styles,
	})
}
