// Screen instantiation — given a [ScreenID] and the active bootstrap,
// build a fresh screen Model wired with its options. Each screen has
// its own factory because options vary by screen; collectively they
// keep the host free of knowledge about each screen's option struct.

package tui

import (
	"strings"
	"time"

	"github.com/aleksey925/kafka-tui/internal/tui/screens/clusters"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/configsrc"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/groups"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/logs"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/messages"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/produce"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/topics"
)

// instantiate constructs the screen for id using the current bootstrap. When
// boot is nil or required deps (e.g. *kafka.Client for topic screens) are
// absent, the active screen is left nil and [renderBody] falls back to the
// placeholder.
func (m *Model) instantiate(id ScreenID) {
	if m.boot == nil {
		return
	}
	// when re-instantiating after a pop the nav seeds are empty; fall back to
	// lastTopic so topic-bound screens (messages, produce) recreate against the
	// correct topic instead of an empty one.
	if m.navTopic == "" {
		m.navTopic = m.lastTopic
	}
	defer m.clearNavSeeds()
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
	m.navTopicsFilter = nil
	m.navPrefill = nil
	m.navGroupFilter = ""
}

func (m *Model) newClusters() *clusters.Model {
	b := m.boot
	return clusters.New(clusters.Options{
		Clusters:        b.Clusters,
		CLIName:         b.CLIName,
		GlobalPath:      b.GlobalPath,
		ProjectPath:     b.ProjectPath,
		Pinger:          b.Pinger,
		Editor:          b.Editor,
		StartupWarnings: b.StartupWarnings,
		Now:             m.now,
		Styles:          m.styles,
	})
}

func (m *Model) newTopics() *topics.Model {
	cfg := m.boot.Loaded.Config
	return topics.New(topics.Options{
		Service:         m.client,
		ReadOnly:        m.clusterRO,
		Columns:         cfg.Topics.Columns,
		FilterTopics:    m.navTopicsFilter,
		FocusTopic:      m.lastTopic,
		RefreshInterval: parseRefresh(cfg.Refresh.TopicsList),
		Now:             m.now,
		Styles:          m.styles,
	})
}

func (m *Model) newTopicConfigs() *topics.ConfigsModel {
	return topics.NewConfigsModel(topics.ConfigsOptions{
		Service: m.client,
		Topic:   m.navTopic,
		Now:     m.now,
		Styles:  m.styles,
	})
}

func (m *Model) newMessages() *messages.Model {
	cfg := m.boot.Loaded.Config
	return messages.New(messages.Options{
		Service:   m.client,
		Topic:     m.navTopic,
		ReadOnly:  m.clusterRO,
		Columns:   cfg.Messages.Columns,
		Clipboard: m.boot.Clipboard,
		Now:       m.now,
		Styles:    m.styles,
	})
}

func (m *Model) newProduce() *produce.Model {
	cfg := m.boot.Loaded.Config
	return produce.New(produce.Options{
		Service:            m.client,
		Cluster:            m.activeClu,
		Topic:              m.navTopic,
		ReadOnly:           m.clusterRO,
		HistorySize:        cfg.Produce.HistorySize,
		History:            m.boot.History,
		Pager:              m.boot.Pager,
		PrefillFromMessage: m.navPrefill,
		Now:                m.now,
		Styles:             m.styles,
	})
}

func (m *Model) newGroups() *groups.Model {
	cfg := m.boot.Loaded.Config
	return groups.New(groups.Options{
		Service:               m.client,
		ReadOnly:              m.clusterRO,
		FilterTopic:           m.navGroupFilter,
		ListRefreshInterval:   parseRefresh(cfg.Refresh.GroupsList),
		DetailRefreshInterval: parseRefresh(cfg.Refresh.GroupDetail),
		Now:                   m.now,
		Styles:                m.styles,
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

// parseRefresh converts the config string ("off", "5s", …) into a duration.
// "off" / unparseable / zero map to 0 (auto-refresh disabled).
func parseRefresh(s string) time.Duration {
	s = strings.TrimSpace(s)
	if s == "" || strings.EqualFold(s, "off") {
		return 0
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0
	}
	if d < 0 {
		return 0
	}
	return d
}
