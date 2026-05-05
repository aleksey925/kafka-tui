package topics

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/help"
	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// ConfigsAction is the host-facing intent of the configs screen.
type ConfigsAction struct {
	Back bool
}

// ConfigsOptions configures a [ConfigsModel].
type ConfigsOptions struct {
	Service Service
	Topic   string
	Now     func() time.Time
	Styles  theme.Styles
}

// ConfigsModel is the topic configs viewer screen.
type ConfigsModel struct {
	svc   Service
	topic string

	configs  []kafka.TopicConfig
	parts    []kafka.PartitionDetail
	cfgTable *components.Table
	partTbl  *components.Table
	toasts   *components.Toasts

	loading       bool
	loadErr       string
	width, height int
	focusParts    bool
	manualRefresh bool

	action ConfigsAction
	now    func() time.Time
	styles theme.Styles
}

func NewConfigsModel(opts ConfigsOptions) *ConfigsModel {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	styles := opts.Styles
	if styles.Palette.Foreground == nil {
		styles = theme.DefaultStyles()
	}
	cfgCols := []components.Column{
		{Title: "Key", Width: 36, Sortable: true},
		{Title: "Value", Width: 32, Sortable: true},
		{Title: "Source", Width: 24, Sortable: true},
	}
	partCols := []components.Column{
		{Title: "Partition", Width: 9, Sortable: true},
		{Title: "Leader", Width: 7, Sortable: true},
		{Title: "Replicas", Width: 18, Sortable: false},
		{Title: "ISR", Width: 18, Sortable: false},
	}
	return &ConfigsModel{
		svc:      opts.Service,
		topic:    opts.Topic,
		cfgTable: components.NewTable(cfgCols, components.WithStyles(styles)),
		partTbl:  components.NewTable(partCols, components.WithStyles(styles)),
		toasts:   components.NewToasts(components.WithToastClock(now), components.WithToastStyles(styles)),
		now:      now,
		styles:   styles,
	}
}

func (m *ConfigsModel) Init() tea.Cmd {
	m.loading = true
	return loadConfigsCmd(m.svc, m.topic)
}

func (m *ConfigsModel) Topic() string { return m.topic }

func (m *ConfigsModel) Action() ConfigsAction { return m.action }

func (m *ConfigsModel) ConsumeAction() ConfigsAction {
	a := m.action
	m.action = ConfigsAction{}
	return a
}

func (m *ConfigsModel) Toasts() *components.Toasts { return m.toasts }

func (m *ConfigsModel) LatestFlash() (components.Toast, bool) {
	if m.toasts == nil {
		return components.Toast{}, false
	}
	return m.toasts.Latest()
}

func (m *ConfigsModel) Title() string {
	return "Topic Configs · " + m.topic
}

func (m *ConfigsModel) Breadcrumb() string {
	if m.focusParts {
		return "partitions"
	}
	return "configs"
}

func (m *ConfigsModel) Configs() []kafka.TopicConfig {
	return append([]kafka.TopicConfig(nil), m.configs...)
}

func (m *ConfigsModel) Partitions() []kafka.PartitionDetail {
	return append([]kafka.PartitionDetail(nil), m.parts...)
}

func (m *ConfigsModel) FocusPartitions() bool { return m.focusParts }

// SetSearch forwards a host-driven filter query to both sub-tables — Tab
// switches focus, not the filter — so an esc cascade always clears it
// without surprising the user.
func (m *ConfigsModel) SetSearch(query string) {
	m.cfgTable.SetSearch(query)
	m.partTbl.SetSearch(query)
}

func (m *ConfigsModel) ActiveFilter() string {
	return m.cfgTable.Search()
}

func (m *ConfigsModel) SetSize(w, h int) {
	m.width, m.height = w, h
	if h > 0 {
		half := maxInt(3, (h-9)/2)
		m.cfgTable.SetHeight(half)
		m.partTbl.SetHeight(half)
	}
}

func (m *ConfigsModel) KeyHints() []layout.KeyHint {
	return layout.HintsFromBindings(m.bindings())
}

func (m *ConfigsModel) HelpSections() []help.Section {
	return help.SectionsFromBindings(m.bindings())
}

func (m *ConfigsModel) bindings() []keymap.Binding {
	return []keymap.Binding{
		{Keys: []string{"tab"}, Label: "switch table", Category: "Topic", Hint: true, Handler: m.actToggleFocus},
		{Keys: []string{"r"}, Label: "refresh now", Category: "Topic", Hint: true, Handler: m.actRefresh},
		{Keys: []string{"esc", "q"}, Label: "back", Category: "Topic", Handler: m.actBack},
		{Keys: []string{"/"}, Label: "filter rows", Category: "Topic", Hint: true},
	}
}

func (m *ConfigsModel) actBack() tea.Cmd {
	m.action.Back = true
	return nil
}

func (m *ConfigsModel) actToggleFocus() tea.Cmd {
	m.focusParts = !m.focusParts
	return nil
}

func (m *ConfigsModel) actRefresh() tea.Cmd {
	m.loading = true
	m.manualRefresh = true
	return loadConfigsCmd(m.svc, m.topic)
}

func (m *ConfigsModel) Update(msg tea.Msg) tea.Cmd {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.SetSize(msg.Width, msg.Height)
		return nil
	case ConfigsLoadedMsg:
		m.handleLoaded(msg)
		return nil
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	}
	return nil
}

func (m *ConfigsModel) handleKey(key tea.KeyPressMsg) tea.Cmd {
	if m.toasts != nil {
		_, _ = m.toasts.Update(key)
	}
	if cmd, ok := keymap.Dispatch(m.bindings(), key); ok {
		return cmd
	}
	tbl, _ := m.activeTable().Update(key)
	if m.focusParts {
		m.partTbl = tbl
	} else {
		m.cfgTable = tbl
	}
	return nil
}

func (m *ConfigsModel) activeTable() *components.Table {
	if m.focusParts {
		return m.partTbl
	}
	return m.cfgTable
}

func (m *ConfigsModel) handleLoaded(msg ConfigsLoadedMsg) {
	m.loading = false
	if msg.Err != nil {
		m.loadErr = msg.Err.Error()
		m.toasts.Push(components.ToastError, "load configs: "+msg.Err.Error())
		m.manualRefresh = false
		return
	}
	m.loadErr = ""
	m.configs = msg.Configs
	m.parts = msg.Partitions
	m.cfgTable.SetRows(configRows(m.configs))
	m.partTbl.SetRows(partitionRows(m.parts))
	if m.manualRefresh {
		m.toasts.Push(components.ToastSuccess, fmt.Sprintf(
			"refreshed · %d configs", len(m.configs),
		))
		m.manualRefresh = false
	}
}

func configRows(cfgs []kafka.TopicConfig) []components.Row {
	out := make([]components.Row, 0, len(cfgs))
	for _, c := range cfgs {
		out = append(out, components.Row{
			ID:     c.Key,
			Values: []string{c.Key, c.Value, c.Source},
		})
	}
	return out
}

func partitionRows(parts []kafka.PartitionDetail) []components.Row {
	out := make([]components.Row, 0, len(parts))
	for _, p := range parts {
		out = append(out, components.Row{
			ID: "p-" + strconv.FormatInt(int64(p.Partition), 10),
			Values: []string{
				strconv.FormatInt(int64(p.Partition), 10),
				strconv.FormatInt(int64(p.Leader), 10),
				replicasFmt(p.Replicas),
				replicasFmt(p.ISR),
			},
		})
	}
	return out
}

func replicasFmt(rs []int32) string {
	if len(rs) == 0 {
		return ""
	}
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		out = append(out, strconv.FormatInt(int64(r), 10))
	}
	return strings.Join(out, ",")
}

func (m *ConfigsModel) View() string {
	cfgTitle := m.styles.HelpTitle.Render(m.formatTitle("Topic configs"))
	partTitle := m.styles.HelpTitle.Render(m.formatTitle("Partitions"))
	parts := []string{
		cfgTitle,
		m.cfgTable.View(),
		"",
		partTitle,
		m.partTbl.View(),
	}
	if m.loading {
		parts = append(parts, m.styles.StatusInfo.Render("(loading…)"))
	}
	if m.loadErr != "" {
		parts = append(parts, m.styles.StatusErr.Render("error: "+m.loadErr))
	}
	return strings.Join(parts, "\n")
}

func (m *ConfigsModel) formatTitle(prefix string) string {
	if m.topic != "" {
		return prefix + " · " + m.topic
	}
	return prefix
}

type ConfigsLoadedMsg struct {
	Configs    []kafka.TopicConfig
	Partitions []kafka.PartitionDetail
	Err        error
}

func loadConfigsCmd(svc Service, topic string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cfgs, err := svc.DescribeAllTopicConfigs(ctx, topic)
		if err != nil {
			return ConfigsLoadedMsg{Err: err}
		}
		parts, err := svc.TopicPartitions(ctx, topic)
		if err != nil {
			return ConfigsLoadedMsg{Configs: cfgs, Err: err}
		}
		return ConfigsLoadedMsg{Configs: cfgs, Partitions: parts}
	}
}
