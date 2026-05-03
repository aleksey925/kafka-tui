package topics

import (
	"context"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// ConfigsAction is the host-facing intent of the configs screen. Today the
// only outbound transition is "back to topics list" via esc/q.
type ConfigsAction struct {
	Back bool
}

// ConfigsOptions configures a [ConfigsModel].
type ConfigsOptions struct {
	// Service is the Kafka admin abstraction. Required.
	Service Service
	// Topic is the topic name to inspect. Required.
	Topic string
	// Now is the injected clock (defaults to time.Now).
	Now func() time.Time
	// Styles overrides the theme palette (mostly for tests).
	Styles theme.Styles
}

// ConfigsModel is the topic configs viewer screen. It shows two tables:
// the resolved topic-level configs (key/value/source) and the partition
// metadata (partition/leader/replicas/isr).
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

	action ConfigsAction
	now    func() time.Time
	styles theme.Styles
}

// NewConfigsModel constructs a configs viewer for the given topic.
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

// Init dispatches the initial load.
func (m *ConfigsModel) Init() tea.Cmd {
	m.loading = true
	return loadConfigsCmd(m.svc, m.topic)
}

// Topic returns the topic this screen is bound to.
func (m *ConfigsModel) Topic() string { return m.topic }

// Action returns the pending host action.
func (m *ConfigsModel) Action() ConfigsAction { return m.action }

// ConsumeAction returns and clears the pending action.
func (m *ConfigsModel) ConsumeAction() ConfigsAction {
	a := m.action
	m.action = ConfigsAction{}
	return a
}

// Toasts exposes the toast queue.
func (m *ConfigsModel) Toasts() *components.Toasts { return m.toasts }

// LatestFlash returns the freshest live toast from this screen's queue.
func (m *ConfigsModel) LatestFlash() (components.Toast, bool) {
	if m.toasts == nil {
		return components.Toast{}, false
	}
	return m.toasts.Latest()
}

// Title returns the frame title rendered by the host.
func (m *ConfigsModel) Title() string {
	return "Topic Configs · " + m.topic
}

// Breadcrumb returns the active sub-table indicator.
func (m *ConfigsModel) Breadcrumb() string {
	if m.focusParts {
		return "partitions"
	}
	return "configs"
}

// Configs returns the current loaded config rows (for tests).
func (m *ConfigsModel) Configs() []kafka.TopicConfig {
	return append([]kafka.TopicConfig(nil), m.configs...)
}

// Partitions returns the current loaded partition rows (for tests).
func (m *ConfigsModel) Partitions() []kafka.PartitionDetail {
	return append([]kafka.PartitionDetail(nil), m.parts...)
}

// FocusPartitions reports whether the partition table currently has focus.
func (m *ConfigsModel) FocusPartitions() bool { return m.focusParts }

// SetSearch forwards a host-driven filter query to the focused sub-table.
func (m *ConfigsModel) SetSearch(query string) {
	if m.focusParts {
		m.partTbl.SetSearch(query)
		return
	}
	m.cfgTable.SetSearch(query)
}

// ActiveFilter returns the focused sub-table's current search query.
func (m *ConfigsModel) ActiveFilter() string {
	if m.focusParts {
		return m.partTbl.Search()
	}
	return m.cfgTable.Search()
}

// SetSize updates width/height.
func (m *ConfigsModel) SetSize(w, h int) {
	m.width, m.height = w, h
	if h > 0 {
		// half each, leaving chrome for header and key hints
		half := maxInt(3, (h-9)/2)
		m.cfgTable.SetHeight(half)
		m.partTbl.SetHeight(half)
	}
}

// KeyHints returns the screen-specific hints.
func (m *ConfigsModel) KeyHints() []layout.KeyHint {
	return []layout.KeyHint{
		{Key: "tab", Label: "switch table"},
		{Key: "/", Label: "search"},
		{Key: "s/S", Label: "sort"},
		{Key: "esc/q", Label: "back"},
	}
}

// Update routes messages.
func (m *ConfigsModel) Update(msg tea.Msg) (*ConfigsModel, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.SetSize(msg.Width, msg.Height)
		return m, nil
	case ConfigsLoadedMsg:
		m.handleLoaded(msg)
		return m, nil
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

func (m *ConfigsModel) handleKey(key tea.KeyPressMsg) (*ConfigsModel, tea.Cmd) {
	if m.toasts != nil {
		_, _ = m.toasts.Update(key)
	}

	active := m.activeTable()
	if active.SearchActive() {
		t, _ := active.Update(key)
		_ = t
		return m, nil
	}

	switch key.String() {
	case "esc", "q":
		m.action.Back = true
		return m, nil
	case "tab":
		m.focusParts = !m.focusParts
		return m, nil
	case "r":
		m.loading = true
		return m, loadConfigsCmd(m.svc, m.topic)
	}
	t, _ := active.Update(key)
	_ = t
	return m, nil
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
		return
	}
	m.loadErr = ""
	m.configs = msg.Configs
	m.parts = msg.Partitions
	m.cfgTable.SetRows(configRows(m.configs))
	m.partTbl.SetRows(partitionRows(m.parts))
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

// View renders the screen body.
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

// ConfigsLoadedMsg is dispatched when the topic-level configs and partition
// metadata have been fetched.
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
