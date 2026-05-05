// Package configsrc implements the `:config sources` screen — a sortable,
// searchable table of every explicitly-set config field plus its provenance
// (which layer/file produced the final value).
//
// Provenance metadata is computed by the config loader during merge and
// surfaced here as two tables: top-level config keys and per-cluster fields.
package configsrc

import (
	"fmt"
	"sort"
	"strings"

	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/config"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/help"
	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// Action describes the screen's pending intent for the host (router).
type Action struct {
	// Back signals the user pressed esc/q.
	Back bool
}

// Options configure a [Model].
type Options struct {
	// Sources is the field-origin metadata produced by config.Load. Required.
	Sources config.Sources
	// Styles overrides the theme palette (mostly for tests).
	Styles theme.Styles
}

// Model is the config sources viewer screen.
type Model struct {
	sources config.Sources

	cfgTable     *components.Table
	clusterTable *components.Table

	focusClusters bool

	width, height int

	action Action
	styles theme.Styles
}

// New constructs a Model.
func New(opts Options) *Model {
	styles := opts.Styles
	if styles.Palette.Foreground == nil {
		styles = theme.DefaultStyles()
	}
	cfgCols := []components.Column{
		{Title: "Field", Width: 32, Sortable: true},
		{Title: "Layer", Width: 10, Sortable: true},
		{Title: "Source", Width: 0, Sortable: true},
	}
	clusterCols := []components.Column{
		{Title: "Cluster", Width: 18, Sortable: true},
		{Title: "Field", Width: 28, Sortable: true},
		{Title: "Layer", Width: 10, Sortable: true},
		{Title: "Source", Width: 0, Sortable: true},
	}
	m := &Model{
		sources:      opts.Sources,
		cfgTable:     components.NewTable(cfgCols, components.WithStyles(styles)),
		clusterTable: components.NewTable(clusterCols, components.WithStyles(styles)),
		styles:       styles,
	}
	m.cfgTable.SetRows(buildConfigRows(opts.Sources.Config))
	m.clusterTable.SetRows(buildClusterRows(opts.Sources.Clusters))
	return m
}

// Init implements the screen contract (no async work to dispatch).
func (m *Model) Init() tea.Cmd { return nil }

// Action returns the pending action.
func (m *Model) Action() Action { return m.action }

// ConsumeAction returns and clears the pending action.
func (m *Model) ConsumeAction() Action {
	a := m.action
	m.action = Action{}
	return a
}

// FocusClusters reports whether the per-cluster table currently has focus.
func (m *Model) FocusClusters() bool { return m.focusClusters }

// Title returns the frame title rendered by the host.
func (m *Model) Title() string { return "Config Sources" }

// Breadcrumb describes which sub-table is active.
func (m *Model) Breadcrumb() string {
	if m.focusClusters {
		return "clusters"
	}
	return "config"
}

// SetSearch forwards a host-driven filter query to both sub-tables. The
// screen treats the host's `/` prompt as a single screen-level filter:
// Tab only switches navigation focus, so an esc cascade can always
// clear the filter without depending on which table is currently in
// focus.
func (m *Model) SetSearch(query string) {
	m.cfgTable.SetSearch(query)
	m.clusterTable.SetSearch(query)
}

// ActiveFilter returns the screen-level search query. Both tables hold
// the same value (see [SetSearch]), so the config table is queried
// directly.
func (m *Model) ActiveFilter() string {
	return m.cfgTable.Search()
}

// SetSize updates width/height.
func (m *Model) SetSize(w, h int) {
	m.width, m.height = w, h
	if h > 0 {
		half := max(3, (h-9)/2)
		m.cfgTable.SetHeight(half)
		m.clusterTable.SetHeight(half)
	}
}

// KeyHints derives the bottom-row entries from the bindings table.
func (m *Model) KeyHints() []layout.KeyHint {
	return layout.HintsFromBindings(m.bindings())
}

// HelpSections derives the `?`-overlay sections from the same source.
func (m *Model) HelpSections() []help.Section {
	return help.SectionsFromBindings(m.bindings())
}

// bindings is the single source of truth for config-sources shortcuts.
// Sort and search keys are claimed by the underlying table component
// (advertise-only here so they appear in help and hints alike).
func (m *Model) bindings() []keymap.Binding {
	return []keymap.Binding{
		{Keys: []string{"tab"}, Label: "switch focused table", Category: "Config sources", Hint: true, Handler: m.actSwitchTable},
		{Keys: []string{"esc", "q"}, Label: "back", Category: "Config sources", Handler: m.actBack},
		{Keys: []string{"/"}, Label: "filter rows", Category: "Config sources", Hint: true},
		{Keys: []string{"s", "S"}, Label: "cycle sort", Category: "Config sources", Hint: true},
	}
}

func (m *Model) actSwitchTable() tea.Cmd { m.focusClusters = !m.focusClusters; return nil }
func (m *Model) actBack() tea.Cmd        { m.action.Back = true; return nil }

// Update routes messages.
func (m *Model) Update(msg tea.Msg) tea.Cmd {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.SetSize(msg.Width, msg.Height)
		return nil
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	}
	return nil
}

func (m *Model) handleKey(key tea.KeyPressMsg) tea.Cmd {
	if cmd, ok := keymap.Dispatch(m.bindings(), key); ok {
		return cmd
	}
	tbl, _ := m.activeTable().Update(key)
	if m.focusClusters {
		m.clusterTable = tbl
	} else {
		m.cfgTable = tbl
	}
	return nil
}

func (m *Model) activeTable() *components.Table {
	if m.focusClusters {
		return m.clusterTable
	}
	return m.cfgTable
}

// View renders the screen body.
func (m *Model) View() string {
	cfgTitle := m.styles.HelpTitle.Render(m.formatTitle("Config fields", !m.focusClusters))
	clusterTitle := m.styles.HelpTitle.Render(m.formatTitle("Cluster fields", m.focusClusters))
	parts := []string{
		cfgTitle,
		m.cfgTable.View(),
		"",
		clusterTitle,
		m.clusterTable.View(),
	}
	return strings.Join(parts, "\n")
}

func (m *Model) formatTitle(prefix string, active bool) string {
	if active {
		return prefix + " (active)"
	}
	return prefix
}

func buildConfigRows(src map[string]config.Source) []components.Row {
	keys := make([]string, 0, len(src))
	for k := range src {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	rows := make([]components.Row, 0, len(keys))
	for _, k := range keys {
		s := src[k]
		rows = append(rows, components.Row{
			ID:     k,
			Values: []string{k, string(s.Layer), s.Path},
		})
	}
	return rows
}

func buildClusterRows(src map[string]map[string]config.Source) []components.Row {
	clusters := make([]string, 0, len(src))
	for c := range src {
		clusters = append(clusters, c)
	}
	sort.Strings(clusters)
	rows := make([]components.Row, 0)
	for _, cluster := range clusters {
		fields := make([]string, 0, len(src[cluster]))
		for f := range src[cluster] {
			fields = append(fields, f)
		}
		sort.Strings(fields)
		for _, f := range fields {
			s := src[cluster][f]
			rows = append(rows, components.Row{
				ID:     fmt.Sprintf("%s.%s", cluster, f),
				Values: []string{cluster, f, string(s.Layer), s.Path},
			})
		}
	}
	return rows
}
