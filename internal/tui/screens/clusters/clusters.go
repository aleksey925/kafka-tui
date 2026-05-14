// Package clusters implements the cluster-list screen.
package clusters

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/config"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/help"
	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// ConnectionStatus enumerates the in-memory connectivity status of a cluster.
type ConnectionStatus int

const (
	StatusUnknown ConnectionStatus = iota
	StatusChecking
	StatusOK
	StatusFailed
)

func (s ConnectionStatus) Icon() string {
	switch s {
	case StatusChecking:
		return "◐"
	case StatusOK:
		return "✓"
	case StatusFailed:
		return "✗"
	default:
		return "?"
	}
}

func (s ConnectionStatus) Label() string {
	switch s {
	case StatusChecking:
		return "◐ checking…"
	case StatusOK:
		return "✓ ok"
	case StatusFailed:
		return "✗ failed"
	default:
		return "? unknown"
	}
}

// Pinger probes a cluster's broker metadata, returning nil on success.
type Pinger interface {
	Ping(ctx context.Context, c config.Cluster) error
}

type PingerFunc func(ctx context.Context, c config.Cluster) error

func (f PingerFunc) Ping(ctx context.Context, c config.Cluster) error { return f(ctx, c) }

// Editor opens path in the user's $EDITOR. Edit returns a [tea.Cmd] (not the
// result directly) so the real implementation can route through
// [tea.ExecProcess] — the only safe way to spawn a full-screen child process
// from inside bubbletea. A blocking exec.Cmd.Run() corrupts the terminal
// because the parent's raw mode / alt-screen / mouse tracking are not released,
// and the child fights bubbletea for stdin.
//
// The returned Cmd must eventually post an [EditCompletedMsg] back to the program.
type Editor interface {
	Edit(path string) tea.Cmd
}

type EditorFunc func(path string) tea.Cmd

func (f EditorFunc) Edit(path string) tea.Cmd { return f(path) }

// DefaultEditor runs `$EDITOR <path>` (falling back to `vi`) through
// [tea.ExecProcess] so bubbletea can release the terminal cleanly while the
// editor is running and restore it afterwards.
//
// I/O wiring (stdin/stdout/stderr) is intentionally NOT set here — bubbletea
// fills in the program's own streams when they are unset.
func DefaultEditor() Editor {
	return EditorFunc(func(path string) tea.Cmd {
		editor := strings.TrimSpace(os.Getenv("EDITOR"))
		if editor == "" {
			editor = "vi"
		}
		parts := strings.Fields(editor)
		args := append([]string(nil), parts[1:]...)
		args = append(args, path)
		execCmd := exec.CommandContext(context.Background(), parts[0], args...) //nolint:gosec // user-controlled $EDITOR
		return tea.ExecProcess(execCmd, func(runErr error) tea.Msg {
			return EditCompletedMsg{Path: path, Err: runErr}
		})
	})
}

// Action describes the screen's pending intent.
type Action struct {
	Connect string
	Quit    bool
	Reload  bool
}

// Options configure a [Model].
type Options struct {
	Clusters                []config.Cluster
	CLIName                 string
	GlobalPath, ProjectPath string
	Pinger                  Pinger
	Editor                  Editor
	PingTimeout             time.Duration
	StartupWarnings         []string
	Now                     func() time.Time
	Styles                  theme.Styles
}

type editTarget struct {
	Label string
	Path  string
}

type Model struct {
	clusters []config.Cluster
	cliName  string

	statuses map[string]ConnectionStatus
	errors   map[string]string

	table  *components.Table
	toasts *components.Toasts

	pinger      Pinger
	editor      Editor
	pingTimeout time.Duration

	editChoices []editTarget
	editing     bool
	editIdx     int

	action     Action
	stagedInit bool

	width, height int

	// connectivity probes are user-driven; the refresher only stamps
	// LastRefresh() for the chrome's "X ago" indicator on config-snapshot
	// arrival.
	refresher components.Refresher

	startupWarn []string
	now         func() time.Time
	styles      theme.Styles
}

// New builds a Model from Options.
func New(opts Options) *Model {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	styles := opts.Styles
	if styles.Palette.Foreground == nil {
		styles = theme.DefaultStyles()
	}
	editor := opts.Editor
	if editor == nil {
		editor = DefaultEditor()
	}
	timeout := opts.PingTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	clusters := append([]config.Cluster(nil), opts.Clusters...)
	statuses := make(map[string]ConnectionStatus, len(clusters))
	for _, c := range clusters {
		statuses[c.Name] = StatusUnknown
	}

	choices := make([]editTarget, 0, 2)
	if opts.GlobalPath != "" {
		choices = append(choices, editTarget{Label: "global", Path: opts.GlobalPath})
	}
	if opts.ProjectPath != "" {
		choices = append(choices, editTarget{Label: "project", Path: opts.ProjectPath})
	}

	tbl := components.NewTable(columnDefs(), components.WithStyles(styles))

	refresher := components.NewRefresher(0, now)
	// anchor "X ago" to construction time so the chrome shows "0s ago"
	// right after entry instead of waiting for the first watcher snapshot.
	refresher.MarkSuccess()
	m := &Model{
		clusters:    clusters,
		cliName:     opts.CLIName,
		statuses:    statuses,
		errors:      map[string]string{},
		table:       tbl,
		toasts:      components.NewToasts(components.WithToastClock(now), components.WithToastStyles(styles)),
		pinger:      opts.Pinger,
		editor:      editor,
		pingTimeout: timeout,
		editChoices: choices,
		refresher:   refresher,
		startupWarn: append([]string(nil), opts.StartupWarnings...),
		now:         now,
		styles:      styles,
	}
	m.refreshTable()
	return m
}

// status column is non-sortable: status is volatile.
func columnDefs() []components.Column {
	return []components.Column{
		{Title: " ", Width: 1},
		{Title: "Name", Width: 24, Sortable: true},
		{Title: "Brokers", Width: 32, Sortable: true},
		{Title: "Flags", Width: 12, Sortable: false},
		{Title: "Status", Width: 14, Sortable: false},
	}
}

// SkipTarget reports the cluster name to bypass the screen for: a CLI inline
// cluster (priority) or the only configured cluster.
func (m *Model) SkipTarget() (string, bool) {
	if m.cliName != "" {
		return m.cliName, true
	}
	if len(m.clusters) == 1 {
		return m.clusters[0].Name, true
	}
	return "", false
}

func (m *Model) Init() tea.Cmd {
	for _, w := range m.startupWarn {
		m.toasts.PushWithLifetime(components.ToastWarning, w, 5*time.Second)
	}
	m.startupWarn = nil
	return nil
}

func (m *Model) RefreshInterval() time.Duration { return m.refresher.Interval() }

func (m *Model) SetRefreshPaused(paused bool) { m.refresher.SetPaused(paused) }

func (m *Model) Action() Action { return m.action }

func (m *Model) ConsumeAction() Action {
	a := m.action
	m.action = Action{}
	return a
}

func (m *Model) Status(name string) ConnectionStatus { return m.statuses[name] }

// SetClusters replaces the cluster list (host calls this after a reload).
// Statuses are preserved by name; missing clusters drop out; new ones get
// StatusUnknown. The cursor stays on the same cluster name when possible.
func (m *Model) SetClusters(list []config.Cluster, cliName string) {
	m.clusters = append([]config.Cluster(nil), list...)
	m.cliName = cliName
	m.refresher.MarkSuccess()
	keep := make(map[string]ConnectionStatus, len(list))
	keepErr := make(map[string]string, len(list))
	for _, c := range list {
		if s, ok := m.statuses[c.Name]; ok {
			keep[c.Name] = s
		} else {
			keep[c.Name] = StatusUnknown
		}
		if e, ok := m.errors[c.Name]; ok {
			keepErr[c.Name] = e
		}
	}
	m.statuses = keep
	m.errors = keepErr
	prevID := ""
	if row, ok := m.table.SelectedRow(); ok {
		prevID = row.ID
	}
	m.refreshTable()
	if prevID != "" {
		m.table.GoToID(prevID)
	}
}

func (m *Model) Toasts() *components.Toasts { return m.toasts }

func (m *Model) LatestFlash() (components.Toast, bool) {
	if m.toasts == nil {
		return components.Toast{}, false
	}
	return m.toasts.Latest()
}

func (m *Model) Title() string {
	total := len(m.clusters)
	if q := m.table.Search(); q != "" {
		return fmt.Sprintf("Clusters [%d/%d] </%s>", m.table.FilteredCount(), total, q)
	}
	return fmt.Sprintf("Clusters [%d]", total)
}

func (m *Model) Breadcrumb() string {
	row, ok := m.table.SelectedRow()
	if !ok {
		return ""
	}
	return row.ID
}

func (m *Model) SetSearch(query string) { m.table.SetSearch(query) }

func (m *Model) ActiveFilter() string { return m.table.Search() }

func (m *Model) HasOverlay() bool { return m.editing }

func (m *Model) SetSize(w, h int) {
	m.width, m.height = w, h
	if h > 0 {
		m.table.SetHeight(maxInt(1, h-6))
	}
	if w > 0 {
		m.table.SetTotalWidth(w)
	}
}

func (m *Model) KeyHints() []layout.KeyHint {
	return layout.HintsFromBindings(m.activeBindings())
}

func (m *Model) HelpSections() []help.Section {
	return help.SectionsFromBindings(m.activeBindings())
}

func (m *Model) activeBindings() []keymap.Binding {
	if m.editing {
		return m.editChooserBindings()
	}
	return m.listBindings()
}

func (m *Model) listBindings() []keymap.Binding {
	return []keymap.Binding{
		{Keys: []string{"enter"}, Label: "connect to cluster", Category: "Cluster", Hint: true, Handler: m.connectCurrent},
		{Keys: []string{"t"}, Label: "test connectivity", Category: "Cluster", Hint: true, Handler: m.testCurrent},
		{Keys: []string{"T"}, Label: "test all clusters", Category: "Cluster", Hint: true, Handler: m.testAll},
		{Keys: []string{"r"}, Label: "reload config from disk", Category: "Cluster", Hint: true, Handler: m.actReload},
		{Keys: []string{"e"}, Label: "edit clusters.yaml", Category: "Cluster", Hint: true, Handler: m.openEditChooser},
		{Keys: []string{"q"}, Label: "quit", Category: "Cluster", Handler: m.actQuit},
		{Keys: []string{"/"}, Label: "filter rows", Category: "Cluster", Hint: true},
	}
}

func (m *Model) actReload() tea.Cmd { m.action.Reload = true; return nil }
func (m *Model) actQuit() tea.Cmd   { m.action.Quit = true; return nil }

func (m *Model) Update(msg tea.Msg) tea.Cmd {
	if !m.stagedInit {
		m.stagedInit = true
	}

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.SetSize(msg.Width, msg.Height)
		return nil
	case PingResultMsg:
		m.handlePingResult(msg)
		return nil
	case EditCompletedMsg:
		m.handleEditCompleted(msg)
		return nil
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	}
	return nil
}

func (m *Model) handleKey(key tea.KeyPressMsg) tea.Cmd {
	if m.editing {
		return m.handleEditChooserKey(key)
	}
	if m.toasts != nil {
		_, _ = m.toasts.Update(key)
	}

	if key.String() == "esc" {
		// esc on the root screen must not quit the app.
		return nil
	}
	if cmd, ok := keymap.Dispatch(m.listBindings(), key); ok {
		return cmd
	}
	tbl, _ := m.table.Update(key)
	m.table = tbl
	return nil
}

func (m *Model) handleEditChooserKey(key tea.KeyPressMsg) tea.Cmd {
	cmd, _ := keymap.Dispatch(m.editChooserBindings(), key)
	return cmd
}

func (m *Model) editChooserBindings() []keymap.Binding {
	return []keymap.Binding{
		{Keys: []string{"esc", "q"}, Label: "back", Category: "Edit chooser", Hint: true, Handler: m.actChooserCancel},
		{Keys: []string{"j", "down"}, Label: "next target", Category: "Edit chooser", Handler: m.actChooserMove(+1)},
		{Keys: []string{"k", "up"}, Label: "previous target", Category: "Edit chooser", Handler: m.actChooserMove(-1)},
		{Keys: []string{"enter"}, Label: "open in $EDITOR", Category: "Edit chooser", Hint: true, Handler: m.actChooserPick},
	}
}

func (m *Model) actChooserCancel() tea.Cmd { m.editing = false; return nil }

func (m *Model) actChooserMove(delta int) func() tea.Cmd {
	return func() tea.Cmd {
		if len(m.editChoices) == 0 {
			return nil
		}
		n := len(m.editChoices)
		m.editIdx = (m.editIdx + delta + n) % n
		return nil
	}
}

func (m *Model) actChooserPick() tea.Cmd {
	if len(m.editChoices) == 0 {
		m.editing = false
		return nil
	}
	path := m.editChoices[m.editIdx].Path
	m.editing = false
	return m.runEditor(path)
}

func (m *Model) openEditChooser() tea.Cmd {
	if len(m.editChoices) == 0 {
		m.toasts.Push(components.ToastWarning, "no clusters.yaml location is configured")
		return nil
	}
	if len(m.editChoices) == 1 {
		return m.runEditor(m.editChoices[0].Path)
	}
	m.editing = true
	m.editIdx = 0
	return nil
}

func (m *Model) connectCurrent() tea.Cmd {
	row, ok := m.table.SelectedRow()
	if !ok {
		return nil
	}
	name := row.ID
	c := m.findCluster(name)
	if c == nil {
		return nil
	}
	if m.pinger == nil {
		m.action.Connect = name
		m.statuses[name] = StatusOK
		m.refreshTable()
		return nil
	}
	m.statuses[name] = StatusChecking
	m.refreshTable()
	return pingCmd(m.pinger, *c, m.pingTimeout, intentConnect)
}

func (m *Model) testCurrent() tea.Cmd {
	row, ok := m.table.SelectedRow()
	if !ok {
		return nil
	}
	name := row.ID
	c := m.findCluster(name)
	if c == nil || m.pinger == nil {
		return nil
	}
	m.statuses[name] = StatusChecking
	m.refreshTable()
	return pingCmd(m.pinger, *c, m.pingTimeout, intentTest)
}

func (m *Model) testAll() tea.Cmd {
	if m.pinger == nil {
		return nil
	}
	cmds := make([]tea.Cmd, 0, len(m.clusters))
	for _, c := range m.clusters {
		m.statuses[c.Name] = StatusChecking
		cmds = append(cmds, pingCmd(m.pinger, c, m.pingTimeout, intentTest))
	}
	m.refreshTable()
	if len(cmds) == 0 {
		return nil
	}
	return tea.Batch(cmds...)
}

func (m *Model) LastRefresh() time.Time { return m.refresher.LastRefresh() }

func (m *Model) findCluster(name string) *config.Cluster {
	for i := range m.clusters {
		if m.clusters[i].Name == name {
			return &m.clusters[i]
		}
	}
	return nil
}

func (m *Model) handlePingResult(msg PingResultMsg) {
	if msg.Err != nil {
		m.statuses[msg.Name] = StatusFailed
		m.errors[msg.Name] = msg.Err.Error()
		m.toasts.Push(components.ToastError, fmt.Sprintf("%s: %s", msg.Name, msg.Err.Error()))
	} else {
		m.statuses[msg.Name] = StatusOK
		delete(m.errors, msg.Name)
		if msg.Intent == intentConnect {
			m.action.Connect = msg.Name
		}
	}
	m.refreshTable()
}

func (m *Model) runEditor(path string) tea.Cmd {
	return m.editor.Edit(path)
}

func (m *Model) handleEditCompleted(msg EditCompletedMsg) {
	if msg.Err != nil {
		m.toasts.Push(components.ToastError, "editor: "+msg.Err.Error())
		return
	}
	m.toasts.Push(components.ToastInfo, "saved "+msg.Path+" — reload pending")
}

func (m *Model) refreshTable() {
	rows := make([]components.Row, 0, len(m.clusters))
	for _, c := range m.clusters {
		rows = append(rows, components.Row{
			ID:     c.Name,
			Values: m.rowValues(c),
		})
	}
	m.table.SetRows(rows)
}

func (m *Model) rowValues(c config.Cluster) []string {
	// leading dot reflects the configured cluster color; the Status column
	// shows live connectivity.
	colorDot := lipgloss.NewStyle().
		Foreground(m.styles.Palette.ClusterColor(c.Color)).
		Render("●")
	name := c.Name
	flags := []string{}
	if c.ReadOnly {
		flags = append(flags, "[RO]")
	}
	if c.Name == m.cliName {
		flags = append(flags, "(cli)")
	}
	return []string{
		colorDot,
		name,
		strings.Join(c.Brokers, ","),
		strings.Join(flags, " "),
		m.statuses[c.Name].Label(),
	}
}

func (m *Model) View() string {
	parts := []string{m.table.View()}
	if m.editing {
		parts = append(parts, m.renderEditChooser())
	}
	return strings.Join(parts, "\n")
}

func (m *Model) EditingChooser() bool { return m.editing }

func (m *Model) EditChoices() []string {
	out := make([]string, 0, len(m.editChoices))
	for _, c := range m.editChoices {
		out = append(out, c.Label)
	}
	return out
}

func (m *Model) EditCursor() int { return m.editIdx }

func (m *Model) renderEditChooser() string {
	lines := []string{m.styles.HelpTitle.Render("Edit clusters.yaml")}
	for i, c := range m.editChoices {
		marker := "( ) "
		style := m.styles.Command
		if i == m.editIdx {
			marker = "(•) "
			style = m.styles.CommandHL
		}
		lines = append(lines, "  "+style.Render(marker+c.Label+"  "+c.Path))
	}
	lines = append(lines, "", m.styles.HintLabel.Render("enter select  esc cancel"))
	box := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		Padding(0, 2).
		Render(strings.Join(lines, "\n"))
	return box
}

// ----- Messages -----

type pingIntent int

const (
	intentTest pingIntent = iota
	intentConnect
)

type PingResultMsg struct {
	Name   string
	Err    error
	Intent pingIntent
}

type EditCompletedMsg struct {
	Path string
	Err  error
}

func pingCmd(p Pinger, c config.Cluster, timeout time.Duration, intent pingIntent) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		err := p.Ping(ctx, c)
		return PingResultMsg{Name: c.Name, Err: err, Intent: intent}
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
