// Package clusters implements the cluster-list screen — the first screen the
// TUI shows when more than one cluster is configured.
//
// The screen renders a sortable, searchable table of clusters with a colored
// `●` swatch, `[RO]` and `(cli)` markers, and a connection-status column
// (`✓ ok` / `✗ failed` / `? unknown` / `◐ checking…`). It owns no Kafka client
// itself — connectivity probes are dispatched through a pluggable [Pinger] and
// editing of `clusters.yaml` through a pluggable [Editor], which keeps the
// screen unit-testable without touching real brokers or the user's editor.
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
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// ConnectionStatus enumerates the in-memory connectivity status of a cluster.
type ConnectionStatus int

const (
	// StatusUnknown means no probe has been attempted yet.
	StatusUnknown ConnectionStatus = iota
	// StatusChecking means a probe is in flight.
	StatusChecking
	// StatusOK means the most recent probe succeeded.
	StatusOK
	// StatusFailed means the most recent probe failed.
	StatusFailed
)

// Icon returns the single-character glyph used in the status column.
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

// Label returns the icon + word combination used in the status column body.
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

// PingerFunc adapts a function into a [Pinger].
type PingerFunc func(ctx context.Context, c config.Cluster) error

// Ping calls f.
func (f PingerFunc) Ping(ctx context.Context, c config.Cluster) error { return f(ctx, c) }

// Editor opens path in the user's `$EDITOR`. Implementations block until the
// editor exits.
type Editor interface {
	Edit(path string) error
}

// EditorFunc adapts a function into an [Editor].
type EditorFunc func(path string) error

// Edit calls f.
func (f EditorFunc) Edit(path string) error { return f(path) }

// DefaultEditor returns an [Editor] that runs `$EDITOR <path>` (falling back
// to `vi`) attached to the current TTY. It is unsuitable for unit tests; tests
// inject an [EditorFunc].
func DefaultEditor() Editor {
	return EditorFunc(func(path string) error {
		editor := os.Getenv("EDITOR")
		if editor == "" {
			editor = "vi"
		}
		cmd := exec.CommandContext(context.Background(), editor, path) //nolint:gosec // user-controlled $EDITOR
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd.Run()
	})
}

// Action describes the screen's pending intent. Read after every Update; the
// host (router) reacts and clears it via [Model.ConsumeAction].
type Action struct {
	// Connect names the cluster the user wants to switch to. The host is
	// expected to push the topics screen.
	Connect string
	// Quit signals the user pressed esc/q with no other screen below.
	Quit bool
}

// Options configure a [Model].
type Options struct {
	// Clusters is the (already-resolved) list of clusters. Order is preserved.
	Clusters []config.Cluster
	// CLIName, when non-empty, marks the cluster from --brokers; that cluster
	// will be displayed with a `(cli)` tag in the screen body.
	CLIName string
	// GlobalPath / ProjectPath are the absolute paths shown in the edit-target
	// chooser. Either may be empty (the chooser will hide that option).
	GlobalPath, ProjectPath string
	// Pinger probes connectivity. If nil, status checking is disabled — only
	// [StatusUnknown] is shown.
	Pinger Pinger
	// Editor opens `clusters.yaml`. Defaults to [DefaultEditor].
	Editor Editor
	// PingTimeout caps each probe. Defaults to 5s.
	PingTimeout time.Duration
	// StartupWarnings are surfaced as warning toasts on first Init.
	StartupWarnings []string
	// Now is the injected clock (defaults to time.Now).
	Now func() time.Time
	// Styles overrides the theme palette (mostly for tests).
	Styles theme.Styles
}

// editTarget identifies one of the chooser entries.
type editTarget struct {
	Label string
	Path  string
}

// Model is the clusters screen.
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

	startupWarn []string
	now         func() time.Time
	styles      theme.Styles
}

// New builds a Model from Options. Status is initialized to Unknown for every
// cluster.
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
		startupWarn: append([]string(nil), opts.StartupWarnings...),
		now:         now,
		styles:      styles,
	}
	m.refreshTable()
	return m
}

// columnDefs returns the table column definitions used by the cluster list.
// The status column is non-sortable: status is volatile.
func columnDefs() []components.Column {
	return []components.Column{
		{Title: " ", Width: 1},
		{Title: "Name", Width: 24, Sortable: true},
		{Title: "Brokers", Width: 32, Sortable: true},
		{Title: "Flags", Width: 12, Sortable: false},
		{Title: "Status", Width: 14, Sortable: false},
	}
}

// SkipTarget reports the cluster name to bypass the screen for. Two cases
// trigger a skip:
//   - exactly one cluster is configured, OR
//   - a CLI inline cluster was supplied (its name takes priority).
//
// The host should call this once before pushing the screen onto the router.
func (m *Model) SkipTarget() (string, bool) {
	if m.cliName != "" {
		return m.cliName, true
	}
	if len(m.clusters) == 1 {
		return m.clusters[0].Name, true
	}
	return "", false
}

// Init returns the startup commands: pushing any warning toasts queued at
// construction and (eventually) probing every cluster's connectivity. The
// initial probe is dispatched lazily on the first Update so tests that never
// drive Update don't accidentally fire it.
func (m *Model) Init() tea.Cmd {
	for _, w := range m.startupWarn {
		m.toasts.PushWithLifetime(components.ToastWarning, w, 5*time.Second)
	}
	m.startupWarn = nil
	return nil
}

// Action returns the current pending action.
func (m *Model) Action() Action { return m.action }

// ConsumeAction returns the pending action and clears it.
func (m *Model) ConsumeAction() Action {
	a := m.action
	m.action = Action{}
	return a
}

// Status returns the current status of the named cluster (or [StatusUnknown]).
func (m *Model) Status(name string) ConnectionStatus { return m.statuses[name] }

// Toasts exposes the toast queue (mostly for tests).
func (m *Model) Toasts() *components.Toasts { return m.toasts }

// SetSize updates width/height (called when the host receives WindowSizeMsg).
func (m *Model) SetSize(w, h int) {
	m.width, m.height = w, h
	if h > 0 {
		// reserve a few rows for chrome (header, toast, hints).
		m.table.SetHeight(maxInt(1, h-6))
	}
}

// KeyHints returns the screen-specific hints shown at the bottom row.
func (m *Model) KeyHints() []layout.KeyHint {
	return []layout.KeyHint{
		{Key: "enter", Label: "connect"},
		{Key: "t/T", Label: "test/all"},
		{Key: "e", Label: "edit"},
		{Key: "r", Label: "refresh"},
		{Key: "/", Label: "search"},
	}
}

// Update routes messages.
func (m *Model) Update(msg tea.Msg) (*Model, tea.Cmd) {
	if !m.stagedInit {
		m.stagedInit = true
		// preserve init's side effects (toasts already queued).
	}

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.SetSize(msg.Width, msg.Height)
		return m, nil
	case PingResultMsg:
		m.handlePingResult(msg)
		return m, nil
	case EditCompletedMsg:
		m.handleEditCompleted(msg)
		return m, nil
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

func (m *Model) handleKey(key tea.KeyPressMsg) (*Model, tea.Cmd) {
	if m.editing {
		return m.handleEditChooserKey(key)
	}
	// any keypress dismisses the topmost sticky toast, per §7.11.
	if m.toasts != nil {
		_, _ = m.toasts.Update(key)
	}

	// while the table is in fuzzy-search mode, every keypress belongs to the
	// search prompt — don't intercept hotkey letters.
	if m.table.SearchActive() {
		tbl, _ := m.table.Update(key)
		m.table = tbl
		return m, nil
	}

	switch key.String() {
	case "enter":
		cmd := m.connectCurrent()
		return m, cmd
	case "t":
		cmd := m.testCurrent()
		return m, cmd
	case "T", "r":
		cmd := m.testAll()
		return m, cmd
	case "e":
		cmd := m.openEditChooser()
		return m, cmd
	case "esc", "q":
		m.action.Quit = true
		return m, nil
	}
	tbl, _ := m.table.Update(key)
	m.table = tbl
	return m, nil
}

func (m *Model) handleEditChooserKey(key tea.KeyPressMsg) (*Model, tea.Cmd) {
	switch key.String() {
	case "esc", "q":
		m.editing = false
		return m, nil
	case "j", "down":
		if len(m.editChoices) > 0 {
			m.editIdx = (m.editIdx + 1) % len(m.editChoices)
		}
		return m, nil
	case "k", "up":
		if len(m.editChoices) > 0 {
			m.editIdx = (m.editIdx - 1 + len(m.editChoices)) % len(m.editChoices)
		}
		return m, nil
	case "enter":
		if len(m.editChoices) == 0 {
			m.editing = false
			return m, nil
		}
		path := m.editChoices[m.editIdx].Path
		m.editing = false
		cmd := m.runEditor(path)
		return m, cmd
	}
	return m, nil
}

// openEditChooser is invoked on the `e` hotkey. With multiple targets it
// brings up the global/project chooser; with a single target it dispatches
// the editor immediately.
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

// connectCurrent handles `enter`: probe + connect.
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
		// no pinger, optimistic connect.
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
	editor := m.editor
	return func() tea.Msg {
		err := editor.Edit(path)
		return EditCompletedMsg{Path: path, Err: err}
	}
}

func (m *Model) handleEditCompleted(msg EditCompletedMsg) {
	if msg.Err != nil {
		m.toasts.Push(components.ToastError, "editor: "+msg.Err.Error())
		return
	}
	m.toasts.Push(components.ToastInfo, "saved "+msg.Path+" — reload pending")
}

// refreshTable rebuilds the underlying table rows from m.clusters.
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
	swatch := lipgloss.NewStyle().
		Foreground(m.styles.Palette.ClusterColor(c.Color)).
		Render("●")
	flags := []string{}
	if c.ReadOnly {
		flags = append(flags, "[RO]")
	}
	if c.Name == m.cliName {
		flags = append(flags, "(cli)")
	}
	return []string{
		swatch,
		c.Name,
		strings.Join(c.Brokers, ","),
		strings.Join(flags, " "),
		m.statuses[c.Name].Label(),
	}
}

// View renders the screen body. Width / height should reflect the area the
// screen is allowed to draw into.
func (m *Model) View() string {
	parts := []string{m.table.View()}
	if m.editing {
		parts = append(parts, m.renderEditChooser())
	}
	if t := m.toasts.View(); t != "" {
		parts = append(parts, t)
	}
	return strings.Join(parts, "\n")
}

// EditingChooser reports whether the global/project chooser modal is open
// (used by tests).
func (m *Model) EditingChooser() bool { return m.editing }

// EditChoices exposes the chooser entries (used by tests).
func (m *Model) EditChoices() []string {
	out := make([]string, 0, len(m.editChoices))
	for _, c := range m.editChoices {
		out = append(out, c.Label)
	}
	return out
}

// EditCursor returns the index of the currently-highlighted chooser entry.
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

// PingResultMsg is delivered when an asynchronous probe finishes.
type PingResultMsg struct {
	Name   string
	Err    error
	Intent pingIntent
}

// EditCompletedMsg is delivered when the editor process exits.
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
