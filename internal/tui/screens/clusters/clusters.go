// Package clusters implements the cluster-list screen.
package clusters

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/config"
	"github.com/aleksey925/kafka-tui/internal/kafka"
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
	// StatusInvalid marks a cluster whose YAML failed to load (unresolved
	// placeholder, vault lookup error, TLS conflict, etc.). The row stays
	// in the picker so the user sees what's missing, but no Ping/Connect
	// is attempted — both surface the load reason instead.
	StatusInvalid
)

func (s ConnectionStatus) Icon() string {
	switch s {
	case StatusChecking:
		return "◐"
	case StatusOK:
		return "✓"
	case StatusFailed:
		return "✗"
	case StatusInvalid:
		return "!"
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
	case StatusInvalid:
		return "! invalid"
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
	Clusters []config.Cluster
	// InvalidClusters are clusters whose YAML failed to load. They show up
	// in the picker with StatusInvalid and a load-time error toast on
	// select, but are never dialed.
	InvalidClusters []config.InvalidCluster
	// CLIName marks the inline cluster from --brokers (for the "(cli)"
	// badge in the picker). Auto-select uses AutoSelectCluster, not this.
	CLIName string
	// AutoSelectCluster names the cluster to auto-connect to at startup.
	// SkipTarget honors it when the name matches a valid cluster;
	// otherwise it falls through to the single-cluster shortcut or shows
	// the picker.
	AutoSelectCluster       string
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
	// invalidNames tracks which rows in the table came from
	// InvalidClusters, so connect/test can short-circuit without re-walking
	// the original slice and the row renderer can pull the load reason
	// from errors[name]. Set membership is the only thing that matters.
	invalidNames map[string]struct{}
	cliName      string
	// autoSelect carries the --cluster / inline-fallback target. The
	// host (app.Init) calls SkipTarget once at startup; on miss it
	// writes pendingStartupToast and the screen's own Init drains it.
	autoSelect          string
	pendingStartupToast string

	statuses map[string]ConnectionStatus
	errors   map[string]string
	// pingGen per cluster bumps on every ping dispatch. Late results from a
	// superseded ping (user re-pings the same cluster mid-flight) are
	// dropped — critical for the connect intent, where applying a stale
	// "OK" would auto-connect to the wrong cluster after a re-press.
	pingGen map[string]uint64

	table  *components.Table
	toasts *components.Toasts

	pinger      Pinger
	editor      Editor
	pingTimeout time.Duration

	editChoices []editTarget
	editMenu    *components.Menu

	action     Action
	stagedInit bool

	width, height int

	// connectivity probes are user-driven; the refresher only stamps
	// LastRefresh() for the chrome's "X ago" indicator on config-snapshot
	// arrival.
	refresher components.Refresher

	startupWarn []string
	// lastWarnings remembers the soft-fallback warnings already surfaced
	// (initially the startupWarn batch). SetClusters diffs against it so
	// the same persistent warning isn't re-toasted on every watcher tick.
	lastWarnings []string
	now          func() time.Time
	styles       theme.Styles
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

	// Merge valid + invalid into one ordered list; invalid keeps its
	// position from clusters.yaml so the user sees rows in their file
	// order, not segregated by status.
	merged := make([]config.Cluster, 0, len(opts.Clusters)+len(opts.InvalidClusters))
	merged = append(merged, opts.Clusters...)
	for _, ic := range opts.InvalidClusters {
		merged = append(merged, ic.Cluster)
	}
	invalidNames := make(map[string]struct{}, len(opts.InvalidClusters))
	errors := make(map[string]string, len(opts.InvalidClusters))
	for _, ic := range opts.InvalidClusters {
		invalidNames[ic.Cluster.Name] = struct{}{}
		if ic.Reason != nil {
			errors[ic.Cluster.Name] = ic.Reason.Error()
		}
	}
	statuses := make(map[string]ConnectionStatus, len(merged))
	for _, c := range merged {
		if _, bad := invalidNames[c.Name]; bad {
			statuses[c.Name] = StatusInvalid
		} else {
			statuses[c.Name] = StatusUnknown
		}
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
		clusters:     merged,
		invalidNames: invalidNames,
		cliName:      opts.CLIName,
		autoSelect:   opts.AutoSelectCluster,
		statuses:     statuses,
		errors:       errors,
		pingGen:      make(map[string]uint64, len(merged)),
		table:        tbl,
		toasts:       components.NewToasts(components.WithToastClock(now), components.WithToastStyles(styles)),
		pinger:       opts.Pinger,
		editor:       editor,
		pingTimeout:  timeout,
		editChoices:  choices,
		refresher:    refresher,
		startupWarn:  append([]string(nil), opts.StartupWarnings...),
		lastWarnings: append([]string(nil), opts.StartupWarnings...),
		now:          now,
		styles:       styles,
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
		// widened 12 → 16 so the [NO-TLS-VERIFY] marker fits.
		{Title: "Notes", Width: 16, Sortable: false},
		{Title: "Status", Width: 14, Sortable: false},
	}
}

// SkipTarget reports the cluster the host should auto-connect to,
// bypassing the picker. Priority: explicit autoSelect (from --cluster,
// or from --brokers inline as a fallback) → the only cluster in the
// list. Invalid or unknown autoSelect targets do NOT auto-skip; the
// user lands on the picker.
//
// Side effect: on an autoSelect miss the reason is stashed in
// pendingStartupToast for the screen's Init to surface as a toast —
// callers don't need to handle the miss explicitly.
func (m *Model) SkipTarget() (string, bool) {
	if m.autoSelect != "" {
		if _, bad := m.invalidNames[m.autoSelect]; bad {
			m.pendingStartupToast = "cluster " + m.autoSelect + " is invalid; see picker"
			return "", false
		}
		if _, exists := m.statuses[m.autoSelect]; !exists {
			m.pendingStartupToast = "cluster " + m.autoSelect + " not found"
			return "", false
		}
		return m.autoSelect, true
	}
	if len(m.clusters) == 1 {
		only := m.clusters[0].Name
		if _, bad := m.invalidNames[only]; bad {
			return "", false
		}
		return only, true
	}
	return "", false
}

func (m *Model) Init() tea.Cmd {
	for _, w := range m.startupWarn {
		m.toasts.PushWithLifetime(components.ToastWarning, w, 5*time.Second)
	}
	m.startupWarn = nil
	if m.pendingStartupToast != "" {
		m.toasts.PushWithLifetime(components.ToastWarning, m.pendingStartupToast, 5*time.Second)
		m.pendingStartupToast = ""
	}
	if n := len(m.invalidNames); n > 0 {
		m.toasts.PushWithLifetime(
			components.ToastWarning,
			fmt.Sprintf("%d cluster(s) failed to load — see status column for reason", n),
			5*time.Second,
		)
	}
	return nil
}

func (m *Model) RefreshInterval() time.Duration { return m.refresher.Interval() }

func (m *Model) Action() Action { return m.action }

func (m *Model) ConsumeAction() Action {
	a := m.action
	m.action = Action{}
	return a
}

func (m *Model) Status(name string) ConnectionStatus { return m.statuses[name] }

// SetClusters replaces the cluster list (host calls this after a reload).
// Valid statuses (ok / failed / unknown) are preserved by name; clusters
// that became invalid in this reload move to StatusInvalid with the new
// reason; missing clusters drop out; the cursor stays on the same name
// when possible. Soft-fallback warnings new to this reload are toasted;
// persistent ones from prior reloads are not re-toasted.
func (m *Model) SetClusters(list []config.Cluster, invalid []config.InvalidCluster, warnings []string, cliName string) {
	merged := make([]config.Cluster, 0, len(list)+len(invalid))
	merged = append(merged, list...)
	for _, ic := range invalid {
		merged = append(merged, ic.Cluster)
	}
	invalidNames := make(map[string]struct{}, len(invalid))
	keepErr := make(map[string]string, len(merged))
	for _, ic := range invalid {
		invalidNames[ic.Cluster.Name] = struct{}{}
		if ic.Reason != nil {
			keepErr[ic.Cluster.Name] = ic.Reason.Error()
		}
	}
	keep := make(map[string]ConnectionStatus, len(merged))
	for _, c := range merged {
		if _, bad := invalidNames[c.Name]; bad {
			keep[c.Name] = StatusInvalid
			continue
		}
		prev, seen := m.statuses[c.Name]
		if !seen || prev == StatusInvalid {
			// freshly valid (or never seen) — start at unknown so the
			// stale load reason from a prior reload doesn't linger
			// after the user fixed the YAML.
			keep[c.Name] = StatusUnknown
			continue
		}
		keep[c.Name] = prev
		if e, ok := m.errors[c.Name]; ok {
			keepErr[c.Name] = e
		}
	}
	newlyInvalid := newlyInvalidNames(m.invalidNames, invalidNames)
	newWarnings := newWarningsSince(m.lastWarnings, warnings)
	// invalidate every pending ping after a reload: a stale PingResult that
	// arrives now could otherwise resurrect a removed cluster (cluster
	// dropped from YAML), overwrite a freshly-invalid status with a phantom
	// "OK" (valid→invalid transition keeps the name in keep, so per-name
	// deletion wouldn't fire), or fire an Intent=connect for a cluster
	// whose underlying config just changed. A fresh ping post-reload always
	// re-bumps the counter from zero.
	m.pingGen = make(map[string]uint64, len(keep))
	m.clusters = merged
	m.invalidNames = invalidNames
	m.cliName = cliName
	m.statuses = keep
	m.errors = keepErr
	m.lastWarnings = append([]string(nil), warnings...)
	m.refresher.MarkSuccess()
	prevID := ""
	if row, ok := m.table.SelectedRow(); ok {
		prevID = row.ID
	}
	m.refreshTable()
	if prevID != "" {
		m.table.GoToID(prevID)
	}
	for _, w := range newWarnings {
		m.toasts.PushWithLifetime(components.ToastWarning, w, 5*time.Second)
	}
	if len(newlyInvalid) > 0 {
		m.toasts.PushWithLifetime(
			components.ToastWarning,
			fmt.Sprintf("%d cluster(s) now invalid: %s",
				len(newlyInvalid), strings.Join(newlyInvalid, ", ")),
			5*time.Second,
		)
	}
}

// newWarningsSince returns warnings present in next but not in prev. The
// same persistent warning across reloads is filtered out — re-toasting
// "clipboard.method: invalid value 'xclip'" every fsnotify tick would
// just be noise.
func newWarningsSince(prev, next []string) []string {
	if len(next) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(prev))
	for _, w := range prev {
		seen[w] = struct{}{}
	}
	out := make([]string, 0, len(next))
	for _, w := range next {
		if _, was := seen[w]; was {
			continue
		}
		out = append(out, w)
	}
	return out
}

// newlyInvalidNames returns the sorted list of names that are invalid in
// next but were not invalid in prev — i.e. clusters that broke since the
// last load. Surfacing only the delta keeps re-toasting noiseless when an
// already-broken cluster persists across reloads.
func newlyInvalidNames(prev, next map[string]struct{}) []string {
	if len(next) == 0 {
		return nil
	}
	out := make([]string, 0, len(next))
	for name := range next {
		if _, was := prev[name]; !was {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func (m *Model) Toasts() *components.Toasts { return m.toasts }

func (m *Model) LatestFlash() (components.Toast, bool) {
	if m.toasts == nil {
		return components.Toast{}, false
	}
	return m.toasts.Latest()
}

func (m *Model) Title() string {
	return "Clusters " + layout.Counter(m.table.Search(), m.table.FilteredCount(), len(m.clusters))
}

func (m *Model) Breadcrumb() string { return "" }

func (m *Model) SetSearch(query string) { m.table.SetSearch(query) }

func (m *Model) ActiveFilter() string { return m.table.Search() }

func (m *Model) HasOverlay() bool { return m.editMenu != nil }

func (m *Model) SetSize(w, h int) {
	m.width, m.height = w, h
	if h > 0 {
		m.table.SetHeight(h)
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
	if m.editMenu != nil {
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
	if m.editMenu != nil {
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
	menu, _ := m.editMenu.Update(key)
	m.editMenu = menu
	if menu.Canceled() {
		m.editMenu = nil
		return nil
	}
	idx, _, ok := menu.Selected()
	if !ok {
		return nil
	}
	m.editMenu = nil
	return m.runEditor(m.editChoices[idx].Path)
}

func (m *Model) editChooserBindings() []keymap.Binding {
	if m.editMenu == nil {
		return nil
	}
	return m.editMenu.Bindings("Edit chooser")
}

func (m *Model) openEditChooser() tea.Cmd {
	if len(m.editChoices) == 0 {
		m.toasts.Push(components.ToastWarning, "no clusters.yaml location is configured")
		return nil
	}
	if len(m.editChoices) == 1 {
		return m.runEditor(m.editChoices[0].Path)
	}
	items := make([]components.MenuItem, 0, len(m.editChoices))
	for _, c := range m.editChoices {
		items = append(items, components.MenuItem{Label: c.Label, Hint: c.Path})
	}
	m.editMenu = components.NewMenu(items,
		components.WithMenuTitle("Edit clusters.yaml"),
		components.WithMenuStyles(m.styles),
	)
	return nil
}

// connectCurrent emits a connect request for the selected cluster. The host
// owns the connectivity gate and reflects the outcome back via
// [Model.SetConnectionStatus]; the picker does not probe on connect here.
func (m *Model) connectCurrent() tea.Cmd {
	row, ok := m.table.SelectedRow()
	if !ok {
		return nil
	}
	name := row.ID
	if m.flashInvalidReason(name) {
		return nil
	}
	m.action.Connect = name
	return nil
}

// SetConnectionStatus lets the host reflect the outcome of a host-driven
// connect attempt on a cluster's row (StatusChecking while dialing,
// StatusFailed on a failed gate). Invalid clusters keep their StatusInvalid
// marker — the host never connects them.
func (m *Model) SetConnectionStatus(name string, status ConnectionStatus) {
	if m.statuses[name] == StatusInvalid {
		return
	}
	m.statuses[name] = status
	m.refreshTable()
}

// ClearConnecting resets a row the host left in StatusChecking for a connect
// it then abandoned (superseded by a newer connect, whose result will never
// arrive for this row). Only a still-checking row is touched, so an OK/failed
// status the user re-probed with `t` in the meantime survives.
func (m *Model) ClearConnecting(name string) {
	if m.statuses[name] != StatusChecking {
		return
	}
	m.statuses[name] = StatusUnknown
	m.refreshTable()
}

func (m *Model) testCurrent() tea.Cmd {
	row, ok := m.table.SelectedRow()
	if !ok {
		return nil
	}
	name := row.ID
	if m.flashInvalidReason(name) {
		return nil
	}
	c := m.findCluster(name)
	if c == nil || m.pinger == nil {
		return nil
	}
	m.statuses[name] = StatusChecking
	m.pingGen[name]++
	m.refreshTable()
	return pingCmd(m.pinger, *c, m.pingTimeout, m.pingGen[name])
}

func (m *Model) testAll() tea.Cmd {
	if m.pinger == nil {
		return nil
	}
	cmds := make([]tea.Cmd, 0, len(m.clusters))
	for _, c := range m.clusters {
		if _, bad := m.invalidNames[c.Name]; bad {
			// skip silently — the invalid row already carries its reason
			// and re-toasting one warning per invalid cluster on every
			// "T" would just be noise.
			continue
		}
		m.statuses[c.Name] = StatusChecking
		m.pingGen[c.Name]++
		cmds = append(cmds, pingCmd(m.pinger, c, m.pingTimeout, m.pingGen[c.Name]))
	}
	m.refreshTable()
	if len(cmds) == 0 {
		return nil
	}
	return tea.Batch(cmds...)
}

// flashInvalidReason returns true (and pushes a toast) when the named
// cluster failed to load. Connect/test handlers short-circuit on it.
func (m *Model) flashInvalidReason(name string) bool {
	if _, bad := m.invalidNames[name]; !bad {
		return false
	}
	msg := "cluster " + name + ": invalid configuration"
	if reason, ok := m.errors[name]; ok && reason != "" {
		msg = "cluster " + name + ": " + reason
	}
	m.toasts.Push(components.ToastError, msg)
	return true
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
	if msg.Gen != m.pingGen[msg.Name] {
		return
	}
	if msg.Err != nil {
		m.statuses[msg.Name] = StatusFailed
		m.errors[msg.Name] = msg.Err.Error()
		m.toasts.Push(components.ToastError, fmt.Sprintf("%s: %s", msg.Name, msg.Err.Error()))
	} else {
		m.statuses[msg.Name] = StatusOK
		delete(m.errors, msg.Name)
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
	if kafka.IsInsecureTLS(c) {
		// [NO-TLS-VERIFY] mirrors the [RO] convention (capitalized,
		// braces-wrapped) and names the exact thing that's off — matches
		// the YAML field skip_verify and the --tls-skip-verify flag.
		flags = append(flags, m.styles.StatusWarn.Render("[NO-TLS-VERIFY]"))
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
	if m.editMenu != nil {
		body := m.table.View()
		popup := lipgloss.Place(m.width, lipgloss.Height(body), lipgloss.Center, lipgloss.Center, m.editMenu.View(0))
		return popup
	}
	return m.table.View()
}

func (m *Model) EditingChooser() bool { return m.editMenu != nil }

func (m *Model) EditChoices() []string {
	out := make([]string, 0, len(m.editChoices))
	for _, c := range m.editChoices {
		out = append(out, c.Label)
	}
	return out
}

func (m *Model) EditCursor() int {
	if m.editMenu == nil {
		return 0
	}
	return m.editMenu.Cursor()
}

// ----- Messages -----

type PingResultMsg struct {
	Name string
	Err  error
	// Gen pins the result to the [Model.pingGen] for Name at dispatch time
	// so handlers drop stale arrivals from a re-issued ping.
	Gen uint64
}

type EditCompletedMsg struct {
	Path string
	Err  error
}

func pingCmd(p Pinger, c config.Cluster, timeout time.Duration, gen uint64) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		err := p.Ping(ctx, c)
		return PingResultMsg{Name: c.Name, Err: err, Gen: gen}
	}
}
