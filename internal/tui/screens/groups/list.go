// Package groups implements the consumer-groups screen — list, detail view,
// and the 4-step reset-offsets flow (§7.6 — §7.8).
//
// The screen owns three sub-modes that share a single [Model]:
//
//   - ModeList shows the groups table (optionally filtered by topic).
//   - ModeDetail renders the focused group's members and per-partition
//     committed/end/lag rows.
//   - ModeReset hosts the 4-step reset flow (scope → strategy → params →
//     preview), with an express path that skips the preview.
//
// The screen owns no Kafka client itself: every read/admin call flows through
// a pluggable [Service] so tests can drive the model with a fake.
package groups

import (
	"context"
	"fmt"
	"image/color"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/help"
	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// Service abstracts the Kafka admin operations the groups screen needs.
type Service interface {
	ListConsumerGroups(ctx context.Context) ([]kafka.GroupListInfo, error)
	FilterGroupsByTopic(ctx context.Context, topic string) ([]kafka.GroupListInfo, error)
	DescribeConsumerGroup(ctx context.Context, group string) (kafka.GroupDescription, error)
	GroupOffsets(ctx context.Context, group string) ([]kafka.PartitionLag, error)
	PreviewReset(ctx context.Context, group string, spec kafka.ResetSpec) (kafka.ResetPreview, error)
	ResetOffsets(ctx context.Context, group string, spec kafka.ResetSpec) (kafka.ResetPreview, error)
	DeleteConsumerGroup(ctx context.Context, group string) error
}

// Action describes the screen's pending intent for the host (router).
type Action struct {
	// Back signals the user pressed esc/q on the list view.
	Back bool
	// Topic, when non-empty, requests navigation to the messages screen for
	// the named topic (raised by `t` in the detail view when the group has
	// exactly one subscribed topic).
	Topic string
	// TopicsForGroup, when non-empty, requests a topics list filtered to the
	// group's subscribed topics (raised by `t` in detail view with multiple).
	TopicsForGroup []string
}

// Mode is the current sub-mode.
type Mode int

const (
	// ModeList: groups list (default).
	ModeList Mode = iota
	// ModeDetail: members + per-partition lag for one group.
	ModeDetail
	// ModeReset: the 4-step reset-offsets flow.
	ModeReset
)

// Options configure a [Model].
type Options struct {
	// Service is the Kafka admin abstraction. Required.
	Service Service
	// ReadOnly disables R/shift+r/D and surfaces warnings.
	ReadOnly bool
	// FilterTopic, when non-empty, scopes the list to groups subscribed to
	// (or with commits for) that topic. The header changes accordingly.
	FilterTopic string
	// ListRefreshInterval, when > 0, drives auto-refresh of the list.
	ListRefreshInterval time.Duration
	// DetailRefreshInterval, when > 0, drives auto-refresh of the detail view.
	DetailRefreshInterval time.Duration
	// Now is the injected clock (defaults to time.Now).
	Now func() time.Time
	// Styles overrides the theme palette (mostly for tests).
	Styles theme.Styles
}

// Model is the consumer-groups screen.
type Model struct {
	svc      Service
	readOnly bool

	filterTopic string

	groups   []kafka.GroupListInfo
	totalLag map[string]int64 // group → cached aggregate lag (lazy)
	memberN  map[string]int   // group → cached member count

	table   *components.Table
	toasts  *components.Toasts
	confirm *components.Confirm
	pending pendingOp

	mode   Mode
	detail *DetailModel
	reset  *ResetModel

	listRefresher   components.Refresher
	detailRefresher components.Refresher

	width, height int
	loading       bool
	// manualRefresh is set when the user pressed `r` and is consumed by
	// handleGroupsLoaded to push a one-shot success toast (auto-refresh
	// ticks stay silent).
	manualRefresh bool

	action Action
	now    func() time.Time
	styles theme.Styles
}

// pendingOp tracks a destructive action awaiting confirmation. An empty
// group means no operation is pending; only the delete flow uses this
// today, so a single field is enough.
type pendingOp struct {
	group string
}

// New constructs a Model.
func New(opts Options) *Model {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	styles := opts.Styles
	if styles.Palette.Foreground == nil {
		styles = theme.DefaultStyles()
	}
	tbl := components.NewTable(listColumns(), components.WithStyles(styles))
	return &Model{
		svc:             opts.Service,
		readOnly:        opts.ReadOnly,
		filterTopic:     opts.FilterTopic,
		totalLag:        map[string]int64{},
		memberN:         map[string]int{},
		table:           tbl,
		toasts:          components.NewToasts(components.WithToastClock(now), components.WithToastStyles(styles)),
		listRefresher:   components.NewRefresher(opts.ListRefreshInterval, now),
		detailRefresher: components.NewRefresher(opts.DetailRefreshInterval, now),
		now:             now,
		styles:          styles,
	}
}

// listColumns returns the table column definitions for the groups list (§7.6).
// The total_lag column is non-sortable while it is partially populated; rows
// without lag fetched yet show "—".
func listColumns() []components.Column {
	return []components.Column{
		{Title: "Group", Flex: true, MinWidth: 24, Sortable: true},
		{Title: "State", Width: 12, Sortable: true},
		{Title: "Members", Width: 8, Sortable: true},
		{Title: "Total Lag", Width: 12, Sortable: true},
		{Title: "Coordinator", Width: 14, Sortable: true},
	}
}

// Init dispatches the initial groups load and schedules the first
// auto-refresh tick (when configured) — the recurring chain only sustains
// itself once started.
func (m *Model) Init() tea.Cmd {
	m.loading = true
	return tea.Batch(loadGroupsCmd(m.svc, m.filterTopic), m.AutoRefreshTick())
}

// Action returns the current pending action.
func (m *Model) Action() Action { return m.action }

// ConsumeAction returns the pending action and clears it.
func (m *Model) ConsumeAction() Action {
	a := m.action
	m.action = Action{}
	return a
}

// CurrentMode returns the current sub-mode (for tests).
func (m *Model) CurrentMode() Mode { return m.mode }

// WantsRawInput reports true while the reset flow is on the params step
// (timestamp / shift / specific offset), where the user is editing free-form
// text and shouldn't have keys captured by global shortcuts.
func (m *Model) WantsRawInput() bool {
	return m.mode == ModeReset && m.reset != nil && m.reset.Step() == StepParams
}

// Toasts exposes the toast queue (for tests).
func (m *Model) Toasts() *components.Toasts { return m.toasts }

// Title returns the frame title rendered by the host. In detail/reset modes
// the title reflects the sub-screen.
func (m *Model) Title() string {
	switch m.mode {
	case ModeDetail:
		if m.detail != nil {
			return "Group · " + m.detail.Group()
		}
	case ModeReset:
		if m.reset != nil {
			return "Reset offsets · " + m.reset.Group()
		}
	case ModeList:
		// fall through to the default list title below.
	}
	total := len(m.groups)
	body := fmt.Sprintf("Consumer Groups [%d]", total)
	if m.filterTopic != "" {
		body = fmt.Sprintf("Consumer Groups · %s [%d]", m.filterTopic, total)
	}
	if q := m.table.Search(); q != "" {
		prefix := "Consumer Groups"
		if m.filterTopic != "" {
			prefix = "Consumer Groups · " + m.filterTopic
		}
		body = fmt.Sprintf("%s [%d/%d] </%s>", prefix, m.table.FilteredCount(), total, q)
	}
	if m.loading {
		body += " (loading…)"
	}
	return body
}

// Breadcrumb returns the selected row identifier (or sub-screen state).
func (m *Model) Breadcrumb() string {
	if m.mode != ModeList {
		return ""
	}
	row, ok := m.table.SelectedRow()
	if !ok {
		return ""
	}
	return row.ID
}

// LatestFlash returns the freshest live toast from the active sub-mode's
// queue. In detail mode the host should see detail toasts; otherwise list
// toasts win.
func (m *Model) LatestFlash() (components.Toast, bool) {
	if m.mode == ModeDetail && m.detail != nil {
		if t, ok := m.detail.LatestFlash(); ok {
			return t, true
		}
	}
	if m.toasts == nil {
		return components.Toast{}, false
	}
	return m.toasts.Latest()
}

// Detail returns the active detail view (or nil) for tests.
func (m *Model) Detail() *DetailModel { return m.detail }

// Reset returns the active reset model (or nil) for tests.
func (m *Model) Reset() *ResetModel { return m.reset }

// Groups returns the loaded groups (defensive copy) for tests.
func (m *Model) Groups() []kafka.GroupListInfo {
	out := make([]kafka.GroupListInfo, len(m.groups))
	copy(out, m.groups)
	return out
}

// FilterTopic returns the active topic filter (empty string if unfiltered).
func (m *Model) FilterTopic() string { return m.filterTopic }

// ConfirmOpen reports whether a confirm dialog is currently visible (tests).
func (m *Model) ConfirmOpen() bool { return m.confirm != nil }

// PendingGroup returns the group currently awaiting confirmation (tests).
func (m *Model) PendingGroup() string { return m.pending.group }

// SetSearch forwards a host-driven filter query to the active sub-screen's
// table (list or detail).
func (m *Model) SetSearch(query string) {
	switch m.mode {
	case ModeDetail:
		if m.detail != nil {
			m.detail.SetSearch(query)
		}
	case ModeList, ModeReset:
		m.table.SetSearch(query)
	}
}

// ActiveFilter returns the search query active on the visible sub-screen.
func (m *Model) ActiveFilter() string {
	if m.mode == ModeDetail && m.detail != nil {
		return m.detail.ActiveFilter()
	}
	return m.table.Search()
}

// HasOverlay reports whether a modal (delete confirm, the multi-step
// reset-offsets flow, or the detail view) sits on top of the list. The
// host yields esc to the screen for any of these — without ModeDetail
// being included here the q/esc fallback would also pop the groups
// screen on detail close, so a single esc would skip the list view.
func (m *Model) HasOverlay() bool {
	return m.confirm != nil || m.mode == ModeReset || m.mode == ModeDetail
}

// SetSize updates width/height. Reserves chrome rows.
func (m *Model) SetSize(w, h int) {
	m.width, m.height = w, h
	if h > 0 {
		m.table.SetHeight(maxInt(1, h-7))
	}
	if w > 0 {
		m.table.SetTotalWidth(w)
	}
	if m.detail != nil {
		m.detail.SetSize(w, h)
	}
	if m.reset != nil {
		m.reset.SetSize(w, h)
	}
}

// KeyHints derives the bottom-row entries from the active mode's
// bindings table.
func (m *Model) KeyHints() []layout.KeyHint {
	return layout.HintsFromBindings(m.activeBindings())
}

// HelpSections derives the `?`-overlay sections from the same source
// as the dispatcher.
func (m *Model) HelpSections() []help.Section {
	return help.SectionsFromBindings(m.activeBindings())
}

func (m *Model) activeBindings() []keymap.Binding {
	switch m.mode {
	case ModeList:
		return m.listBindings()
	case ModeDetail:
		if m.detail != nil {
			return m.detail.bindings()
		}
	case ModeReset:
		if m.reset != nil {
			return m.reset.bindings()
		}
	}
	return m.listBindings()
}

// listBindings is the single source of truth for the groups list mode.
func (m *Model) listBindings() []keymap.Binding {
	bs := []keymap.Binding{
		{Keys: []string{"enter"}, Label: "open group detail", Category: "Group", Hint: true, Handler: m.openDetail},
		{Keys: []string{"r"}, Label: "refresh now", Category: "Group", Hint: true, Handler: m.actListRefresh},
		{Keys: []string{"esc", "q"}, Label: "back", Category: "Group", Handler: m.actListBack},
	}
	mut := []keymap.Binding{
		{Keys: []string{"R"}, Label: "reset offsets (full flow)", Category: "Mutating", Hint: true, Handler: func() tea.Cmd { return m.openReset(false) }},
		{Keys: []string{"shift+r"}, Label: "reset offsets (express)", Category: "Mutating", Hint: true, Handler: func() tea.Cmd { return m.openReset(true) }},
		{Keys: []string{"D"}, Label: "delete group", Category: "Mutating", Hint: true, Handler: m.openDeleteConfirm},
	}
	if m.readOnly {
		for i := range mut {
			mut[i].Category = ""
			mut[i].Hint = false
		}
	}
	bs = append(bs, mut...)
	bs = append(bs,
		keymap.Binding{Keys: []string{"/"}, Label: "filter rows", Category: "Group", Hint: true},
		keymap.Binding{Keys: []string{"ctrl+r"}, Label: "toggle auto-refresh", Category: "Group", Hint: true},
	)
	return bs
}

func (m *Model) actListRefresh() tea.Cmd {
	if m.loading {
		return nil
	}
	m.manualRefresh = true
	return m.refreshCmd()
}

func (m *Model) actListBack() tea.Cmd {
	m.action.Back = true
	return nil
}

// Update routes messages.
func (m *Model) Update(msg tea.Msg) tea.Cmd {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.SetSize(msg.Width, msg.Height)
		return nil
	case GroupsLoadedMsg:
		m.handleGroupsLoaded(msg)
		return nil
	case GroupLagsLoadedMsg:
		m.handleLagsLoaded(msg)
		return nil
	case DetailLoadedMsg:
		if m.detail != nil {
			m.detail.HandleLoaded(msg)
		}
		return nil
	case GroupDeletedMsg:
		m.handleDeleted(msg)
		cmd := m.refreshCmd()
		return cmd
	case ListRefreshTickMsg:
		cmd := m.handleListRefreshTick()
		return cmd
	case DetailRefreshTickMsg:
		if m.detail == nil || m.mode != ModeDetail {
			return nil
		}
		cmd := m.handleDetailRefreshTick()
		return cmd
	case ResetPreviewMsg, ResetCommittedMsg:
		if m.reset != nil {
			r, cmd := m.reset.Update(msg)
			m.reset = r
			a := r.ConsumeAction()
			cmd = m.handleResetAction(a, cmd)
			return cmd
		}
		return nil
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	}
	return nil
}

func (m *Model) handleKey(key tea.KeyPressMsg) tea.Cmd {
	switch m.mode {
	case ModeDetail:
		return m.handleDetailKey(key)
	case ModeReset:
		return m.handleResetKey(key)
	case ModeList:
		// fall through to list-mode handling below.
	}
	if m.confirm != nil {
		return m.handleConfirmKey(key)
	}
	if m.toasts != nil {
		_, _ = m.toasts.Update(key)
	}
	if m.table.SearchActive() {
		tbl, _ := m.table.Update(key)
		m.table = tbl
		return nil
	}
	return m.handleListKey(key)
}

func (m *Model) handleListKey(key tea.KeyPressMsg) tea.Cmd {
	if cmd, ok := keymap.Dispatch(m.listBindings(), key); ok {
		return cmd
	}
	tbl, _ := m.table.Update(key)
	m.table = tbl
	return nil
}

func (m *Model) openDetail() tea.Cmd {
	row, ok := m.table.SelectedRow()
	if !ok {
		return nil
	}
	d := NewDetailModel(DetailOptions{
		Service:  m.svc,
		Group:    row.ID,
		ReadOnly: m.readOnly,
		Now:      m.now,
		Styles:   m.styles,
	})
	d.SetSize(m.width, m.height)
	m.detail = d
	m.mode = ModeDetail
	// also kick off the detail-refresh tick chain — it only sustains
	// itself once started.
	return tea.Batch(d.Init(), m.DetailRefreshTick())
}

func (m *Model) handleDetailKey(key tea.KeyPressMsg) tea.Cmd {
	d, cmd := m.detail.Update(key)
	m.detail = d
	a := d.ConsumeAction()
	switch {
	case a.Back:
		m.detail = nil
		m.mode = ModeList
	case a.OpenReset:
		return m.openResetForGroup(d.Group(), false, ScopeDetail{Group: d.Group()})
	case a.OpenResetExpress:
		return m.openResetForGroup(d.Group(), true, ScopeDetail{Group: d.Group()})
	case a.Delete:
		m.pending = pendingOp{group: d.Group()}
		m.confirm = components.NewConfirm(
			"Delete consumer group",
			fmt.Sprintf("Delete group %q? This cannot be undone.", d.Group()),
			components.WithConfirmStyles(m.styles),
		)
		m.detail = nil
		m.mode = ModeList
	case a.Topic != "":
		m.action.Topic = a.Topic
	case len(a.TopicsForGroup) > 0:
		m.action.TopicsForGroup = a.TopicsForGroup
	}
	return cmd
}

func (m *Model) handleResetKey(key tea.KeyPressMsg) tea.Cmd {
	r, cmd := m.reset.Update(key)
	m.reset = r
	a := r.ConsumeAction()
	cmd = m.handleResetAction(a, cmd)
	return cmd
}

// handleResetAction interprets the reset model's pending action and returns
// the (possibly batched) tea.Cmd that the caller should dispatch.
func (m *Model) handleResetAction(a ResetAction, prev tea.Cmd) tea.Cmd {
	switch {
	case a.Cancel:
		m.reset = nil
		m.mode = ModeList
		return prev
	case a.Done:
		m.reset = nil
		m.mode = ModeList
		if a.Result != nil {
			m.toasts.Push(components.ToastSuccess, fmt.Sprintf(
				"Reset %s — %d partitions",
				a.Result.Strategy.String(),
				len(a.Result.Partitions),
			))
		}
		return tea.Batch(prev, m.refreshCmd())
	}
	return prev
}

func (m *Model) openReset(express bool) tea.Cmd {
	if m.readOnly {
		m.toasts.Push(components.ToastWarning, "cluster is read-only — reset blocked")
		return nil
	}
	row, ok := m.table.SelectedRow()
	if !ok {
		return nil
	}
	scope := ScopeWholeGroup{Group: row.ID}
	if m.filterTopic != "" {
		scope = ScopeWholeGroup{Group: row.ID, Topic: m.filterTopic}
	}
	return m.openResetForGroup(row.ID, express, scope)
}

func (m *Model) openResetForGroup(group string, express bool, scope ResetScope) tea.Cmd {
	if m.readOnly {
		m.toasts.Push(components.ToastWarning, "cluster is read-only — reset blocked")
		return nil
	}
	r := NewResetModel(ResetOptions{
		Service: m.svc,
		Group:   group,
		Scope:   scope,
		Express: express,
		Now:     m.now,
		Styles:  m.styles,
	})
	r.SetSize(m.width, m.height)
	m.reset = r
	m.mode = ModeReset
	return r.Init()
}

func (m *Model) openDeleteConfirm() tea.Cmd {
	if m.readOnly {
		m.toasts.Push(components.ToastWarning, "cluster is read-only — delete blocked")
		return nil
	}
	row, ok := m.table.SelectedRow()
	if !ok {
		return nil
	}
	m.pending = pendingOp{group: row.ID}
	m.confirm = components.NewConfirm(
		"Delete consumer group",
		fmt.Sprintf("Delete group %q? This cannot be undone.", row.ID),
		components.WithConfirmStyles(m.styles),
	)
	return nil
}

func (m *Model) handleConfirmKey(key tea.KeyPressMsg) tea.Cmd {
	c, _ := m.confirm.Update(key)
	m.confirm = c
	switch c.Result() {
	case components.ConfirmPending:
		return nil
	case components.ConfirmYes:
		op := m.pending
		m.confirm = nil
		m.pending = pendingOp{}
		if op.group != "" {
			return deleteCmd(m.svc, op.group)
		}
	case components.ConfirmNo:
		m.confirm = nil
		m.pending = pendingOp{}
	}
	return nil
}

func (m *Model) handleGroupsLoaded(msg GroupsLoadedMsg) {
	m.loading = false
	if msg.Err != nil {
		m.toasts.Push(components.ToastError, "load groups: "+msg.Err.Error())
		m.manualRefresh = false
		return
	}
	m.listRefresher.MarkSuccess()
	if m.manualRefresh {
		m.toasts.Push(components.ToastSuccess, fmt.Sprintf("refreshed · %d groups", len(msg.Groups)))
		m.manualRefresh = false
	}
	m.groups = msg.Groups
	m.refreshTable()
}

func (m *Model) handleLagsLoaded(msg GroupLagsLoadedMsg) {
	if msg.Err != nil {
		m.toasts.Push(components.ToastError, "lag for "+msg.Group+": "+msg.Err.Error())
		return
	}
	m.totalLag[msg.Group] = msg.TotalLag
	m.memberN[msg.Group] = msg.MemberCount
	m.refreshTable()
}

func (m *Model) handleDeleted(msg GroupDeletedMsg) {
	if msg.Err != nil {
		if kafka.IsNonEmptyGroup(msg.Err) {
			m.toasts.Push(components.ToastError, "cannot delete non-empty group "+msg.Group)
			return
		}
		m.toasts.Push(components.ToastError, "delete "+msg.Group+": "+msg.Err.Error())
		return
	}
	m.toasts.Push(components.ToastSuccess, "deleted group "+msg.Group)
}

func (m *Model) handleListRefreshTick() tea.Cmd {
	if m.mode != ModeList {
		return nil
	}
	next := m.AutoRefreshTick()
	if next == nil || m.listRefresher.Paused() || m.loading {
		return next
	}
	return tea.Batch(m.refreshCmd(), next)
}

func (m *Model) handleDetailRefreshTick() tea.Cmd {
	if m.mode != ModeDetail || m.detail == nil {
		return nil
	}
	next := m.DetailRefreshTick()
	if next == nil || m.detailRefresher.Paused() {
		return next
	}
	return tea.Batch(m.detail.RefreshCmd(), next)
}

// RefreshInterval returns the effective auto-refresh tick for the active
// sub-mode (list or detail). The host uses it to drive the chrome's
// Refresh: indicator and the ctrl+r toggle.
func (m *Model) RefreshInterval() time.Duration {
	if m.mode == ModeDetail {
		return m.detailRefresher.Interval()
	}
	return m.listRefresher.Interval()
}

// SetRefreshPaused toggles auto-refresh on both list and detail tickers
// without stopping them — flipping back to false resumes the regular
// cadence on whichever sub-mode is active.
func (m *Model) SetRefreshPaused(paused bool) {
	m.listRefresher.SetPaused(paused)
	m.detailRefresher.SetPaused(paused)
}

// LastRefresh returns the wall-clock time of the most recent successful
// load for the active sub-mode — detail when the user is inspecting one
// group, list otherwise. Mirrors RefreshInterval so the chrome's "auto Xs"
// and "Y ago" stay in sync with each other.
func (m *Model) LastRefresh() time.Time {
	if m.mode == ModeDetail && m.detail != nil {
		return m.detail.LastRefresh()
	}
	return m.listRefresher.LastRefresh()
}

// AutoRefreshTick returns a [tea.Cmd] that emits a tick for the list refresh
// interval. Hosts opt-in by calling this from Init.
func (m *Model) AutoRefreshTick() tea.Cmd { return m.listRefresher.Tick(ListRefreshTickMsg{}) }

// DetailRefreshTick returns a [tea.Cmd] that emits a tick for the detail-view
// refresh interval.
func (m *Model) DetailRefreshTick() tea.Cmd { return m.detailRefresher.Tick(DetailRefreshTickMsg{}) }

func (m *Model) refreshCmd() tea.Cmd {
	m.loading = true
	return loadGroupsCmd(m.svc, m.filterTopic)
}

// refreshTable rebuilds the underlying table rows from m.groups.
func (m *Model) refreshTable() {
	rows := make([]components.Row, 0, len(m.groups))
	for _, g := range m.groups {
		members := "—"
		if n, ok := m.memberN[g.Group]; ok {
			members = strconv.Itoa(n)
		}
		lag := "—"
		if l, ok := m.totalLag[g.Group]; ok {
			lag = formatThousands(l)
		}
		state := lipgloss.NewStyle().
			Foreground(groupStateColor(m.styles, g.State)).
			Render(g.State)
		rows = append(rows, components.Row{
			ID: g.Group,
			Values: []string{
				g.Group,
				state,
				members,
				lag,
				strconv.FormatInt(int64(g.Coordinator), 10),
			},
		})
	}
	m.table.SetRows(rows)
}

// groupStateColor maps a Kafka group state to a palette color so the State
// cell visually communicates health at a glance.
func groupStateColor(s theme.Styles, state string) color.Color {
	switch strings.ToLower(state) {
	case "stable":
		return s.Palette.StatusOK
	case "preparingrebalance", "completingrebalance":
		return s.Palette.StatusWarn
	case "dead":
		return s.Palette.StatusError
	case "empty":
		return s.Palette.Muted
	default:
		return s.Palette.Foreground
	}
}

// FetchLagsForVisible loads the cached lag for every currently visible group.
// The list screen calls this once after the initial groups load and on each
// refresh tick — lags are expensive so they're surfaced lazily and cached.
func (m *Model) FetchLagsForVisible() tea.Cmd {
	if len(m.groups) == 0 {
		return nil
	}
	cmds := make([]tea.Cmd, 0, len(m.groups))
	for _, g := range m.groups {
		cmds = append(cmds, loadLagCmd(m.svc, g.Group))
	}
	return tea.Batch(cmds...)
}

// View renders the screen body.
func (m *Model) View() string {
	switch m.mode {
	case ModeDetail:
		return m.detail.View()
	case ModeReset:
		return m.reset.View()
	case ModeList:
		// fall through to default list rendering below.
	}
	parts := []string{m.table.View()}
	if m.confirm != nil {
		parts = append(parts, m.confirm.View(m.width))
	}
	return strings.Join(parts, "\n")
}

// ----- Messages -----

// GroupsLoadedMsg replaces the current groups list with a fresh batch.
type GroupsLoadedMsg struct {
	Groups []kafka.GroupListInfo
	Err    error
}

// GroupLagsLoadedMsg surfaces the lazy-loaded total lag for a single group.
type GroupLagsLoadedMsg struct {
	Group       string
	TotalLag    int64
	MemberCount int
	Err         error
}

// GroupDeletedMsg reports the result of a delete-group operation.
type GroupDeletedMsg struct {
	Group string
	Err   error
}

// ListRefreshTickMsg is the periodic auto-refresh tick for the list view.
type ListRefreshTickMsg struct{}

// DetailRefreshTickMsg is the periodic auto-refresh tick for the detail view.
type DetailRefreshTickMsg struct{}

func loadGroupsCmd(svc Service, filterTopic string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var (
			groups []kafka.GroupListInfo
			err    error
		)
		if filterTopic != "" {
			groups, err = svc.FilterGroupsByTopic(ctx, filterTopic)
		} else {
			groups, err = svc.ListConsumerGroups(ctx)
		}
		return GroupsLoadedMsg{Groups: groups, Err: err}
	}
}

func loadLagCmd(svc Service, group string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		rows, err := svc.GroupOffsets(ctx, group)
		if err != nil {
			return GroupLagsLoadedMsg{Group: group, Err: err}
		}
		var total int64
		members := map[string]struct{}{}
		for _, r := range rows {
			if r.Lag > 0 {
				total += r.Lag
			}
			if r.MemberID != "" {
				members[r.MemberID] = struct{}{}
			}
		}
		return GroupLagsLoadedMsg{
			Group:       group,
			TotalLag:    total,
			MemberCount: len(members),
		}
	}
}

func deleteCmd(svc Service, group string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := svc.DeleteConsumerGroup(ctx, group)
		return GroupDeletedMsg{Group: group, Err: err}
	}
}

// formatThousands renders an integer with thousands separators (1,234,567).
func formatThousands(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := false
	if strings.HasPrefix(s, "-") {
		neg = true
		s = s[1:]
	}
	if len(s) <= 3 {
		if neg {
			return "-" + s
		}
		return s
	}
	var b strings.Builder
	pre := len(s) % 3
	if pre > 0 {
		b.WriteString(s[:pre])
		if len(s) > pre {
			b.WriteByte(',')
		}
	}
	for i := pre; i < len(s); i += 3 {
		b.WriteString(s[i : i+3])
		if i+3 < len(s) {
			b.WriteByte(',')
		}
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
