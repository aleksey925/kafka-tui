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

	listInterval   time.Duration
	detailInterval time.Duration

	width, height int
	loading       bool

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
		svc:            opts.Service,
		readOnly:       opts.ReadOnly,
		filterTopic:    opts.FilterTopic,
		totalLag:       map[string]int64{},
		memberN:        map[string]int{},
		table:          tbl,
		toasts:         components.NewToasts(components.WithToastClock(now), components.WithToastStyles(styles)),
		listInterval:   opts.ListRefreshInterval,
		detailInterval: opts.DetailRefreshInterval,
		now:            now,
		styles:         styles,
	}
}

// listColumns returns the table column definitions for the groups list (§7.6).
// The total_lag column is non-sortable while it is partially populated; rows
// without lag fetched yet show "—".
func listColumns() []components.Column {
	return []components.Column{
		{Title: "Group", Flex: true, MinWidth: 24, Sortable: true},
		{Title: "State", Width: 12, Sortable: true},
		{Title: "Members", Width: 8, Align: lipgloss.Right, Sortable: true},
		{Title: "Total Lag", Width: 12, Align: lipgloss.Right, Sortable: true},
		{Title: "Coordinator", Width: 14, Sortable: true},
	}
}

// Init dispatches the initial groups load.
func (m *Model) Init() tea.Cmd {
	m.loading = true
	return loadGroupsCmd(m.svc, m.filterTopic)
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
	body := fmt.Sprintf("Consumer Groups [%d]", len(m.groups))
	if m.filterTopic != "" {
		body = fmt.Sprintf("Consumer Groups · %s [%d]", m.filterTopic, len(m.groups))
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

// KeyHints returns the screen-specific hints shown at the bottom row.
func (m *Model) KeyHints() []layout.KeyHint {
	switch m.mode {
	case ModeDetail:
		return m.detail.KeyHints()
	case ModeReset:
		return m.reset.KeyHints()
	case ModeList:
		// fall through to the default list hints below.
	}
	hints := []layout.KeyHint{
		{Key: "enter", Label: "detail"},
		{Key: "/", Label: "search"},
	}
	if !m.readOnly {
		hints = append(hints,
			layout.KeyHint{Key: "R", Label: "reset"},
			layout.KeyHint{Key: "shift+r", Label: "express"},
			layout.KeyHint{Key: "D", Label: "delete"},
		)
	}
	hints = append(hints, layout.KeyHint{Key: "r", Label: "reload"})
	return hints
}

// Update routes messages.
func (m *Model) Update(msg tea.Msg) (*Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.SetSize(msg.Width, msg.Height)
		return m, nil
	case GroupsLoadedMsg:
		m.handleGroupsLoaded(msg)
		return m, nil
	case GroupLagsLoadedMsg:
		m.handleLagsLoaded(msg)
		return m, nil
	case DetailLoadedMsg:
		if m.detail != nil {
			m.detail.HandleLoaded(msg)
		}
		return m, nil
	case GroupDeletedMsg:
		m.handleDeleted(msg)
		cmd := m.refreshCmd()
		return m, cmd
	case ListRefreshTickMsg:
		cmd := m.handleListRefreshTick()
		return m, cmd
	case DetailRefreshTickMsg:
		if m.detail == nil || m.mode != ModeDetail {
			return m, nil
		}
		cmd := m.handleDetailRefreshTick()
		return m, cmd
	case ResetPreviewMsg, ResetCommittedMsg:
		if m.reset != nil {
			r, cmd := m.reset.Update(msg)
			m.reset = r
			a := r.ConsumeAction()
			cmd = m.handleResetAction(a, cmd)
			return m, cmd
		}
		return m, nil
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

func (m *Model) handleKey(key tea.KeyPressMsg) (*Model, tea.Cmd) {
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
		return m, nil
	}
	return m.handleListKey(key)
}

func (m *Model) handleListKey(key tea.KeyPressMsg) (*Model, tea.Cmd) {
	switch key.String() {
	case "esc", "q":
		m.action.Back = true
		return m, nil
	case "enter":
		return m.openDetail()
	case "r":
		cmd := m.refreshCmd()
		return m, cmd
	case "R":
		return m.openReset(false)
	case "shift+r":
		return m.openReset(true)
	case "D":
		return m.openDeleteConfirm()
	}
	tbl, _ := m.table.Update(key)
	m.table = tbl
	return m, nil
}

func (m *Model) openDetail() (*Model, tea.Cmd) {
	row, ok := m.table.SelectedRow()
	if !ok {
		return m, nil
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
	return m, d.Init()
}

func (m *Model) handleDetailKey(key tea.KeyPressMsg) (*Model, tea.Cmd) {
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
	return m, cmd
}

func (m *Model) handleResetKey(key tea.KeyPressMsg) (*Model, tea.Cmd) {
	r, cmd := m.reset.Update(key)
	m.reset = r
	a := r.ConsumeAction()
	cmd = m.handleResetAction(a, cmd)
	return m, cmd
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

func (m *Model) openReset(express bool) (*Model, tea.Cmd) {
	if m.readOnly {
		m.toasts.Push(components.ToastWarning, "cluster is read-only — reset blocked")
		return m, nil
	}
	row, ok := m.table.SelectedRow()
	if !ok {
		return m, nil
	}
	scope := ScopeWholeGroup{Group: row.ID}
	if m.filterTopic != "" {
		scope = ScopeWholeGroup{Group: row.ID, Topic: m.filterTopic}
	}
	return m.openResetForGroup(row.ID, express, scope)
}

func (m *Model) openResetForGroup(group string, express bool, scope ResetScope) (*Model, tea.Cmd) {
	if m.readOnly {
		m.toasts.Push(components.ToastWarning, "cluster is read-only — reset blocked")
		return m, nil
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
	return m, r.Init()
}

func (m *Model) openDeleteConfirm() (*Model, tea.Cmd) {
	if m.readOnly {
		m.toasts.Push(components.ToastWarning, "cluster is read-only — delete blocked")
		return m, nil
	}
	row, ok := m.table.SelectedRow()
	if !ok {
		return m, nil
	}
	m.pending = pendingOp{group: row.ID}
	m.confirm = components.NewConfirm(
		"Delete consumer group",
		fmt.Sprintf("Delete group %q? This cannot be undone.", row.ID),
		components.WithConfirmStyles(m.styles),
	)
	return m, nil
}

func (m *Model) handleConfirmKey(key tea.KeyPressMsg) (*Model, tea.Cmd) {
	c, _ := m.confirm.Update(key)
	m.confirm = c
	switch c.Result() {
	case components.ConfirmPending:
		return m, nil
	case components.ConfirmYes:
		op := m.pending
		m.confirm = nil
		m.pending = pendingOp{}
		if op.group != "" {
			return m, deleteCmd(m.svc, op.group)
		}
	case components.ConfirmNo:
		m.confirm = nil
		m.pending = pendingOp{}
	}
	return m, nil
}

func (m *Model) handleGroupsLoaded(msg GroupsLoadedMsg) {
	m.loading = false
	if msg.Err != nil {
		m.toasts.Push(components.ToastError, "load groups: "+msg.Err.Error())
		return
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
	if m.listInterval <= 0 || m.mode != ModeList {
		return nil
	}
	return tea.Batch(m.refreshCmd(), m.AutoRefreshTick())
}

func (m *Model) handleDetailRefreshTick() tea.Cmd {
	if m.detailInterval <= 0 || m.mode != ModeDetail || m.detail == nil {
		return nil
	}
	return tea.Batch(m.detail.RefreshCmd(), m.DetailRefreshTick())
}

// AutoRefreshTick returns a [tea.Cmd] that emits a tick for the list refresh
// interval. Hosts opt-in by calling this from Init.
func (m *Model) AutoRefreshTick() tea.Cmd {
	if m.listInterval <= 0 {
		return nil
	}
	return tea.Tick(m.listInterval, func(time.Time) tea.Msg {
		return ListRefreshTickMsg{}
	})
}

// DetailRefreshTick returns a [tea.Cmd] that emits a tick for the detail-view
// refresh interval.
func (m *Model) DetailRefreshTick() tea.Cmd {
	if m.detailInterval <= 0 {
		return nil
	}
	return tea.Tick(m.detailInterval, func(time.Time) tea.Msg {
		return DetailRefreshTickMsg{}
	})
}

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
