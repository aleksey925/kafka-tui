// Package groups implements the consumer-groups screen — list, detail view,
// and the 4-step reset-offsets flow.
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

// Persistence keys for the two cadences this screen owns — stable across
// releases (renaming would orphan previously-saved values).
const (
	refreshIntervalScreenListID   = "groups"
	refreshIntervalScreenDetailID = "group_detail"
)

// Seeds used when no persisted row exists for the corresponding screen ID.
// Detail polls faster because lag values move faster than group composition.
const (
	defaultListRefreshInterval   = 30 * time.Second
	defaultDetailRefreshInterval = 5 * time.Second
)

// refreshIntervalIOTimeout caps the synchronous load (construction) and save
// (post-pick) — a stalled disk would otherwise block the cmd loop.
const refreshIntervalIOTimeout = 500 * time.Millisecond

// Service abstracts the Kafka admin operations the groups screen needs.
type Service interface {
	ListConsumerGroups(ctx context.Context) ([]kafka.GroupListInfo, error)
	FilterGroupsByTopic(ctx context.Context, topic string) ([]kafka.GroupListInfo, error)
	DescribeConsumerGroup(ctx context.Context, group string) (kafka.GroupDescription, error)
	GroupOffsets(ctx context.Context, group string) ([]kafka.PartitionLag, error)
	// TopicsPartitions returns the full partition list for each requested
	// topic, fetched from cluster metadata. Used to expand topic-level
	// reset scope to every partition of the topic — not just the ones the
	// group already has commits for.
	TopicsPartitions(ctx context.Context, topics ...string) (map[string][]int32, error)
	PreviewReset(ctx context.Context, group string, spec kafka.ResetSpec) (kafka.ResetPreview, error)
	ResetOffsets(ctx context.Context, group string, spec kafka.ResetSpec) (kafka.ResetPreview, error)
	DeleteConsumerGroup(ctx context.Context, group string) error
}

// Action describes the screen's pending intent for the host (router).
type Action struct {
	Back  bool
	Topic string
}

type Mode int

const (
	ModeList Mode = iota
	ModeDetail
	ModeReset
)

type Options struct {
	Service     Service
	ReadOnly    bool
	FilterTopic string
	// RefreshIntervals persists each cadence across runs (keyed separately
	// for the list and detail views). nil disables persistence; both
	// refreshers start at their respective default constants.
	RefreshIntervals components.RefreshIntervalRepository
	Now              func() time.Time
	Styles           theme.Styles
}

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
	// resetOrigin remembers which mode was active before reset opened, so
	// cancel / done returns the user there instead of always falling back
	// to the list — opening reset from inside detail must restore detail.
	resetOrigin Mode

	listRefresher   components.Refresher
	detailRefresher components.Refresher

	refreshIntervals components.RefreshIntervalRepository
	refreshPicker    *components.RefreshPicker

	width, height int
	loading       bool
	// manualRefresh distinguishes user-initiated `r` from auto ticks so the
	// success toast only fires for the former.
	manualRefresh bool

	action Action
	now    func() time.Time
	styles theme.Styles
}

// pendingOp tracks a destructive action awaiting confirmation.
type pendingOp struct {
	group string
}

// emDash is the placeholder shown for an unloaded or unavailable cell.
const emDash = "—"

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

	listInterval := defaultListRefreshInterval
	detailInterval := defaultDetailRefreshInterval
	if opts.RefreshIntervals != nil {
		listInterval = loadPersistedInterval(opts.RefreshIntervals, refreshIntervalScreenListID, listInterval)
		detailInterval = loadPersistedInterval(opts.RefreshIntervals, refreshIntervalScreenDetailID, detailInterval)
	}

	return &Model{
		svc:              opts.Service,
		readOnly:         opts.ReadOnly,
		filterTopic:      opts.FilterTopic,
		totalLag:         map[string]int64{},
		memberN:          map[string]int{},
		table:            tbl,
		toasts:           components.NewToasts(components.WithToastClock(now), components.WithToastStyles(styles)),
		listRefresher:    components.NewRefresher(listInterval, now),
		detailRefresher:  components.NewRefresher(detailInterval, now),
		refreshIntervals: opts.RefreshIntervals,
		now:              now,
		styles:           styles,
	}
}

func loadPersistedInterval(repo components.RefreshIntervalRepository, screenID string, fallback time.Duration) time.Duration {
	ctx, cancel := context.WithTimeout(context.Background(), refreshIntervalIOTimeout)
	defer cancel()
	if d, ok, err := repo.LoadRefreshInterval(ctx, screenID); err == nil && ok {
		return d
	}
	return fallback
}

func listColumns() []components.Column {
	return []components.Column{
		{Title: "State", Width: 12, Sortable: true},
		{Title: "ID", Flex: true, MinWidth: 24, Sortable: true},
		{Title: "Coordinator", Width: 14, Sortable: true},
		{Title: "Protocol", Width: 12, Sortable: true},
		{Title: "Members", Width: 8, Sortable: true},
		{Title: "Total Lag", Width: 12, Sortable: true},
	}
}

// Init dispatches the initial groups load and schedules the first
// auto-refresh tick — the recurring chain only sustains itself once started.
func (m *Model) Init() tea.Cmd {
	m.loading = true
	return tea.Batch(loadGroupsCmd(m.svc, m.filterTopic), m.AutoRefreshTick())
}

func (m *Model) Action() Action { return m.action }

func (m *Model) ConsumeAction() Action {
	a := m.action
	m.action = Action{}
	return a
}

func (m *Model) CurrentMode() Mode { return m.mode }

// WantsRawInput is true on the reset params step where the user types
// free-form text.
func (m *Model) WantsRawInput() bool {
	return m.mode == ModeReset && m.reset != nil && m.reset.Step() == StepParams
}

func (m *Model) Toasts() *components.Toasts { return m.toasts }

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
	}
	prefix := "Consumer Groups"
	if m.filterTopic != "" {
		prefix += " · " + m.filterTopic
	}
	body := prefix + " " + layout.Counter(m.table.Search(), m.table.FilteredCount(), len(m.groups))
	if m.loading {
		body += " (loading…)"
	}
	return body
}

func (m *Model) Breadcrumb() string { return "" }

// LatestFlash returns the freshest live toast from the active sub-mode's queue.
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

func (m *Model) Detail() *DetailModel { return m.detail }

func (m *Model) Reset() *ResetModel { return m.reset }

func (m *Model) Groups() []kafka.GroupListInfo {
	out := make([]kafka.GroupListInfo, len(m.groups))
	copy(out, m.groups)
	return out
}

// CachedLag returns the cached (totalLag, memberCount) snapshot for group
// and whether either entry is present. Exposed so tests can assert that
// pruneLagCache actually drops entries for groups that left the listing.
func (m *Model) CachedLag(group string) (totalLag int64, memberCount int, ok bool) {
	lag, lagOK := m.totalLag[group]
	members, memOK := m.memberN[group]
	return lag, members, lagOK || memOK
}

func (m *Model) FilterTopic() string { return m.filterTopic }

func (m *Model) ConfirmOpen() bool { return m.confirm != nil }

func (m *Model) PendingGroup() string { return m.pending.group }

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

func (m *Model) ActiveFilter() string {
	if m.mode == ModeDetail && m.detail != nil {
		return m.detail.ActiveFilter()
	}
	return m.table.Search()
}

// HasOverlay must include ModeDetail or a single esc would pop both the
// detail view and the list, skipping the list entirely.
func (m *Model) HasOverlay() bool {
	return m.confirm != nil || m.refreshPicker != nil || m.mode == ModeReset || m.mode == ModeDetail
}

func (m *Model) SetSize(w, h int) {
	m.width, m.height = w, h
	if h > 0 {
		m.table.SetHeight(h)
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

func (m *Model) KeyHints() []layout.KeyHint {
	return layout.HintsFromBindings(m.activeBindings())
}

func (m *Model) HelpSections() []help.Section {
	return help.SectionsFromBindings(m.activeBindings())
}

func (m *Model) activeBindings() []keymap.Binding {
	if m.refreshPicker != nil {
		return m.refreshPicker.Bindings("Refresh interval")
	}
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

func (m *Model) listBindings() []keymap.Binding {
	bs := []keymap.Binding{
		{Keys: []string{"enter"}, Label: "open group detail", Category: "Group", Hint: true, Handler: m.openDetail},
		{Keys: []string{"r"}, Label: "refresh now", Category: "Group", Hint: true, Handler: m.actListRefresh},
		{Keys: []string{"esc", "q"}, Label: "back", Category: "Group", Handler: m.actListBack},
	}
	mut := []keymap.Binding{
		{Keys: []string{"R"}, Label: "reset group offsets", Category: "Mutating", Hint: true, Handler: m.openReset},
		{Keys: []string{"ctrl+d"}, Label: "delete group", Category: "Mutating", Hint: true, Handler: m.openDeleteConfirm},
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
		keymap.Binding{Keys: []string{"ctrl+r"}, Label: "set refresh interval", Category: "Group", Hint: true},
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

func (m *Model) Update(msg tea.Msg) tea.Cmd {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.SetSize(msg.Width, msg.Height)
		return nil
	case GroupsLoadedMsg:
		return m.handleGroupsLoaded(msg)
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
	case tea.PasteMsg:
		// the picker is the only sub-overlay on this screen that owns a
		// text buffer; route paste there when it's open.
		if m.refreshPicker != nil {
			m.refreshPicker, _ = m.refreshPicker.Update(msg)
		}
		return nil
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	}
	return nil
}

func (m *Model) handleKey(key tea.KeyPressMsg) tea.Cmd {
	// picker owns the foreground when open — every key feeds it.
	if m.refreshPicker != nil {
		return m.handlePickerKey(key)
	}
	switch m.mode {
	case ModeDetail:
		return m.handleDetailKey(key)
	case ModeReset:
		return m.handleResetKey(key)
	case ModeList:
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
	// kick off the detail-refresh tick chain — it only sustains itself once started.
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
		return m.openResetForGroup(d.Group(), d.ResetScope())
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

func (m *Model) handleResetAction(a ResetAction, prev tea.Cmd) tea.Cmd {
	switch {
	case a.Cancel:
		m.reset = nil
		m.mode = m.resetOrigin
		return prev
	case a.Done:
		m.reset = nil
		m.mode = m.resetOrigin
		if a.Result != nil {
			m.toasts.Push(components.ToastSuccess, fmt.Sprintf(
				"Reset %s — %d partitions",
				a.Result.Strategy.String(),
				len(a.Result.Partitions),
			))
		}
		return tea.Batch(prev, m.postResetRefresh())
	}
	return prev
}

// postResetRefresh re-fetches the data backing the screen the user
// returns to. Without this the detail view (or list) keeps showing the
// pre-reset offsets until the next manual `r` or auto-tick — which made
// it hard to confirm the commit visually.
func (m *Model) postResetRefresh() tea.Cmd {
	if m.mode == ModeDetail && m.detail != nil {
		return m.detail.RefreshCmd()
	}
	return m.refreshCmd()
}

func (m *Model) openReset() tea.Cmd {
	if m.readOnly {
		m.toasts.Push(components.ToastWarning, "cluster is read-only — reset blocked")
		return nil
	}
	row, ok := m.table.SelectedRow()
	if !ok {
		return nil
	}
	return m.openResetForGroup(row.ID, ScopeWholeGroup{Group: row.ID})
}

func (m *Model) openResetForGroup(group string, scope ResetScope) tea.Cmd {
	if m.readOnly {
		m.toasts.Push(components.ToastWarning, "cluster is read-only — reset blocked")
		return nil
	}
	r := NewResetModel(ResetOptions{
		Service: m.svc,
		Group:   group,
		Scope:   scope,
		Now:     m.now,
		Styles:  m.styles,
	})
	r.SetSize(m.width, m.height)
	m.resetOrigin = m.mode
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

func (m *Model) handleGroupsLoaded(msg GroupsLoadedMsg) tea.Cmd {
	m.loading = false
	if msg.Err != nil {
		m.toasts.Push(components.ToastError, "load groups: "+msg.Err.Error())
		m.manualRefresh = false
		return nil
	}
	m.listRefresher.MarkSuccess()
	if m.manualRefresh {
		m.toasts.Push(components.ToastSuccess, fmt.Sprintf("refreshed · %d groups", len(msg.Groups)))
		m.manualRefresh = false
	}
	m.groups = msg.Groups
	m.pruneLagCache()
	m.refreshTable()
	return m.FetchLagsForVisible()
}

// pruneLagCache drops cached lag/member counts for groups that are no
// longer in the listing — without this the table would keep flashing stale
// values for groups deleted between refreshes.
func (m *Model) pruneLagCache() {
	if len(m.totalLag) == 0 && len(m.memberN) == 0 {
		return
	}
	live := make(map[string]struct{}, len(m.groups))
	for _, g := range m.groups {
		live[g.Group] = struct{}{}
	}
	for k := range m.totalLag {
		if _, ok := live[k]; !ok {
			delete(m.totalLag, k)
		}
	}
	for k := range m.memberN {
		if _, ok := live[k]; !ok {
			delete(m.memberN, k)
		}
	}
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
	if next == nil || m.loading {
		return next
	}
	return tea.Batch(m.refreshCmd(), next)
}

func (m *Model) handleDetailRefreshTick() tea.Cmd {
	if m.mode != ModeDetail || m.detail == nil {
		return nil
	}
	next := m.DetailRefreshTick()
	if next == nil {
		return nil
	}
	return tea.Batch(m.detail.RefreshCmd(), next)
}

func (m *Model) RefreshInterval() time.Duration {
	if m.mode == ModeDetail {
		return m.detailRefresher.Interval()
	}
	return m.listRefresher.Interval()
}

func (m *Model) LastRefresh() time.Time {
	if m.mode == ModeDetail && m.detail != nil {
		return m.detail.LastRefresh()
	}
	return m.listRefresher.LastRefresh()
}

func (m *Model) AutoRefreshTick() tea.Cmd { return m.listRefresher.Tick(ListRefreshTickMsg{}) }

func (m *Model) DetailRefreshTick() tea.Cmd { return m.detailRefresher.Tick(DetailRefreshTickMsg{}) }

func (m *Model) refreshCmd() tea.Cmd {
	m.loading = true
	return loadGroupsCmd(m.svc, m.filterTopic)
}

func (m *Model) refreshTable() {
	rows := make([]components.Row, 0, len(m.groups))
	for _, g := range m.groups {
		members := emDash
		if n, ok := m.memberN[g.Group]; ok {
			members = strconv.Itoa(n)
		}
		lag := emDash
		if l, ok := m.totalLag[g.Group]; ok {
			lag = formatThousands(l)
		}
		state := lipgloss.NewStyle().
			Foreground(groupStateColor(m.styles, g.State)).
			Render(g.State)
		protocol := g.Protocol
		if protocol == "" {
			protocol = emDash
		}
		rows = append(rows, components.Row{
			ID: g.Group,
			Values: []string{
				state,
				g.Group,
				strconv.FormatInt(int64(g.Coordinator), 10),
				protocol,
				members,
				lag,
			},
		})
	}
	m.table.SetRows(rows)
}

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

// FetchLagsForVisible loads cached lag for every visible group; lags are
// expensive so they're surfaced lazily and cached.
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

func (m *Model) View() string {
	if m.refreshPicker != nil {
		return m.refreshPicker.View(m.width)
	}
	switch m.mode {
	case ModeDetail:
		return m.detail.View()
	case ModeReset:
		return m.reset.View()
	case ModeList:
	}
	if m.confirm != nil {
		return m.confirm.View(m.width, m.height)
	}
	return m.table.View()
}

// OpenRefreshPicker mounts the picker for whichever refresher is currently
// in the foreground: detail's cadence while we're in the detail sub-view,
// list's cadence otherwise. Implements [tui.RefreshConfigurable].
func (m *Model) OpenRefreshPicker() {
	current := m.currentRefresher().Interval()
	m.refreshPicker = components.NewRefreshPicker(
		current,
		components.WithRefreshPickerStyles(m.styles),
	)
}

// currentRefresher selects which cadence the picker should edit. Kept
// internal so callers don't accidentally rebind to the wrong one.
func (m *Model) currentRefresher() *components.Refresher {
	if m.mode == ModeDetail {
		return &m.detailRefresher
	}
	return &m.listRefresher
}

// currentRefreshScreenID is the persistence key matching [currentRefresher].
func (m *Model) currentRefreshScreenID() string {
	if m.mode == ModeDetail {
		return refreshIntervalScreenDetailID
	}
	return refreshIntervalScreenListID
}

// currentTickMsg is the tick type emitted by the active refresher's chain —
// needed to bootstrap the chain after a 0 → >0 transition.
func (m *Model) currentTickMsg() tea.Msg {
	if m.mode == ModeDetail {
		return DetailRefreshTickMsg{}
	}
	return ListRefreshTickMsg{}
}

func (m *Model) handlePickerKey(key tea.KeyPressMsg) tea.Cmd {
	m.refreshPicker, _ = m.refreshPicker.Update(key)
	if m.refreshPicker.Canceled() {
		m.refreshPicker = nil
		return nil
	}
	d, ok := m.refreshPicker.Selected()
	if !ok {
		return nil
	}
	r := m.currentRefresher()
	tickMsg := m.currentTickMsg()
	screenID := m.currentRefreshScreenID()
	m.refreshPicker = nil
	cmd := r.SetInterval(d, tickMsg)
	m.persistRefreshInterval(screenID, d)
	return cmd
}

func (m *Model) persistRefreshInterval(screenID string, d time.Duration) {
	if m.refreshIntervals == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), refreshIntervalIOTimeout)
	defer cancel()
	// the adapter logs the underlying SQLite error before wrapping; the
	// screen only adds the user-visible toast.
	if err := m.refreshIntervals.SaveRefreshInterval(ctx, screenID, d); err != nil {
		m.toasts.Push(components.ToastWarning, "couldn't persist refresh interval: "+err.Error())
	}
}

// ----- Messages -----

type GroupsLoadedMsg struct {
	Groups []kafka.GroupListInfo
	Err    error
}

type GroupLagsLoadedMsg struct {
	Group       string
	TotalLag    int64
	MemberCount int
	Err         error
}

type GroupDeletedMsg struct {
	Group string
	Err   error
}

type ListRefreshTickMsg struct{}

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
