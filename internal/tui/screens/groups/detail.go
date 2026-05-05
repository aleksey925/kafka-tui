package groups

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// SortMode controls the ordering of the partition table in the detail view.
type SortMode int

const (
	// SortGrouped sorts rows topic-first, partition-second, with a "┄┄┄"
	// separator between topics. This is the default §7.7 view.
	SortGrouped SortMode = iota
	// SortFlat sorts by lag descending, ignoring topic boundaries.
	SortFlat
)

// DetailAction is the host-facing intent of the detail view.
type DetailAction struct {
	// Back signals esc/q.
	Back bool
	// OpenReset asks the host to push the reset model with scope = whole detail.
	OpenReset bool
	// OpenResetExpress is OpenReset + skip preview.
	OpenResetExpress bool
	// Delete asks the host to confirm deleting the group.
	Delete bool
	// Topic, when non-empty, requests navigation to the messages screen for
	// the group's single topic (raised by `t` when the group has one topic).
	Topic string
	// TopicsForGroup, when non-empty, requests a topics list filtered to the
	// group's subscribed topics (raised by `t` when the group has multiple).
	TopicsForGroup []string
}

// DetailOptions configure a [DetailModel].
type DetailOptions struct {
	Service  Service
	Group    string
	ReadOnly bool
	Now      func() time.Time
	Styles   theme.Styles
}

// DetailModel renders members + per-partition lag for a single group.
type DetailModel struct {
	svc      Service
	group    string
	readOnly bool

	desc kafka.GroupDescription
	rows []kafka.PartitionLag

	sortMode SortMode
	table    *components.Table
	toasts   *components.Toasts

	width, height int
	loading       bool
	loadErr       string
	// manualRefresh is set when the user pressed `r` and is consumed by
	// HandleLoaded to push a one-shot success toast (auto ticks stay silent).
	manualRefresh bool
	// lastRefresh marks the wall-clock time of the most recent successful
	// detail load. Drives the chrome's "X ago" indicator while the user
	// is in detail mode.
	lastRefresh time.Time

	action DetailAction
	now    func() time.Time
	styles theme.Styles
}

// NewDetailModel constructs a fresh detail view.
func NewDetailModel(opts DetailOptions) *DetailModel {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	styles := opts.Styles
	if styles.Palette.Foreground == nil {
		styles = theme.DefaultStyles()
	}
	tbl := components.NewTable(detailColumns(), components.WithStyles(styles))
	return &DetailModel{
		svc:      opts.Service,
		group:    opts.Group,
		readOnly: opts.ReadOnly,
		table:    tbl,
		toasts:   components.NewToasts(components.WithToastClock(now), components.WithToastStyles(styles)),
		now:      now,
		styles:   styles,
	}
}

// detailColumns returns the column specs for the per-partition table.
func detailColumns() []components.Column {
	return []components.Column{
		{Title: "Topic", Width: 24, Sortable: true},
		{Title: "P", Width: 4, Sortable: true},
		{Title: "Committed", Width: 14, Sortable: true},
		{Title: "End", Width: 14, Sortable: true},
		{Title: "Lag", Width: 14, Sortable: true},
		{Title: "Member", Width: 24, Sortable: true},
	}
}

// Init dispatches the initial load.
func (d *DetailModel) Init() tea.Cmd {
	d.loading = true
	return loadDetailCmd(d.svc, d.group)
}

// RefreshCmd dispatches another refresh (used by the auto-refresh tick and
// the manual `r` press).
func (d *DetailModel) RefreshCmd() tea.Cmd {
	d.loading = true
	return loadDetailCmd(d.svc, d.group)
}

// Group returns the group name this detail view is bound to.
func (d *DetailModel) Group() string { return d.group }

// SortMode returns the current sort mode.
func (d *DetailModel) SortMode() SortMode { return d.sortMode }

// Description returns the loaded group description (defensive copy).
func (d *DetailModel) Description() kafka.GroupDescription { return d.desc }

// Rows returns the loaded partition rows (defensive copy).
func (d *DetailModel) Rows() []kafka.PartitionLag {
	out := make([]kafka.PartitionLag, len(d.rows))
	copy(out, d.rows)
	return out
}

// Toasts exposes the toast queue (for tests).
func (d *DetailModel) Toasts() *components.Toasts { return d.toasts }

// LatestFlash returns the freshest live toast from this submodel's queue.
func (d *DetailModel) LatestFlash() (components.Toast, bool) {
	if d.toasts == nil {
		return components.Toast{}, false
	}
	return d.toasts.Latest()
}

// Action returns the current pending action.
func (d *DetailModel) Action() DetailAction { return d.action }

// ConsumeAction returns the pending action and clears it.
func (d *DetailModel) ConsumeAction() DetailAction {
	a := d.action
	d.action = DetailAction{}
	return a
}

// SetSearch forwards a host-driven filter query to the partition table.
func (d *DetailModel) SetSearch(query string) { d.table.SetSearch(query) }

// ActiveFilter returns the partition table's current search query.
func (d *DetailModel) ActiveFilter() string { return d.table.Search() }

// SetSize updates width/height.
func (d *DetailModel) SetSize(w, h int) {
	d.width, d.height = w, h
	if h > 0 {
		// reserve rows for the header block and chrome.
		d.table.SetHeight(maxInt(1, h-headerLineCount-3))
	}
}

// headerLineCount is the number of header lines reserved by View() for the
// title block (group name, members, coordinator). Used to size the table.
const headerLineCount = 4

// KeyHints derives bottom-row entries from the bindings table.
func (d *DetailModel) KeyHints() []layout.KeyHint {
	return layout.HintsFromBindings(d.bindings())
}

// bindings is the single source of truth for group-detail shortcuts.
func (d *DetailModel) bindings() []keymap.Binding {
	bs := []keymap.Binding{
		{Keys: []string{"tab"}, Label: "toggle grouped / flat view", Category: "Group", Hint: true, Handler: d.actToggleSort},
		{Keys: []string{"t"}, Label: "jump to topics for this group", Category: "Group", Hint: true, Handler: d.actTopicJump},
		{Keys: []string{"r"}, Label: "refresh now", Category: "Group", Handler: d.actRefresh},
		{Keys: []string{"esc", "q"}, Label: "back", Category: "Group", Handler: d.actBack},
	}
	mut := []keymap.Binding{
		{Keys: []string{"R"}, Label: "reset offsets (full flow)", Category: "Mutating", Hint: true, Handler: d.actOpenReset(false)},
		{Keys: []string{"shift+r"}, Label: "reset offsets (express)", Category: "Mutating", Hint: true, Handler: d.actOpenReset(true)},
		{Keys: []string{"D"}, Label: "delete group", Category: "Mutating", Hint: true, Handler: d.actDelete},
	}
	if d.readOnly {
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

func (d *DetailModel) actToggleSort() tea.Cmd { d.toggleSort(); return nil }
func (d *DetailModel) actTopicJump() tea.Cmd  { d.handleTopicJump(); return nil }
func (d *DetailModel) actBack() tea.Cmd       { d.action.Back = true; return nil }

func (d *DetailModel) actOpenReset(express bool) func() tea.Cmd {
	return func() tea.Cmd {
		if d.readOnly {
			d.toasts.Push(components.ToastWarning, "cluster is read-only — reset blocked")
			return nil
		}
		if express {
			d.action.OpenResetExpress = true
		} else {
			d.action.OpenReset = true
		}
		return nil
	}
}

func (d *DetailModel) actDelete() tea.Cmd {
	if d.readOnly {
		d.toasts.Push(components.ToastWarning, "cluster is read-only — delete blocked")
		return nil
	}
	d.action.Delete = true
	return nil
}

func (d *DetailModel) actRefresh() tea.Cmd {
	if d.loading {
		return nil
	}
	d.manualRefresh = true
	return d.RefreshCmd()
}

// Update routes a message into the detail view.
func (d *DetailModel) Update(msg tea.Msg) (*DetailModel, tea.Cmd) {
	switch msg := msg.(type) {
	case DetailLoadedMsg:
		d.HandleLoaded(msg)
		return d, nil
	case tea.KeyPressMsg:
		return d.handleKey(msg)
	}
	return d, nil
}

func (d *DetailModel) handleKey(key tea.KeyPressMsg) (*DetailModel, tea.Cmd) {
	if d.toasts != nil {
		_, _ = d.toasts.Update(key)
	}
	if d.table.SearchActive() {
		tbl, _ := d.table.Update(key)
		d.table = tbl
		return d, nil
	}
	if cmd, ok := keymap.Dispatch(d.bindings(), key); ok {
		return d, cmd
	}
	tbl, _ := d.table.Update(key)
	d.table = tbl
	return d, nil
}

// handleTopicJump implements §7.7 `t`: jump to topics scoped to this group.
// One subscribed topic → straight to messages of that topic; multiple → topics
// list filtered by the group's topics.
func (d *DetailModel) handleTopicJump() {
	topics := d.subscribedTopics()
	switch len(topics) {
	case 0:
		d.toasts.Push(components.ToastInfo, "no topics for this group")
	case 1:
		d.action.Topic = topics[0]
	default:
		d.action.TopicsForGroup = topics
	}
}

// subscribedTopics returns the (sorted, deduplicated) list of topics this
// group has commits for or is currently subscribed to.
func (d *DetailModel) subscribedTopics() []string {
	seen := map[string]struct{}{}
	for _, r := range d.rows {
		seen[r.Topic] = struct{}{}
	}
	for _, m := range d.desc.Members {
		for _, t := range m.Topics {
			seen[t] = struct{}{}
		}
		for _, a := range m.Assignments {
			seen[a.Topic] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

func (d *DetailModel) toggleSort() {
	if d.sortMode == SortGrouped {
		d.sortMode = SortFlat
	} else {
		d.sortMode = SortGrouped
	}
	d.refreshTable()
}

// HandleLoaded merges fresh data into the detail view (also called by the
// list-screen router so DetailLoadedMsg can be dispatched from outside).
func (d *DetailModel) HandleLoaded(msg DetailLoadedMsg) {
	d.loading = false
	if msg.Err != nil {
		d.loadErr = msg.Err.Error()
		d.toasts.Push(components.ToastError, "load detail: "+msg.Err.Error())
		d.manualRefresh = false
		return
	}
	d.loadErr = ""
	d.lastRefresh = d.now()
	d.desc = msg.Description
	d.rows = msg.Rows
	d.refreshTable()
	if d.manualRefresh {
		d.toasts.Push(components.ToastSuccess, fmt.Sprintf(
			"refreshed · %d partitions", len(d.rows),
		))
		d.manualRefresh = false
	}
}

// LastRefresh returns the wall-clock time of the most recent successful
// detail load (zero before any load completes).
func (d *DetailModel) LastRefresh() time.Time { return d.lastRefresh }

// refreshTable rebuilds the partition rows according to the active sort mode.
func (d *DetailModel) refreshTable() {
	rows := append([]kafka.PartitionLag(nil), d.rows...)
	switch d.sortMode {
	case SortGrouped:
		sort.Slice(rows, func(i, j int) bool {
			if rows[i].Topic != rows[j].Topic {
				return rows[i].Topic < rows[j].Topic
			}
			return rows[i].Partition < rows[j].Partition
		})
	case SortFlat:
		sort.Slice(rows, func(i, j int) bool {
			if rows[i].Lag != rows[j].Lag {
				return rows[i].Lag > rows[j].Lag
			}
			if rows[i].Topic != rows[j].Topic {
				return rows[i].Topic < rows[j].Topic
			}
			return rows[i].Partition < rows[j].Partition
		})
	}

	tableRows := make([]components.Row, 0, len(rows))
	prevTopic := ""
	for _, r := range rows {
		if d.sortMode == SortGrouped && prevTopic != "" && prevTopic != r.Topic {
			tableRows = append(tableRows, components.Row{
				ID:     "sep-" + r.Topic,
				Values: []string{"┄┄┄", "", "", "", "", ""},
			})
		}
		tableRows = append(tableRows, components.Row{
			ID: rowID(r),
			Values: []string{
				r.Topic,
				strconv.FormatInt(int64(r.Partition), 10),
				offsetCell(r.Committed),
				offsetCell(r.End),
				lagCell(r.Lag),
				r.MemberID,
			},
		})
		prevTopic = r.Topic
	}
	d.table.SetRows(tableRows)
}

func rowID(r kafka.PartitionLag) string {
	return r.Topic + "/" + strconv.FormatInt(int64(r.Partition), 10)
}

// View renders the detail body.
func (d *DetailModel) View() string {
	parts := d.headerBlock()
	parts = append(parts, d.table.View())
	if d.loadErr != "" {
		parts = append(parts, d.styles.StatusErr.Render("error: "+d.loadErr))
	}
	return strings.Join(parts, "\n")
}

// headerBlock returns the §7.7 header lines: title, members, coordinator,
// sort-mode indicator. Always returns exactly headerLineCount lines so the
// layout can size the table reliably.
func (d *DetailModel) headerBlock() []string {
	title := d.styles.HelpTitle.Render("Group · " + d.group)
	state := d.desc.State
	if state == "" {
		state = "?"
	}
	statusLine := d.styles.StatusInfo.Render(fmt.Sprintf(
		"state: %s   protocol: %s",
		state, valueOr(d.desc.Protocol, "—"),
	))
	membersLine := d.styles.StatusInfo.Render(d.formatMembersLine())
	coordLine := d.styles.StatusInfo.Render(fmt.Sprintf(
		"coordinator: %d %s   sort: %s",
		d.desc.CoordinatorID,
		d.coordHostPort(),
		d.sortLabel(),
	))
	return []string{title, statusLine, membersLine, coordLine}
}

func (d *DetailModel) sortLabel() string {
	if d.sortMode == SortFlat {
		return "flat (lag desc)"
	}
	return "grouped"
}

func (d *DetailModel) coordHostPort() string {
	if d.desc.CoordinatorHost == "" {
		return ""
	}
	return fmt.Sprintf("(%s:%d)", d.desc.CoordinatorHost, d.desc.CoordinatorPort)
}

func valueOr(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

// formatMembersLine renders the §7.7 members preview, truncating to fit width
// with `+N more`.
func (d *DetailModel) formatMembersLine() string {
	if len(d.desc.Members) == 0 {
		return "members: (none)"
	}
	width := d.width
	if width <= 0 {
		width = 80
	}
	prefix := fmt.Sprintf("members (%d): ", len(d.desc.Members))
	avail := max(width-len(prefix), 10)
	names := make([]string, 0, len(d.desc.Members))
	for _, m := range d.desc.Members {
		names = append(names, memberLabel(m))
	}
	return prefix + truncateMembers(names, avail)
}

// truncateMembers joins names with ", " and replaces any tail that would
// overflow with "+N more".
func truncateMembers(names []string, width int) string {
	if len(names) == 0 {
		return ""
	}
	var b strings.Builder
	for i, n := range names {
		piece := n
		if i > 0 {
			piece = ", " + n
		}
		more := len(names) - i - 1
		moreSuffix := ""
		if more > 0 {
			moreSuffix = ", +" + strconv.Itoa(more) + " more"
		}
		if b.Len()+len(piece)+len(moreSuffix) > width {
			if b.Len() == 0 {
				// even a single name overflows — render it raw.
				b.WriteString(n)
				if i < len(names)-1 {
					b.WriteString(", +")
					b.WriteString(strconv.Itoa(len(names) - i - 1))
					b.WriteString(" more")
				}
				return b.String()
			}
			b.WriteString(", +")
			b.WriteString(strconv.Itoa(len(names) - i))
			b.WriteString(" more")
			return b.String()
		}
		b.WriteString(piece)
	}
	return b.String()
}

func memberLabel(m kafka.GroupMember) string {
	if m.InstanceID != "" {
		return m.InstanceID
	}
	return m.MemberID
}

func offsetCell(v int64) string {
	if v < 0 {
		return "—"
	}
	return formatThousands(v)
}

func lagCell(v int64) string {
	if v < 0 {
		return "—"
	}
	return formatThousands(v)
}

// ----- Messages -----

// DetailLoadedMsg surfaces the (description, partition lags) snapshot for the
// detail view. Dispatched both by Init and by the auto-refresh tick.
type DetailLoadedMsg struct {
	Description kafka.GroupDescription
	Rows        []kafka.PartitionLag
	Err         error
}

func loadDetailCmd(svc Service, group string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		desc, err := svc.DescribeConsumerGroup(ctx, group)
		if err != nil {
			return DetailLoadedMsg{Err: err}
		}
		rows, err := svc.GroupOffsets(ctx, group)
		if err != nil {
			return DetailLoadedMsg{Description: desc, Err: err}
		}
		return DetailLoadedMsg{Description: desc, Rows: rows}
	}
}
