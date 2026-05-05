package groups

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

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
	// separator between topics.
	SortGrouped SortMode = iota
	// SortFlat sorts by lag descending, ignoring topic boundaries.
	SortFlat
)

// DetailAction is the host-facing intent of the detail view.
type DetailAction struct {
	Back             bool
	OpenReset        bool
	OpenResetExpress bool
	Delete           bool
	// Topic / TopicsForGroup request navigation from `t`: single subscribed
	// topic vs filtered topics list.
	Topic          string
	TopicsForGroup []string
}

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

	loading bool
	loadErr string
	// manualRefresh is consumed by HandleLoaded to push a one-shot success
	// toast (auto ticks stay silent).
	manualRefresh bool
	lastRefresh   time.Time

	action DetailAction
	now    func() time.Time
	styles theme.Styles
}

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

func detailColumns() []components.Column {
	return []components.Column{
		{Title: "Topic", Width: 24, Sortable: true},
		{Title: "Partition", Width: 9, Sortable: true},
		{Title: "Committed", Width: 14, Sortable: true},
		{Title: "End", Width: 14, Sortable: true},
		{Title: "Lag", Width: 14, Sortable: true},
		{Title: "Member", Width: 24, Sortable: true},
	}
}

func (d *DetailModel) Init() tea.Cmd {
	d.loading = true
	return loadDetailCmd(d.svc, d.group)
}

func (d *DetailModel) RefreshCmd() tea.Cmd {
	d.loading = true
	return loadDetailCmd(d.svc, d.group)
}

func (d *DetailModel) Group() string { return d.group }

func (d *DetailModel) SortMode() SortMode { return d.sortMode }

func (d *DetailModel) Description() kafka.GroupDescription { return d.desc }

func (d *DetailModel) Rows() []kafka.PartitionLag {
	out := make([]kafka.PartitionLag, len(d.rows))
	copy(out, d.rows)
	return out
}

func (d *DetailModel) Toasts() *components.Toasts { return d.toasts }

func (d *DetailModel) LatestFlash() (components.Toast, bool) {
	if d.toasts == nil {
		return components.Toast{}, false
	}
	return d.toasts.Latest()
}

func (d *DetailModel) Action() DetailAction { return d.action }

func (d *DetailModel) ConsumeAction() DetailAction {
	a := d.action
	d.action = DetailAction{}
	return a
}

func (d *DetailModel) SetSearch(query string) { d.table.SetSearch(query) }

func (d *DetailModel) ActiveFilter() string { return d.table.Search() }

func (d *DetailModel) SetSize(_, h int) {
	if h > 0 {
		d.table.SetHeight(maxInt(1, h-headerLineCount-3))
	}
}

// headerLineCount is the number of header lines reserved by View() above the
// table — kept in sync with [DetailModel.headerBlock].
const headerLineCount = 2

func (d *DetailModel) KeyHints() []layout.KeyHint {
	return layout.HintsFromBindings(d.bindings())
}

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

// handleTopicJump implements `t`: one subscribed topic → messages of that
// topic; multiple → topics list filtered by the group's topics.
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

// HandleLoaded merges fresh data; also called by the list-screen router.
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

func (d *DetailModel) LastRefresh() time.Time { return d.lastRefresh }

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

func (d *DetailModel) View() string {
	parts := d.headerBlock()
	parts = append(parts, d.table.View())
	if d.loadErr != "" {
		parts = append(parts, d.styles.StatusErr.Render("error: "+d.loadErr))
	}
	return strings.Join(parts, "\n")
}

// headerBlock returns exactly headerLineCount lines so layout can size the
// table reliably. The frame already shows "Group · <name>" in its top
// border — the body skips a duplicate title and packs every metadata field
// into a single chip line (per-partition member ownership lives in the
// table's Member column, so a separate names list would be redundant).
func (d *DetailModel) headerBlock() []string {
	state := d.desc.State
	if state == "" {
		state = emDash
	}
	statePart := d.styles.StatusInfo.Render("State: ") + lipgloss.NewStyle().
		Foreground(groupStateColor(d.styles, d.desc.State)).
		Bold(true).
		Render(state)

	chips := []string{
		statePart,
		d.styles.StatusInfo.Render("Coordinator: " + d.coordSummary()),
		d.styles.StatusInfo.Render("Protocol: " + valueOr(d.desc.Protocol, emDash)),
		d.styles.StatusInfo.Render("Members: " + strconv.Itoa(len(d.desc.Members))),
		d.styles.StatusInfo.Render("Total Lag: " + formatThousands(d.totalLag())),
		d.styles.StatusInfo.Render("Sort: " + d.sortLabel()),
	}
	sep := d.styles.StatusInfo.Render("  ·  ")
	return []string{strings.Join(chips, sep), ""}
}

// totalLag aggregates positive lags across loaded partitions. Negative
// values are -1 sentinels (no committed offset, or end offset failed to
// load — see kafka.PartitionLag) and are excluded.
func (d *DetailModel) totalLag() int64 {
	var total int64
	for _, r := range d.rows {
		if r.Lag > 0 {
			total += r.Lag
		}
	}
	return total
}

func (d *DetailModel) sortLabel() string {
	if d.sortMode == SortFlat {
		return "flat (lag desc)"
	}
	return "grouped"
}

// coordSummary renders the coordinator as "id (host:port)", or just "id"
// when the host hasn't been resolved. Returns emDash before the first
// successful describe (lastRefresh is the canonical "loaded" indicator —
// using it avoids a brittle all-zero-fields heuristic that misfires on
// broker id 0).
func (d *DetailModel) coordSummary() string {
	if d.lastRefresh.IsZero() {
		return emDash
	}
	if d.desc.CoordinatorHost == "" {
		return strconv.FormatInt(int64(d.desc.CoordinatorID), 10)
	}
	return fmt.Sprintf("%d (%s:%d)", d.desc.CoordinatorID, d.desc.CoordinatorHost, d.desc.CoordinatorPort)
}

func valueOr(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

func offsetCell(v int64) string {
	if v < 0 {
		return emDash
	}
	return formatThousands(v)
}

func lagCell(v int64) string {
	if v < 0 {
		return emDash
	}
	return formatThousands(v)
}

// ----- Messages -----

// DetailLoadedMsg surfaces the (description, partition lags) snapshot.
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
