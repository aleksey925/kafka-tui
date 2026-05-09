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

// DetailAction is the host-facing intent of the detail view.
type DetailAction struct {
	Back   bool
	Delete bool
	// OpenReset asks the host to open the reset flow. The host calls
	// [DetailModel.ResetScope] to learn whether the user wants the whole
	// group, a single topic, or one specific partition.
	OpenReset bool
	// Topic requests navigation to messages of the focused topic (`t`).
	Topic string
}

type DetailOptions struct {
	Service  Service
	Group    string
	ReadOnly bool
	Now      func() time.Time
	Styles   theme.Styles
}

// FocusPane identifies which sub-table currently has keyboard focus.
type FocusPane int

const (
	// FocusTopics is the default — j/k navigates topics; partitions follow.
	FocusTopics FocusPane = iota
	// FocusPartitions has the cursor inside the lower table.
	FocusPartitions
)

// DetailModel renders a single group as two stacked tables: an outer
// topics summary on top, with the partitions of the focused topic below.
// Schemas are split so each table only carries columns that make sense at
// its level (no aggregate cells leaking onto partition rows or vice
// versa). `tab` toggles focus, `enter` drills from topics into the
// partitions pane, `esc` walks back up.
type DetailModel struct {
	svc      Service
	group    string
	readOnly bool

	desc kafka.GroupDescription
	rows []kafka.PartitionLag
	// topicPartitions carries the authoritative per-topic partition list
	// fetched from cluster metadata. Falls back to rows-derived
	// partitions if the metadata fetch failed.
	topicPartitions map[string][]int32

	topicsTable *components.Table
	partsTable  *components.Table
	toasts      *components.Toasts

	focus FocusPane
	// lastTopic caches the topic whose partitions are loaded into
	// partsTable so the cursor-driven sync skips no-op rebuilds.
	lastTopic string

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
	return &DetailModel{
		svc:         opts.Service,
		group:       opts.Group,
		readOnly:    opts.ReadOnly,
		topicsTable: components.NewTable(topicColumns(), components.WithStyles(styles)),
		partsTable:  components.NewTable(partColumns(), components.WithStyles(styles)),
		toasts:      components.NewToasts(components.WithToastClock(now), components.WithToastStyles(styles)),
		now:         now,
		styles:      styles,
	}
}

func topicColumns() []components.Column {
	return []components.Column{
		{Title: "ID", Flex: true, MinWidth: 24, Sortable: true},
		{Title: "Partitions", Width: 10, Sortable: true},
		{Title: "Total Lag", Width: 12, Sortable: true},
		{Title: "Members", Width: 10, Sortable: true},
	}
}

func partColumns() []components.Column {
	return []components.Column{
		{Title: "Partition", Width: 9, Sortable: true},
		{Title: "Committed", Width: 14, Sortable: true},
		{Title: "End", Width: 14, Sortable: true},
		{Title: "Lag", Width: 14, Sortable: true},
		{Title: "Member", Flex: true, MinWidth: 20, Sortable: true},
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

func (d *DetailModel) Group() string                       { return d.group }
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

// Focus reports the currently active sub-table.
func (d *DetailModel) Focus() FocusPane { return d.focus }

// FocusedTopic returns the topic owning the currently selected row in
// the topics sub-table, or "" if it's empty. Safe to call on a partially-
// initialized model (the keymap-validation tests construct zero-value
// DetailModels).
func (d *DetailModel) FocusedTopic() string {
	if d.topicsTable == nil {
		return ""
	}
	if row, ok := d.topicsTable.SelectedRow(); ok {
		return row.ID
	}
	return ""
}

// SetSearch forwards a host-driven filter query to both sub-tables —
// `tab` switches focus, not the filter.
func (d *DetailModel) SetSearch(query string) {
	d.topicsTable.SetSearch(query)
	d.partsTable.SetSearch(query)
	d.syncPartitions()
}

func (d *DetailModel) ActiveFilter() string { return d.topicsTable.Search() }

// SetSize sizes both sub-tables. Width is forwarded so their Flex columns
// (ID / Member) expand to fill the body — same convention as the list
// and reset preview tables. Height is split: topics get a third of the
// area (floored at 3 rows), partitions take the rest because they're
// usually the busier pane.
func (d *DetailModel) SetSize(w, h int) {
	if w > 0 {
		d.topicsTable.SetTotalWidth(w)
		d.partsTable.SetTotalWidth(w)
	}
	if h <= 0 {
		return
	}
	budget := maxInt(1, h-headerLineCount-detailChromeRows)
	topicsH := maxInt(3, budget/3)
	partsH := maxInt(3, budget-topicsH)
	d.topicsTable.SetHeight(topicsH)
	d.partsTable.SetHeight(partsH)
}

const (
	// headerLineCount is the number of header lines reserved by View()
	// above the tables — kept in sync with [DetailModel.headerBlock].
	headerLineCount = 2
	// detailChromeRows is the layout overhead between the chip header and
	// the bottom of the partitions table: one title row above topics, one
	// blank divider, one title row above partitions.
	detailChromeRows = 3
)

func (d *DetailModel) KeyHints() []layout.KeyHint {
	return layout.HintsFromBindings(d.bindings())
}

func (d *DetailModel) bindings() []keymap.Binding {
	bs := []keymap.Binding{
		{Keys: []string{"tab"}, Label: "switch table", Category: "Group", Hint: true, Handler: d.actToggleFocus},
		{Keys: []string{"enter"}, Label: "open partitions", Category: "Group", Hint: true, Handler: d.actDrillIn},
		{Keys: []string{"t"}, Label: "jump to topic messages", Category: "Group", Hint: true, Handler: d.actTopicJump},
		{Keys: []string{"r"}, Label: "refresh now", Category: "Group", Hint: true, Handler: d.actRefresh},
		{Keys: []string{"esc", "q"}, Label: "back", Category: "Group", Handler: d.actBack},
	}
	mut := []keymap.Binding{
		{Keys: []string{"R"}, Label: d.resetLabel(), Category: "Mutating", Hint: true, Handler: d.actOpenReset},
		{Keys: []string{"ctrl+d"}, Label: "delete group", Category: "Mutating", Hint: true, Handler: d.actDelete},
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

func (d *DetailModel) actToggleFocus() tea.Cmd {
	if d.focus == FocusTopics {
		d.focus = FocusPartitions
	} else {
		d.focus = FocusTopics
	}
	return nil
}

// actDrillIn moves focus from topics → partitions (no-op when already
// there). esc walks back up the chain.
func (d *DetailModel) actDrillIn() tea.Cmd {
	d.focus = FocusPartitions
	return nil
}

// actBack: from the partitions pane, esc returns focus to the topics
// pane (so the user never accidentally exits the detail view by drilling
// in and pressing esc once); from the topics pane it raises Back.
func (d *DetailModel) actBack() tea.Cmd {
	if d.focus == FocusPartitions {
		d.focus = FocusTopics
		return nil
	}
	d.action.Back = true
	return nil
}

func (d *DetailModel) actTopicJump() tea.Cmd {
	topic := d.FocusedTopic()
	if topic == "" {
		d.toasts.Push(components.ToastInfo, "no topic selected")
		return nil
	}
	d.action.Topic = topic
	return nil
}

func (d *DetailModel) actOpenReset() tea.Cmd {
	if d.readOnly {
		d.toasts.Push(components.ToastWarning, "cluster is read-only — reset blocked")
		return nil
	}
	d.action.OpenReset = true
	return nil
}

// ResetScope returns the scope the user implicitly chose by where their
// cursor is parked: focused on a partition row → a single partition;
// focused on a topic → that topic's partitions; otherwise → the whole
// group. The host calls this when wiring [DetailAction.OpenReset] into
// the reset flow.
func (d *DetailModel) ResetScope() ResetScope {
	if d.focus == FocusPartitions {
		if topic, partition, ok := d.focusedPartition(); ok {
			return ScopePartition{Group: d.group, Topic: topic, Partition: partition}
		}
	}
	if topic := d.FocusedTopic(); topic != "" {
		return ScopeTopic{
			Group:   d.group,
			Topic:   topic,
			Members: d.partitionsOfTopic(topic),
		}
	}
	return ScopeWholeGroup{Group: d.group}
}

func (d *DetailModel) focusedPartition() (string, int32, bool) {
	if d.partsTable == nil {
		return "", 0, false
	}
	row, ok := d.partsTable.SelectedRow()
	if !ok {
		return "", 0, false
	}
	topic, partStr, cut := strings.Cut(row.ID, "/")
	if !cut {
		return "", 0, false
	}
	p, err := strconv.ParseInt(partStr, 10, 32)
	if err != nil {
		return "", 0, false
	}
	return topic, int32(p), true
}

// partitionsOfTopic returns every partition belonging to topic, preferring
// the cluster-metadata snapshot so the reset spans partitions that have
// no prior commits. Falls back to the offsets-derived list (only
// partitions the group has touched) if the metadata fetch wasn't
// available — degraded coverage is still better than failing the flow.
func (d *DetailModel) partitionsOfTopic(topic string) []kafka.TopicPartition {
	if ps, ok := d.topicPartitions[topic]; ok && len(ps) > 0 {
		out := make([]kafka.TopicPartition, len(ps))
		for i, p := range ps {
			out[i] = kafka.TopicPartition{Topic: topic, Partition: p}
		}
		return out
	}
	out := make([]kafka.TopicPartition, 0)
	for _, r := range d.rows {
		if r.Topic == topic {
			out = append(out, kafka.TopicPartition{Topic: topic, Partition: r.Partition})
		}
	}
	return out
}

// resetLabel surfaces the implicit scope right inside the binding label
// so the chrome's hint always tells the user what `R` will actually
// reset (group / topic / partition) at the current cursor position.
// Mirrors [ResetScope]'s branching but skips the partition-list build —
// bindings() is rebuilt every render and we only need the kind here.
func (d *DetailModel) resetLabel() string {
	if d.focus == FocusPartitions {
		if _, _, ok := d.focusedPartition(); ok {
			return "reset partition offset"
		}
	}
	if d.FocusedTopic() != "" {
		return "reset topic offsets"
	}
	return "reset group offsets"
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
	if d.activeTable().SearchActive() {
		d.forwardToActive(key)
		d.syncPartitions()
		return d, nil
	}
	if cmd, ok := keymap.Dispatch(d.bindings(), key); ok {
		d.syncPartitions()
		return d, cmd
	}
	d.forwardToActive(key)
	d.syncPartitions()
	return d, nil
}

func (d *DetailModel) activeTable() *components.Table {
	if d.focus == FocusPartitions {
		return d.partsTable
	}
	return d.topicsTable
}

func (d *DetailModel) forwardToActive(key tea.KeyPressMsg) {
	if d.focus == FocusPartitions {
		tbl, _ := d.partsTable.Update(key)
		d.partsTable = tbl
		return
	}
	tbl, _ := d.topicsTable.Update(key)
	d.topicsTable = tbl
}

// syncPartitions rebuilds the partitions sub-table whenever the focused
// topic in the topics pane changes. Without this, the lower pane would
// show stale partitions while the user navigates the topic list.
func (d *DetailModel) syncPartitions() {
	topic := d.FocusedTopic()
	if topic == d.lastTopic {
		return
	}
	d.lastTopic = topic
	d.partsTable.SetRows(buildPartitionRows(d.rows, topic))
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
	d.topicPartitions = msg.TopicPartitions
	d.topicsTable.SetRows(buildTopicRows(d.rows, d.topicPartitions))
	// rebuild the partitions pane against the freshly-loaded data
	// directly. syncPartitions's caching is for cursor-driven nav; the
	// load path always wants a rebuild because offsets/lag may have
	// changed even when the focused topic stayed the same.
	focused := d.FocusedTopic()
	d.lastTopic = focused
	d.partsTable.SetRows(buildPartitionRows(d.rows, focused))
	if d.manualRefresh {
		d.toasts.Push(components.ToastSuccess, fmt.Sprintf(
			"refreshed · %d partitions", len(d.rows),
		))
		d.manualRefresh = false
	}
}

func (d *DetailModel) LastRefresh() time.Time { return d.lastRefresh }

func buildTopicRows(rows []kafka.PartitionLag, partitions map[string][]int32) []components.Row {
	byTopic := map[string][]kafka.PartitionLag{}
	topics := make([]string, 0)
	for _, r := range rows {
		if _, seen := byTopic[r.Topic]; !seen {
			topics = append(topics, r.Topic)
		}
		byTopic[r.Topic] = append(byTopic[r.Topic], r)
	}
	sort.Strings(topics)
	out := make([]components.Row, 0, len(topics))
	for _, topic := range topics {
		ps := byTopic[topic]
		var totalLag int64
		members := map[string]struct{}{}
		for _, p := range ps {
			if p.Lag > 0 {
				totalLag += p.Lag
			}
			if p.MemberID != "" {
				members[p.MemberID] = struct{}{}
			}
		}
		// partition count comes from cluster metadata when available so
		// the cell reflects the topic's true breadth — partitions without
		// commits would otherwise be invisible. The rows-derived count is
		// the fallback for the rare metadata-fetch failure.
		partCount := len(ps)
		if full, ok := partitions[topic]; ok && len(full) > partCount {
			partCount = len(full)
		}
		out = append(out, components.Row{
			ID: topic,
			Values: []string{
				topic,
				strconv.Itoa(partCount),
				lagCell(totalLag),
				strconv.Itoa(len(members)),
			},
		})
	}
	return out
}

func buildPartitionRows(rows []kafka.PartitionLag, topic string) []components.Row {
	if topic == "" {
		return nil
	}
	parts := make([]kafka.PartitionLag, 0)
	for _, r := range rows {
		if r.Topic == topic {
			parts = append(parts, r)
		}
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].Partition < parts[j].Partition })
	out := make([]components.Row, 0, len(parts))
	for _, p := range parts {
		out = append(out, components.Row{
			ID: rowID(p),
			Values: []string{
				strconv.FormatInt(int64(p.Partition), 10),
				offsetCell(p.Committed),
				offsetCell(p.End),
				lagCell(p.Lag),
				p.MemberID,
			},
		})
	}
	return out
}

func rowID(r kafka.PartitionLag) string {
	return r.Topic + "/" + strconv.FormatInt(int64(r.Partition), 10)
}

func (d *DetailModel) View() string {
	parts := d.headerBlock()
	parts = append(parts,
		d.titleFor("Topics", FocusTopics, len(d.topicsTable.Rows())),
		d.topicsTable.View(),
		"",
		d.titleFor(d.partitionsTitle(), FocusPartitions, len(d.partsTable.Rows())),
		d.partsTable.View(),
	)
	if d.loadErr != "" {
		parts = append(parts, d.styles.StatusErr.Render("error: "+d.loadErr))
	}
	return strings.Join(parts, "\n")
}

func (d *DetailModel) partitionsTitle() string {
	if d.lastTopic == "" {
		return "Partitions"
	}
	return "Partitions · " + d.lastTopic
}

// titleFor styles the per-pane heading. The active pane gets a filled
// triangle prefix and the accent color; the inactive pane stays muted —
// a subtle but unambiguous way to show where keystrokes go.
func (d *DetailModel) titleFor(label string, pane FocusPane, count int) string {
	body := fmt.Sprintf("%s [%d]", label, count)
	if d.focus == pane {
		return d.styles.HelpTitle.Render("▸ " + body)
	}
	return d.styles.StatusInfo.Render("  " + body)
}

// headerBlock renders the metadata chip line above the panes. Returns
// exactly headerLineCount lines (the second is a blank divider).
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

// coordSummary renders the coordinator as "id (host:port)", or just "id"
// when the host hasn't been resolved. Returns emDash before the first
// successful describe.
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

// DetailLoadedMsg surfaces the (description, partition lags, topic
// partitions) snapshot. TopicPartitions carries the full per-topic
// partition list so topic-scope resets target every partition, not just
// the ones with prior commits — see [DetailModel.ResetScope].
type DetailLoadedMsg struct {
	Description     kafka.GroupDescription
	Rows            []kafka.PartitionLag
	TopicPartitions map[string][]int32
	Err             error
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
		// the partitions metadata fetch is best-effort; failures degrade
		// to the d.rows-derived view (partial coverage of topic scope).
		parts, _ := svc.TopicsPartitions(ctx, uniqueTopicsFromRows(rows)...)
		return DetailLoadedMsg{Description: desc, Rows: rows, TopicPartitions: parts}
	}
}

func uniqueTopicsFromRows(rows []kafka.PartitionLag) []string {
	seen := make(map[string]struct{}, len(rows))
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		if _, ok := seen[r.Topic]; ok {
			continue
		}
		seen[r.Topic] = struct{}{}
		out = append(out, r.Topic)
	}
	return out
}
