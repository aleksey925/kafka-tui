// Package topics implements the topics list screen.
package topics

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/help"
	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// refreshIntervalScreenID is the persistence key for this screen's chosen
// refresh cadence. Stable across releases — changing it would orphan
// previously-saved values in the user's SQLite store.
const refreshIntervalScreenID = "topics"

// defaultRefreshInterval seeds the refresher when the user hasn't picked a
// value yet (no persisted row); subsequent runs read from the picker store.
const defaultRefreshInterval = 30 * time.Second

// Service abstracts the Kafka admin operations the topics screen needs.
type Service interface {
	ListTopics(ctx context.Context) ([]kafka.TopicSummary, error)
	TopicWatermarks(ctx context.Context, topic string) (kafka.TopicWatermarks, error)
	TopicSize(ctx context.Context, topic string) (int64, error)
	DescribeTopicConfigs(ctx context.Context, topic string) ([]kafka.TopicConfig, error)
	DescribeAllTopicConfigs(ctx context.Context, topic string) ([]kafka.TopicConfig, error)
	TopicPartitions(ctx context.Context, topic string) ([]kafka.PartitionDetail, error)
	CreateTopic(ctx context.Context, spec kafka.CreateTopicSpec) error
	DeleteTopic(ctx context.Context, topic string) error
	CloneTopic(ctx context.Context, src, dst string, opts kafka.CloneOptions) (<-chan kafka.CloneProgress, error)
	AlterTopicConfig(ctx context.Context, topic, key, value string) error

	// Batch fetchers — one RPC per category regardless of topic count.
	TopicWatermarksBatch(ctx context.Context, topics ...string) (map[string]kafka.TopicWatermarks, error)
	TopicSizesBatch(ctx context.Context, topics ...string) (map[string]int64, error)
	DescribeTopicConfigsBatch(ctx context.Context, topics ...string) (map[string][]kafka.TopicConfig, error)
}

// Action describes the screen's pending intent for the host (router).
type Action struct {
	Messages string
	Configs  string
	Groups   string
	Produce  string
	Quit     bool
}

type Mode int

const (
	ModeList Mode = iota
	ModeCreate
	ModeClone
	ModeCloning
)

type Options struct {
	Service    Service
	ReadOnly   bool
	Columns    []string
	FocusTopic string
	// RefreshIntervals persists the user's chosen cadence across runs. nil
	// disables persistence; the screen always starts at
	// [defaultRefreshInterval].
	RefreshIntervals components.RefreshIntervalRepository
	Now              func() time.Time
	Styles           theme.Styles
}

var DefaultColumns = []string{"name", "partitions", "replicas", "cleanup_policy", "messages", "size"}

type Model struct {
	svc      Service
	readOnly bool

	columns      []string
	focusTopic   string
	allTopics    []kafka.TopicSummary
	hiddenIntern int
	showInternal bool

	watermarks map[string]kafka.TopicWatermarks
	sizes      map[string]int64
	configs    map[string][]kafka.TopicConfig

	shownWarnings map[string]struct{}

	table   *components.Table
	toasts  *components.Toasts
	confirm *components.Confirm
	pending pendingOp

	mode     Mode
	create   *CreateForm
	clone    *CloneForm
	progress kafka.CloneProgress
	cloneCh  <-chan kafka.CloneProgress
	cloneCxl context.CancelFunc

	width, height int
	loading       bool
	refresher     components.Refresher
	manualRefresh bool

	refreshIntervals components.RefreshIntervalRepository
	refreshPicker    *components.RefreshPicker

	action Action

	now    func() time.Time
	styles theme.Styles
}

type pendingOp struct {
	topic string
}

func New(opts Options) *Model {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	styles := opts.Styles
	if styles.Palette.Foreground == nil {
		styles = theme.DefaultStyles()
	}
	cols := opts.Columns
	if len(cols) == 0 {
		cols = append([]string(nil), DefaultColumns...)
	} else {
		cols = append([]string(nil), cols...)
	}

	tbl := components.NewTable(buildColumns(cols), components.WithStyles(styles))

	interval := components.LoadRefreshIntervalOr(opts.RefreshIntervals, refreshIntervalScreenID, defaultRefreshInterval)

	return &Model{
		svc:              opts.Service,
		readOnly:         opts.ReadOnly,
		columns:          cols,
		focusTopic:       opts.FocusTopic,
		watermarks:       map[string]kafka.TopicWatermarks{},
		sizes:            map[string]int64{},
		configs:          map[string][]kafka.TopicConfig{},
		shownWarnings:    map[string]struct{}{},
		table:            tbl,
		toasts:           components.NewToasts(components.WithToastClock(now), components.WithToastStyles(styles)),
		now:              now,
		styles:           styles,
		refresher:        components.NewRefresher(interval, now),
		refreshIntervals: opts.RefreshIntervals,
	}
}

// Init schedules the first auto-refresh tick — the recurring chain only
// sustains itself once started.
func (m *Model) Init() tea.Cmd {
	return tea.Batch(m.refreshCmd(), m.AutoRefreshTick())
}

func (m *Model) Action() Action { return m.action }

func (m *Model) ConsumeAction() Action {
	a := m.action
	m.action = Action{}
	return a
}

func (m *Model) CurrentMode() Mode { return m.mode }

// WantsRawInput is true in INSERT / segmented popup so global shortcuts
// like `?` stay reachable from NORMAL.
func (m *Model) WantsRawInput() bool {
	switch m.mode {
	case ModeCreate:
		return m.create.Mode() == FormInsert || m.create.Form().PopupActive()
	case ModeClone:
		return m.clone.Mode() == FormInsert || m.clone.Form().PopupActive()
	case ModeList, ModeCloning:
		return false
	}
	return false
}

func (m *Model) Toasts() *components.Toasts { return m.toasts }

func (m *Model) LatestFlash() (components.Toast, bool) {
	if m.toasts == nil {
		return components.Toast{}, false
	}
	return m.toasts.Latest()
}

func (m *Model) Topics() []kafka.TopicSummary {
	return m.visibleTopics()
}

func (m *Model) AllTopics() []kafka.TopicSummary {
	out := make([]kafka.TopicSummary, len(m.allTopics))
	copy(out, m.allTopics)
	return out
}

func (m *Model) Cursor() int { return m.table.Cursor() }

func (m *Model) Title() string {
	visible := len(m.visibleTopics())
	q := m.table.Search()
	// hidden-internal count only makes sense without an active filter — once
	// the user is searching, the filtered count is the headline.
	var body string
	if q == "" && m.hiddenIntern > 0 {
		body = fmt.Sprintf("Topics [%d, +%d internal hidden]", visible, m.hiddenIntern)
	} else {
		body = "Topics " + layout.Counter(q, m.table.FilteredCount(), visible)
	}
	if m.loading {
		body += " (loading…)"
	}
	return body
}

func (m *Model) Breadcrumb() string { return "" }

func (m *Model) ShowInternal() bool { return m.showInternal }

func (m *Model) HiddenInternalCount() int { return m.hiddenIntern }

func (m *Model) CreateForm() *CreateForm { return m.create }

func (m *Model) CloneForm() *CloneForm { return m.clone }

func (m *Model) CloneProgress() kafka.CloneProgress { return m.progress }

func (m *Model) SetSearch(query string) { m.table.SetSearch(query) }

func (m *Model) ActiveFilter() string { return m.table.Search() }

// HasOverlay includes forms so the host's q/esc fallback yields to the
// form's NORMAL/INSERT dispatcher instead of popping the screen.
func (m *Model) HasOverlay() bool {
	return m.confirm != nil ||
		m.refreshPicker != nil ||
		m.mode == ModeCloning ||
		m.mode == ModeCreate ||
		m.mode == ModeClone
}

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
	if m.refreshPicker != nil {
		return m.pickerBindings()
	}
	switch m.mode {
	case ModeCreate:
		return m.createBindings()
	case ModeClone:
		return m.cloneBindings()
	case ModeCloning:
		return m.cloningBindings()
	case ModeList:
		return m.listBindings()
	}
	return m.listBindings()
}

func (m *Model) listBindings() []keymap.Binding {
	bs := []keymap.Binding{
		{Keys: []string{"enter", "m"}, Label: "browse messages", Category: "Topic", Hint: true, Handler: m.actMessages},
		{Keys: []string{"c"}, Label: "topic configs", Category: "Topic", Hint: true, Handler: m.actConfigs},
		{Keys: []string{"g"}, Label: "consumer groups for topic", Category: "Topic", Hint: true, Handler: m.actGroups},
		{Keys: []string{"i"}, Label: "toggle internal topics", Category: "Topic", Handler: m.actToggleInternal},
		{Keys: []string{"r"}, Label: "refresh now", Category: "Topic", Hint: true, Handler: m.actRefresh},
		{Keys: []string{"esc", "q"}, Label: "back", Category: "Topic", Handler: m.actQuit},
	}
	mut := []keymap.Binding{
		{Keys: []string{"n"}, Label: "new topic", Category: "Mutating", Hint: true, Handler: m.actNewTopic},
		{Keys: []string{"y"}, Label: "clone topic", Category: "Mutating", Hint: true, Handler: m.actCloneTopic},
		{Keys: []string{"p"}, Label: "produce to topic", Category: "Mutating", Hint: true, Handler: m.actProduceTopic},
		{Keys: []string{"ctrl+d"}, Label: "delete topic", Category: "Mutating", Hint: true, Handler: m.actDeleteTopic},
	}
	if m.readOnly {
		for i := range mut {
			mut[i].Category = ""
			mut[i].Hint = false
		}
	}
	bs = append(bs, mut...)
	// advertise-only: `/` and `ctrl+r` are owned by the host. ctrl+r opens
	// the refresh-interval picker (the actual binding lives in the host's
	// global dispatch).
	bs = append(bs,
		keymap.Binding{Keys: []string{"/"}, Label: "filter rows", Category: "Topic", Hint: true},
		keymap.Binding{Keys: []string{"ctrl+r"}, Label: "set refresh interval", Category: "Topic", Hint: true},
	)
	return bs
}

// pickerBindings exposes the picker's own keymap for help / hints while the
// overlay is open. The picker's internal Update owns the actual key handling.
func (m *Model) pickerBindings() []keymap.Binding {
	if m.refreshPicker == nil {
		return nil
	}
	return m.refreshPicker.Bindings("Refresh interval")
}

func (m *Model) Update(msg tea.Msg) tea.Cmd {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.SetSize(msg.Width, msg.Height)
		return nil
	case TopicsLoadedMsg:
		m.handleLoaded(msg)
		return nil
	case TopicMutatedMsg:
		m.handleMutated(msg)
		cmd := m.refreshCmd()
		return cmd
	case cloneStartedMsg:
		m.cloneCh = msg.ch
		m.cloneCxl = msg.cancel
		return clonePollCmd(msg.ch)
	case CloneProgressMsg:
		cmd := m.handleCloneProgress(msg)
		return cmd
	case RefreshTickMsg:
		cmd := m.HandleRefreshTick()
		return cmd
	case tea.PasteMsg:
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
	// picker owns the foreground when open — every key feeds it until the
	// user confirms or cancels.
	if m.refreshPicker != nil {
		return m.handlePickerKey(key)
	}
	switch m.mode {
	case ModeList:
	case ModeCreate:
		return m.handleCreateKey(key)
	case ModeClone:
		return m.handleCloneKey(key)
	case ModeCloning:
		return m.handleCloningKey(key)
	}

	if m.confirm != nil {
		return m.handleConfirmKey(key)
	}
	if m.toasts != nil {
		_, _ = m.toasts.Update(key)
	}
	return m.handleListKey(key)
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
	m.refreshPicker = nil
	cmd := m.refresher.SetInterval(d, RefreshTickMsg{})
	m.persistRefreshInterval(d)
	return cmd
}

func (m *Model) persistRefreshInterval(d time.Duration) {
	components.SaveRefreshIntervalOrToast(m.refreshIntervals, refreshIntervalScreenID, d, m.toasts)
}

// OpenRefreshPicker mounts the host-driven refresh-interval picker.
// Implements [tui.RefreshConfigurable].
func (m *Model) OpenRefreshPicker() {
	m.refreshPicker = components.NewRefreshPicker(
		m.refresher.Interval(),
		components.WithRefreshPickerStyles(m.styles),
	)
}

func (m *Model) handleListKey(key tea.KeyPressMsg) tea.Cmd {
	if cmd, ok := keymap.Dispatch(m.listBindings(), key); ok {
		return cmd
	}
	tbl, _ := m.table.Update(key)
	m.table = tbl
	return nil
}

func (m *Model) actMessages() tea.Cmd {
	if row, ok := m.table.SelectedRow(); ok {
		m.action.Messages = row.ID
	}
	return nil
}

func (m *Model) actConfigs() tea.Cmd {
	if row, ok := m.table.SelectedRow(); ok {
		m.action.Configs = row.ID
	}
	return nil
}

func (m *Model) actGroups() tea.Cmd {
	if row, ok := m.table.SelectedRow(); ok {
		m.action.Groups = row.ID
	}
	return nil
}

func (m *Model) actToggleInternal() tea.Cmd {
	m.showInternal = !m.showInternal
	m.refreshTable()
	return nil
}

func (m *Model) actRefresh() tea.Cmd {
	if m.loading {
		return nil
	}
	m.manualRefresh = true
	return m.refreshCmd()
}

func (m *Model) actQuit() tea.Cmd {
	m.action.Quit = true
	return nil
}

func (m *Model) blockedReadOnly(action string) tea.Cmd {
	m.toasts.Push(components.ToastWarning, "cluster is read-only — "+action+" blocked")
	return nil
}

func (m *Model) actNewTopic() tea.Cmd {
	if m.readOnly {
		return m.blockedReadOnly("create")
	}
	m.openCreateForm()
	return nil
}

func (m *Model) actCloneTopic() tea.Cmd {
	if m.readOnly {
		return m.blockedReadOnly("clone")
	}
	m.openCloneForm()
	return nil
}

func (m *Model) actProduceTopic() tea.Cmd {
	if m.readOnly {
		return m.blockedReadOnly("produce")
	}
	if row, ok := m.table.SelectedRow(); ok {
		m.action.Produce = row.ID
	}
	return nil
}

func (m *Model) actDeleteTopic() tea.Cmd {
	if m.readOnly {
		return m.blockedReadOnly("delete")
	}
	return m.openDeleteConfirm()
}

func (m *Model) openDeleteConfirm() tea.Cmd {
	row, ok := m.table.SelectedRow()
	if !ok {
		return nil
	}
	m.pending = pendingOp{topic: row.ID}
	m.confirm = components.NewConfirm(
		"Delete topic",
		fmt.Sprintf("Delete topic %q? This cannot be undone.", row.ID),
		components.WithConfirmStyles(m.styles),
	)
	return nil
}

func (m *Model) openCreateForm() {
	m.create = NewCreateForm(m.styles)
	m.mode = ModeCreate
}

func (m *Model) openCloneForm() {
	row, ok := m.table.SelectedRow()
	if !ok {
		m.toasts.Push(components.ToastWarning, "no topic selected")
		return
	}
	m.clone = NewCloneForm(row.ID, m.styles)
	m.mode = ModeClone
}

func (m *Model) handleCreateKey(key tea.KeyPressMsg) tea.Cmd {
	if cmd, ok := keymap.Dispatch(m.createBindings(), key); ok {
		return cmd
	}
	c, _ := m.create.Update(key)
	m.create = c
	return nil
}

func (m *Model) handleCloneKey(key tea.KeyPressMsg) tea.Cmd {
	if cmd, ok := keymap.Dispatch(m.cloneBindings(), key); ok {
		return cmd
	}
	c, _ := m.clone.Update(key)
	m.clone = c
	return nil
}

func (m *Model) handleCloningKey(key tea.KeyPressMsg) tea.Cmd {
	cmd, _ := keymap.Dispatch(m.cloningBindings(), key)
	return cmd
}

// createBindings owns the create-topic form. esc has dual semantics: in
// INSERT or with a popup it's owned by the form (returns to NORMAL / closes
// popup); in plain NORMAL it closes the overlay.
func (m *Model) createBindings() []keymap.Binding {
	return []keymap.Binding{
		{Keys: []string{"ctrl+s"}, Label: "submit (create topic)", Category: "Create topic", Hint: true, Handler: m.actCreateSubmit},
		{Keys: []string{"esc"}, Label: "cancel / leave INSERT / close popup", Category: "Create topic", Hint: true, HandlerMsg: m.actCreateEsc},
	}
}

func (m *Model) cloneBindings() []keymap.Binding {
	return []keymap.Binding{
		{Keys: []string{"ctrl+s"}, Label: "submit (clone topic)", Category: "Clone topic", Hint: true, Handler: m.actCloneSubmit},
		{Keys: []string{"esc"}, Label: "cancel / leave INSERT / close popup", Category: "Clone topic", Hint: true, HandlerMsg: m.actCloneEsc},
	}
}

func (m *Model) cloningBindings() []keymap.Binding {
	return []keymap.Binding{
		{Keys: []string{"esc"}, Label: "leave (clone keeps running in background)", Category: "Cloning", Hint: true, Handler: m.actCloningLeave},
	}
}

func (m *Model) actCreateSubmit() tea.Cmd {
	spec, err := m.create.Spec()
	if err != nil {
		m.create.SetError(err.Error())
		return nil
	}
	m.create = nil
	m.mode = ModeList
	m.toasts.Push(components.ToastInfo, "creating "+spec.Name+"…")
	return createCmd(m.svc, spec)
}

func (m *Model) actCreateEsc(key tea.KeyPressMsg) tea.Cmd {
	if m.create.Mode() == FormInsert || m.create.Form().PopupActive() {
		c, _ := m.create.Update(key)
		m.create = c
		return nil
	}
	m.create = nil
	m.mode = ModeList
	return nil
}

func (m *Model) actCloneSubmit() tea.Cmd {
	src, dst, err := m.clone.Submit()
	if err != nil {
		m.clone.SetError(err.Error())
		return nil
	}
	m.mode = ModeCloning
	m.progress = kafka.CloneProgress{}
	m.toasts.Push(components.ToastInfo, "cloning "+src+" → "+dst+"…")
	return cloneStartCmd(m.svc, src, dst, m.clone.Options())
}

func (m *Model) actCloneEsc(key tea.KeyPressMsg) tea.Cmd {
	if m.clone.Mode() == FormInsert || m.clone.Form().PopupActive() {
		c, _ := m.clone.Update(key)
		m.clone = c
		return nil
	}
	m.clone = nil
	m.mode = ModeList
	return nil
}

func (m *Model) actCloningLeave() tea.Cmd {
	m.mode = ModeList
	return nil
}

func (m *Model) handleCloneProgress(msg CloneProgressMsg) tea.Cmd {
	m.progress = msg.Progress
	if msg.Progress.Done {
		m.mode = ModeList
		m.clone = nil
		ch := m.cloneCh
		m.cloneCh = nil
		if m.cloneCxl != nil {
			m.cloneCxl()
			m.cloneCxl = nil
		}
		if msg.Progress.Err != nil {
			m.toasts.Push(components.ToastError, "clone failed: "+msg.Progress.Err.Error())
		} else {
			m.toasts.Push(components.ToastSuccess, fmt.Sprintf("clone done — %d records", msg.Progress.Copied))
		}
		// drain any remaining items so the producer goroutine isn't blocked
		// waiting on a closed-but-buffered channel.
		if ch != nil {
			go drainChannel(ch)
		}
		return nil
	}
	if m.cloneCh != nil {
		return clonePollCmd(m.cloneCh)
	}
	return nil
}

// Close releases background resources — the host calls it before swapping
// screens so an in-flight clone goroutine doesn't keep its kgo.Client
// pinned until the outer context times out. Safe when nothing is in flight.
func (m *Model) Close() {
	if m.cloneCxl != nil {
		m.cloneCxl()
		m.cloneCxl = nil
	}
	if m.cloneCh != nil {
		ch := m.cloneCh
		m.cloneCh = nil
		go drainChannel(ch)
	}
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
		if op.topic != "" {
			cmd := deleteCmd(m.svc, op.topic)
			return cmd
		}
	case components.ConfirmNo:
		m.confirm = nil
		m.pending = pendingOp{}
	}
	return nil
}

func (m *Model) PendingTopic() string {
	return m.pending.topic
}

func (m *Model) ConfirmOpen() bool { return m.confirm != nil }

func (m *Model) handleLoaded(msg TopicsLoadedMsg) {
	m.loading = false
	if msg.Err != nil {
		m.toasts.Push(components.ToastError, "load topics: "+msg.Err.Error())
		m.manualRefresh = false
		return
	}
	m.refresher.MarkSuccess()
	if m.manualRefresh {
		m.toasts.Push(components.ToastSuccess, fmt.Sprintf("refreshed · %d topics", len(msg.Topics)))
		m.manualRefresh = false
	}
	m.allTopics = msg.Topics
	for _, w := range msg.Watermarks {
		m.watermarks[w.Topic] = w.Watermarks
	}
	for _, s := range msg.Sizes {
		m.sizes[s.Topic] = s.Size
	}
	for _, c := range msg.Configs {
		m.configs[c.Topic] = c.Configs
	}
	for _, w := range msg.Warnings {
		if _, seen := m.shownWarnings[w]; seen {
			continue
		}
		m.shownWarnings[w] = struct{}{}
		m.toasts.Push(components.ToastWarning, w)
	}
	m.refreshTable()
	if m.focusTopic != "" {
		m.table.GoToID(m.focusTopic)
		m.focusTopic = ""
	}
}

func (m *Model) handleMutated(msg TopicMutatedMsg) {
	if msg.Err != nil {
		m.toasts.Push(components.ToastError, msg.Op+" "+msg.Topic+": "+msg.Err.Error())
		return
	}
	m.toasts.Push(components.ToastSuccess, msg.Op+" "+msg.Topic+" — done")
}

func (m *Model) visibleTopics() []kafka.TopicSummary {
	hidden := 0
	out := make([]kafka.TopicSummary, 0, len(m.allTopics))
	for _, t := range m.allTopics {
		if t.IsInternal && !m.showInternal {
			hidden++
			continue
		}
		out = append(out, t)
	}
	m.hiddenIntern = hidden
	return out
}

func (m *Model) refreshTable() {
	visible := m.visibleTopics()
	rows := make([]components.Row, 0, len(visible))
	for _, t := range visible {
		rows = append(rows, components.Row{
			ID:     t.Name,
			Values: m.rowValues(t),
		})
	}
	m.table.SetRows(rows)
}

func (m *Model) rowValues(t kafka.TopicSummary) []string {
	out := make([]string, 0, len(m.columns))
	for _, col := range m.columns {
		out = append(out, m.cellFor(col, t))
	}
	return out
}

func (m *Model) cellFor(col string, t kafka.TopicSummary) string {
	switch col {
	case "name":
		return t.Name
	case "partitions":
		return strconv.Itoa(t.Partitions)
	case "replicas":
		return strconv.Itoa(t.Replicas)
	case "messages":
		if w, ok := m.watermarks[t.Name]; ok {
			return formatThousands(w.MessageCount)
		}
		return "—"
	case "size":
		if s, ok := m.sizes[t.Name]; ok {
			return formatBytes(s)
		}
		return "—"
	case "cleanup_policy":
		return findConfig(m.configs[t.Name], kafka.ConfigCleanupPolicy)
	case "retention_ms":
		return findConfig(m.configs[t.Name], kafka.ConfigRetentionMs)
	case "min_isr":
		return findConfig(m.configs[t.Name], kafka.ConfigMinInSyncReplica)
	case "internal":
		if t.IsInternal {
			return "✓"
		}
		return ""
	default:
		return ""
	}
}

func (m *Model) View() string {
	if m.refreshPicker != nil {
		return m.refreshPicker.View(m.width)
	}
	switch m.mode {
	case ModeList:
	case ModeCreate:
		return m.create.View(m.width)
	case ModeClone:
		return m.clone.View(m.width)
	case ModeCloning:
		return m.renderCloningOverlay()
	}

	if m.confirm != nil {
		return m.confirm.View(m.width, m.height)
	}
	return m.table.View()
}

func (m *Model) renderCloningOverlay() string {
	header := m.styles.HelpTitle.Render("Cloning…")
	body := fmt.Sprintf(
		"copied %s / %s records",
		formatThousands(m.progress.Copied),
		formatThousands(m.progress.Total),
	)
	hint := m.styles.HintLabel.Render("esc — return to list (clone continues in background)")
	return strings.Join([]string{header, m.styles.Command.Render(body), "", hint}, "\n")
}

func (m *Model) refreshCmd() tea.Cmd {
	m.loading = true
	return loadCmd(m.svc)
}

// AutoRefreshTick emits a tick for the configured refresh interval. Hosts
// that opt-in call this from Init.
func (m *Model) AutoRefreshTick() tea.Cmd { return m.refresher.Tick(RefreshTickMsg{}) }

// HandleRefreshTick triggers a reload + reschedules another tick. The reload
// is skipped while a previous load is in flight or auto-refresh is paused;
// the ticker keeps running so resuming is instantaneous.
func (m *Model) HandleRefreshTick() tea.Cmd {
	next := m.refresher.Tick(RefreshTickMsg{})
	if next == nil || m.loading {
		return next
	}
	return tea.Batch(m.refreshCmd(), next)
}

func (m *Model) RefreshInterval() time.Duration { return m.refresher.Interval() }

func (m *Model) LastRefresh() time.Time { return m.refresher.LastRefresh() }

// TopicsLoadedMsg is dispatched after a refresh completes.
type TopicsLoadedMsg struct {
	Topics     []kafka.TopicSummary
	Watermarks []TopicWatermarkResult
	Sizes      []TopicSizeResult
	Configs    []TopicConfigResult
	// Warnings carries summaries of whole-category batch-RPC failures.
	// Per-topic errors inside an otherwise-successful batch are dropped.
	Warnings []string
	Err      error
}

type TopicWatermarkResult struct {
	Topic      string
	Watermarks kafka.TopicWatermarks
}

type TopicSizeResult struct {
	Topic string
	Size  int64
}

type TopicConfigResult struct {
	Topic   string
	Configs []kafka.TopicConfig
}

// TopicMutatedMsg reports the result of a create / delete / clone op.
type TopicMutatedMsg struct {
	Op    string
	Topic string
	Err   error
}

type CloneProgressMsg struct {
	Progress kafka.CloneProgress
}

type RefreshTickMsg struct{}

// loadCmd refreshes the topics list along with per-topic watermarks, sizes,
// and configs. The three batches run concurrently so wall-clock load time
// is bound by the slowest single RPC, not by topic count. A whole-category
// failure is surfaced as one warning toast.
func loadCmd(svc Service) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		topics, err := svc.ListTopics(ctx)
		if err != nil {
			return TopicsLoadedMsg{Err: err}
		}
		names := make([]string, len(topics))
		for i, t := range topics {
			names[i] = t.Name
		}

		var (
			wmMap                map[string]kafka.TopicWatermarks
			sizeMap              map[string]int64
			cfgMap               map[string][]kafka.TopicConfig
			wmErr, szErr, cfgErr error
			wg                   sync.WaitGroup
		)
		wg.Add(3)
		go func() {
			defer wg.Done()
			wmMap, wmErr = svc.TopicWatermarksBatch(ctx, names...)
		}()
		go func() {
			defer wg.Done()
			sizeMap, szErr = svc.TopicSizesBatch(ctx, names...)
		}()
		go func() {
			defer wg.Done()
			cfgMap, cfgErr = svc.DescribeTopicConfigsBatch(ctx, names...)
		}()
		wg.Wait()

		return TopicsLoadedMsg{
			Topics:     topics,
			Watermarks: flattenWatermarks(wmMap, topics),
			Sizes:      flattenSizes(sizeMap, topics),
			Configs:    flattenConfigs(cfgMap, topics),
			Warnings: batchFetchWarnings(
				batchFetchStat{name: "watermarks", err: wmErr},
				batchFetchStat{name: "size", err: szErr},
				batchFetchStat{name: "configs", err: cfgErr},
			),
		}
	}
}

func flattenWatermarks(m map[string]kafka.TopicWatermarks, topics []kafka.TopicSummary) []TopicWatermarkResult {
	if len(m) == 0 {
		return nil
	}
	out := make([]TopicWatermarkResult, 0, len(m))
	for _, t := range topics {
		if w, ok := m[t.Name]; ok {
			out = append(out, TopicWatermarkResult{Topic: t.Name, Watermarks: w})
		}
	}
	return out
}

func flattenSizes(m map[string]int64, topics []kafka.TopicSummary) []TopicSizeResult {
	if len(m) == 0 {
		return nil
	}
	out := make([]TopicSizeResult, 0, len(m))
	for _, t := range topics {
		if s, ok := m[t.Name]; ok {
			out = append(out, TopicSizeResult{Topic: t.Name, Size: s})
		}
	}
	return out
}

func flattenConfigs(m map[string][]kafka.TopicConfig, topics []kafka.TopicSummary) []TopicConfigResult {
	if len(m) == 0 {
		return nil
	}
	out := make([]TopicConfigResult, 0, len(m))
	for _, t := range topics {
		if c, ok := m[t.Name]; ok {
			out = append(out, TopicConfigResult{Topic: t.Name, Configs: c})
		}
	}
	return out
}

type batchFetchStat struct {
	name string
	err  error
}

func batchFetchWarnings(stats ...batchFetchStat) []string {
	var out []string
	for _, s := range stats {
		if s.err != nil {
			out = append(out, fmt.Sprintf("%s unavailable: %s", s.name, s.err.Error()))
		}
	}
	return out
}

func deleteCmd(svc Service, topic string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err := svc.DeleteTopic(ctx, topic)
		return TopicMutatedMsg{Op: "delete", Topic: topic, Err: err}
	}
}

func createCmd(svc Service, spec kafka.CreateTopicSpec) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err := svc.CreateTopic(ctx, spec)
		return TopicMutatedMsg{Op: "create", Topic: spec.Name, Err: err}
	}
}

// cloneStartedMsg hands the freshly-opened progress channel back to the
// model so it can drive a chain of clonePollCmds.
type cloneStartedMsg struct {
	ch     <-chan kafka.CloneProgress
	cancel context.CancelFunc
}

func cloneStartCmd(svc Service, src, dst string, opts kafka.CloneOptions) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		ch, err := svc.CloneTopic(ctx, src, dst, opts)
		if err != nil {
			cancel()
			return CloneProgressMsg{Progress: kafka.CloneProgress{Done: true, Err: err}}
		}
		return cloneStartedMsg{ch: ch, cancel: cancel}
	}
}

// clonePollCmd reads one progress message from ch. When the channel closes
// before a Done flag arrived, it synthesizes one so the screen always
// transitions back to ModeList.
func clonePollCmd(ch <-chan kafka.CloneProgress) tea.Cmd {
	return func() tea.Msg {
		p, ok := <-ch
		if !ok {
			return CloneProgressMsg{Progress: kafka.CloneProgress{Done: true}}
		}
		return CloneProgressMsg{Progress: p}
	}
}

// drainChannel releases the clone goroutine when the user transitions away
// before the channel is fully drained.
func drainChannel(ch <-chan kafka.CloneProgress) {
	for range ch {
		_ = struct{}{}
	}
}

func buildColumns(keys []string) []components.Column {
	out := make([]components.Column, 0, len(keys))
	for _, k := range keys {
		out = append(out, columnSpec(k))
	}
	return out
}

func columnSpec(key string) components.Column {
	switch key {
	case "name":
		return components.Column{Title: "Name", Flex: true, MinWidth: 24, Sortable: true}
	case "partitions":
		return components.Column{Title: "Partitions", Width: 10, Sortable: true}
	case "replicas":
		return components.Column{Title: "Replicas", Width: 8, Sortable: true}
	case "messages":
		return components.Column{Title: "Messages", Width: 12, Sortable: true}
	case "size":
		return components.Column{Title: "Size", Width: 10, Sortable: true}
	case "cleanup_policy":
		return components.Column{Title: "Cleanup", Width: 12, Sortable: true}
	case "retention_ms":
		return components.Column{Title: "Retention", Width: 14, Sortable: true}
	case "min_isr":
		return components.Column{Title: "MinISR", Width: 7, Sortable: true}
	case "internal":
		return components.Column{Title: "Int", Width: 4, Sortable: false}
	default:
		return components.Column{Title: key, Width: 10}
	}
}

func findConfig(cfgs []kafka.TopicConfig, key string) string {
	for _, c := range cfgs {
		if c.Key == key {
			return c.Value
		}
	}
	return "—"
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

func formatBytes(n int64) string {
	const (
		kib = 1024
		mib = 1024 * kib
		gib = 1024 * mib
	)
	switch {
	case n >= gib:
		return fmt.Sprintf("%.1f GiB", float64(n)/float64(gib))
	case n >= mib:
		return fmt.Sprintf("%.1f MiB", float64(n)/float64(mib))
	case n >= kib:
		return fmt.Sprintf("%.1f KiB", float64(n)/float64(kib))
	default:
		return strconv.FormatInt(n, 10) + " B"
	}
}
