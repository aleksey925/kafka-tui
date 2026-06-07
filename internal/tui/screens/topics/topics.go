// Package topics implements the topics list screen.
package topics

import (
	"context"
	"fmt"
	"maps"
	"sort"
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
	"github.com/aleksey925/kafka-tui/internal/tui/lifecycle"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// refreshIntervalScreenID is the persistence key for this screen's chosen
// refresh cadence. Stable across releases — changing it would orphan
// previously-saved values in the user's state file.
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
	TopicWatermarksBatch(ctx context.Context, topics ...string) (map[string]kafka.BatchResult[kafka.TopicWatermarks], error)
	TopicSizesBatch(ctx context.Context, topics ...string) (map[string]kafka.BatchResult[int64], error)
	DescribeTopicConfigsBatch(ctx context.Context, topics ...string) (map[string]kafka.BatchResult[[]kafka.TopicConfig], error)

	// RegisterDenials returns the subset of per-topic ACL denials not
	// yet observed by the session-scoped dedup cache (one aggregated
	// warn-toast per RPC group).
	RegisterDenials(ds []kafka.Denial) []kafka.Denial
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

	cols         components.ColumnSelection[kafka.TopicSummary]
	focusTopic   string
	allTopics    []kafka.TopicSummary
	hiddenIntern int
	showInternal bool

	watermarks map[string]kafka.BatchResult[kafka.TopicWatermarks]
	sizes      map[string]kafka.BatchResult[int64]
	configs    map[string]kafka.BatchResult[[]kafka.TopicConfig]

	shownWarnings map[string]struct{}

	table        *components.Table
	toasts       *components.Toasts
	confirm      *components.Confirm
	cloneConfirm *components.Confirm
	pending      pendingOp

	mode     Mode
	create   *CreateForm
	clone    *CloneForm
	progress kafka.CloneProgress
	cloneCh  <-chan kafka.CloneProgress
	cloneCxl context.CancelFunc
	// cloneSrc / cloneDst capture the names for the in-flight clone so
	// the final success / error toast can identify which clone finished —
	// m.clone is reset to nil before the result lands and CloneProgressMsg
	// no longer carries them in its error path.
	cloneSrc string
	cloneDst string

	width, height int
	loading       bool
	refresher     components.Refresher

	refreshIntervals components.RefreshIntervalRepository
	refreshPicker    *components.RefreshPicker

	action Action

	track *lifecycle.Tracker

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
	interval := components.LoadRefreshIntervalOr(opts.RefreshIntervals, refreshIntervalScreenID, defaultRefreshInterval)

	m := &Model{
		svc:              opts.Service,
		readOnly:         opts.ReadOnly,
		focusTopic:       opts.FocusTopic,
		watermarks:       map[string]kafka.BatchResult[kafka.TopicWatermarks]{},
		sizes:            map[string]kafka.BatchResult[int64]{},
		configs:          map[string]kafka.BatchResult[[]kafka.TopicConfig]{},
		shownWarnings:    map[string]struct{}{},
		toasts:           components.NewToasts(components.WithToastClock(now), components.WithToastStyles(styles)),
		now:              now,
		styles:           styles,
		refresher:        components.NewRefresher(interval, now),
		refreshIntervals: opts.RefreshIntervals,
		track:            lifecycle.New(),
	}

	sel, unknown := m.columnSchema().Resolve(opts.Columns)
	m.cols = sel
	m.table = components.NewTable(sel.TableColumns(), components.WithStyles(styles))
	if len(unknown) > 0 {
		m.toasts.Push(components.ToastWarning, "ignoring unknown topics columns: "+strings.Join(unknown, ", "))
	}

	return m
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
		m.cloneConfirm != nil ||
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
		{Keys: []string{"i"}, Label: "toggle internal topics", Category: "Topic", Hint: true, Handler: m.actToggleInternal},
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
	return m.refreshPicker.Bindings()
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
	m.refresher.MarkManual()
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

func (m *Model) handleCreateKey(key tea.KeyPressMsg) tea.Cmd {
	// `s` is the submit shortcut in NORMAL but a literal letter in
	// INSERT or with a segmented popup open — only fire the screen
	// bindings when the user isn't typing.
	inEdit := m.create.Mode() == FormInsert || m.create.Form().PopupActive()
	if !inEdit || key.String() == "esc" {
		if cmd, ok := keymap.Dispatch(m.createBindings(), key); ok {
			return cmd
		}
	}
	c, _ := m.create.Update(key)
	m.create = c
	return nil
}

// createBindings owns the create-topic form. esc has dual semantics: in
// INSERT or with a popup it's owned by the form (returns to NORMAL / closes
// popup); in plain NORMAL it closes the overlay.
func (m *Model) createBindings() []keymap.Binding {
	return []keymap.Binding{
		{Keys: []string{"s"}, Label: "submit (create topic)", Category: "Create topic", Hint: true, Handler: m.actCreateSubmit},
		{Keys: []string{"esc"}, Label: "cancel / leave INSERT / close popup", Category: "Create topic", Hint: true, HandlerMsg: m.actCreateEsc},
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

// Close releases background resources — the host calls it before swapping
// screens so an in-flight clone goroutine doesn't keep its kgo.Client
// pinned until the outer context times out. Safe when nothing is in flight.
func (m *Model) Close() {
	m.track.Close()
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
	if !m.track.Validate(msg.Gen) {
		return
	}
	m.loading = false
	manual := m.refresher.ConsumeManual()
	if msg.Err != nil {
		m.toasts.Push(components.ToastError, "load topics: "+msg.Err.Error())
		return
	}
	m.refresher.MarkSuccess()
	if manual {
		m.toasts.Push(components.ToastSuccess, fmt.Sprintf("refreshed · %d topics", len(msg.Topics)))
	}
	m.allTopics = msg.Topics
	maps.Copy(m.watermarks, msg.Watermarks)
	maps.Copy(m.sizes, msg.Sizes)
	maps.Copy(m.configs, msg.Configs)
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
			Values: m.cols.Row(t),
		})
	}
	m.table.SetRows(rows)
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
		if m.cloneConfirm != nil {
			return m.cloneConfirm.View(m.width, m.height)
		}
		return m.clone.View(m.width)
	case ModeCloning:
		return m.renderCloningOverlay()
	}

	if m.confirm != nil {
		return m.confirm.View(m.width, m.height)
	}
	return m.table.View()
}

func (m *Model) refreshCmd() tea.Cmd {
	m.loading = true
	ctx, gen := m.track.Dispatch()
	return loadCmd(m.svc, ctx, gen)
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
	Watermarks map[string]kafka.BatchResult[kafka.TopicWatermarks]
	Sizes      map[string]kafka.BatchResult[int64]
	Configs    map[string]kafka.BatchResult[[]kafka.TopicConfig]
	// Warnings carries whole-category batch-RPC failures plus the
	// freshly-observed per-topic ACL denials returned by the dedup cache.
	Warnings []string
	Err      error
	// Gen pins the result to the dispatch-time generation; the handler
	// drops mismatches via [lifecycle.Tracker.Validate].
	Gen uint64
}

// TopicMutatedMsg reports the result of a create / delete / clone op.
type TopicMutatedMsg struct {
	Op    string
	Topic string
	Err   error
}

type RefreshTickMsg struct{}

// loadCmd refreshes the topics list along with per-topic watermarks, sizes,
// and configs. The three batches run concurrently so wall-clock load time
// is bound by the slowest single RPC, not by topic count. Whole-category
// failures and per-topic ACL denials are aggregated into one warning toast
// each via the session-scoped dedup cache.
func loadCmd(svc Service, parentCtx context.Context, gen uint64) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(parentCtx, 30*time.Second)
		defer cancel()
		topics, err := svc.ListTopics(ctx)
		if parentCtx.Err() != nil {
			return nil
		}
		if err != nil {
			return TopicsLoadedMsg{Err: err, Gen: gen}
		}
		names := make([]string, len(topics))
		for i, t := range topics {
			names[i] = t.Name
		}

		var (
			wmMap                map[string]kafka.BatchResult[kafka.TopicWatermarks]
			sizeMap              map[string]kafka.BatchResult[int64]
			cfgMap               map[string]kafka.BatchResult[[]kafka.TopicConfig]
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

		if parentCtx.Err() != nil {
			return nil
		}

		warnings := batchFetchWarnings(
			batchFetchStat{name: "watermarks", err: wmErr},
			batchFetchStat{name: "size", err: szErr},
			batchFetchStat{name: "configs", err: cfgErr},
		)
		denials := kafka.CollectDenials(kafka.RPCKindWatermarks, wmMap)
		denials = append(denials, kafka.CollectDenials(kafka.RPCKindSize, sizeMap)...)
		denials = append(denials, kafka.CollectDenials(kafka.RPCKindConfigs, cfgMap)...)
		warnings = append(warnings, formatDenialWarnings(svc.RegisterDenials(denials))...)

		return TopicsLoadedMsg{
			Topics:     topics,
			Watermarks: wmMap,
			Sizes:      sizeMap,
			Configs:    cfgMap,
			Warnings:   warnings,
			Gen:        gen,
		}
	}
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

// formatDenialWarnings groups freshly-observed denials by RPC and
// produces one warn-toast string per group. CollectDenials filters to
// ACL only, so the label is hardcoded — add a kind dimension here if a
// future ErrKind starts flowing through.
func formatDenialWarnings(fresh []kafka.Denial) []string {
	if len(fresh) == 0 {
		return nil
	}
	grouped := make(map[kafka.RPCKind][]string)
	for _, d := range fresh {
		grouped[d.RPC] = append(grouped[d.RPC], d.Topic)
	}
	out := make([]string, 0, len(grouped))
	for rpc, topics := range grouped {
		sort.Strings(topics)
		shown := topics
		var suffix string
		if len(topics) > maxDenialTopicsShown {
			shown = topics[:maxDenialTopicsShown]
			suffix = fmt.Sprintf(", +%d more", len(topics)-maxDenialTopicsShown)
		}
		noun := "topic"
		if len(topics) != 1 {
			noun = "topics"
		}
		out = append(out, fmt.Sprintf(
			"%s: %d %s denied (ACL): %s%s",
			rpc, len(topics), noun, strings.Join(shown, ", "), suffix,
		))
	}
	sort.Strings(out)
	return out
}

const maxDenialTopicsShown = 5

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

// columnSchema declares every topics column once: its config key, table spec,
// and cell renderer. Cell closures read the live watermark / size / config maps
// off m, which are mutated in place (maps.Copy), never reassigned.
func (m *Model) columnSchema() components.ColumnSchema[kafka.TopicSummary] {
	return components.NewColumnSchema([]components.ColumnField[kafka.TopicSummary]{
		{Key: "name", Col: components.Column{Title: "Name", Flex: true, MinWidth: 24, Sortable: true},
			Cell: func(t kafka.TopicSummary) string { return t.Name }},
		{Key: "partitions", Col: components.Column{Title: "Partitions", Width: 10, Sortable: true},
			Cell: func(t kafka.TopicSummary) string { return strconv.Itoa(t.Partitions) }},
		{Key: "replicas", Col: components.Column{Title: "Replicas", Width: 8, Sortable: true},
			Cell: func(t kafka.TopicSummary) string { return strconv.Itoa(t.Replicas) }},
		{Key: "messages", Col: components.Column{Title: "Messages", Width: 12, Sortable: true},
			Cell: func(t kafka.TopicSummary) string {
				r, ok := m.watermarks[t.Name]
				if !ok {
					return "—"
				}
				if marker, denied := denialMarker(r.Err); denied {
					return marker
				}
				return formatThousands(r.Value.MessageCount)
			}},
		{Key: "size", Col: components.Column{Title: "Size", Width: 10, Sortable: true},
			Cell: func(t kafka.TopicSummary) string {
				r, ok := m.sizes[t.Name]
				if !ok {
					return "—"
				}
				if marker, denied := denialMarker(r.Err); denied {
					return marker
				}
				return formatBytes(r.Value)
			}},
		{Key: "cleanup_policy", Col: components.Column{Title: "Cleanup", Width: 12, Sortable: true},
			Cell: func(t kafka.TopicSummary) string { return findConfig(m.configs[t.Name], kafka.ConfigCleanupPolicy) }},
		{Key: "retention_ms", Col: components.Column{Title: "Retention", Width: 14, Sortable: true},
			Cell: func(t kafka.TopicSummary) string { return findConfig(m.configs[t.Name], kafka.ConfigRetentionMs) }},
		{Key: "min_isr", Col: components.Column{Title: "MinISR", Width: 7, Sortable: true},
			Cell: func(t kafka.TopicSummary) string { return findConfig(m.configs[t.Name], kafka.ConfigMinInSyncReplica) }},
		{Key: "internal", Col: components.Column{Title: "Int", Width: 4, Sortable: false},
			Cell: func(t kafka.TopicSummary) string {
				if t.IsInternal {
					return "✓"
				}
				return ""
			}},
	}, DefaultColumns)
}

func findConfig(r kafka.BatchResult[[]kafka.TopicConfig], key string) string {
	if marker, denied := denialMarker(r.Err); denied {
		return marker
	}
	for _, c := range r.Value {
		if c.Key == key {
			return c.Value
		}
	}
	return "—"
}

// denialMarker returns ⊘ for ACL-class errors, — otherwise. The second
// return is false when err is nil so the caller falls through to the
// value rendering.
func denialMarker(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	if kafka.ClassifyError(err) == kafka.ErrKindACL {
		return "⊘", true
	}
	return "—", true
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
