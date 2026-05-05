// Package topics implements the topics list screen — the main entry point
// for browsing topics on the active cluster.
//
// The screen renders a sortable, searchable, configurable-columns table of
// topics with a counter line ("47 topics, 3 internal hidden"), and dispatches
// hotkeys for navigation (enter / m → messages, c → configs, g → consumer
// groups for the topic, p → produce) plus inline overlays for create / clone
// / delete. It owns no Kafka client itself — all admin and read calls flow
// through a pluggable [Service], which keeps the screen unit-testable.
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

// Service abstracts the Kafka admin operations the topics screen needs.
// Production code wires this to a real *kafka.Client; tests pass a fake.
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

	// Batch fetchers used by the list view to render the full row in
	// O(1) RPCs (per category) regardless of topic count.
	TopicWatermarksBatch(ctx context.Context, topics ...string) (map[string]kafka.TopicWatermarks, error)
	TopicSizesBatch(ctx context.Context, topics ...string) (map[string]int64, error)
	DescribeTopicConfigsBatch(ctx context.Context, topics ...string) (map[string][]kafka.TopicConfig, error)
}

// Action describes the screen's pending intent for the host (router).
//
// It is read after every Update; the host reacts and clears it via
// [Model.ConsumeAction]. Multiple fields can be set in the same struct, but
// in practice only one will ever be non-zero per Update tick.
type Action struct {
	// Messages requests navigation to the messages screen for the named topic.
	Messages string
	// Configs requests navigation to the topic configs screen.
	Configs string
	// Groups requests navigation to the consumer-groups list filtered by topic.
	Groups string
	// Produce requests navigation to the produce form for the named topic.
	Produce string
	// Quit signals the user pressed esc/q on the list view.
	Quit bool
}

// Mode is the screen's current sub-mode.
type Mode int

const (
	// ModeList: showing the topic list (default).
	ModeList Mode = iota
	// ModeCreate: create-topic form is overlaid.
	ModeCreate
	// ModeClone: clone-topic form is overlaid.
	ModeClone
	// ModeCloning: clone is in flight, progress overlay visible.
	ModeCloning
)

// Options configure a [Model].
type Options struct {
	// Service is the Kafka admin abstraction. Required.
	Service Service
	// ReadOnly disables destructive hotkeys (n/D/y/p) and surfaces warnings.
	ReadOnly bool
	// Columns lists the column keys to render, in order. Empty falls back to
	// [DefaultColumns].
	Columns []string
	// FilterTopics, when non-empty, limits the displayed topics to this set.
	// Used for groups→topics navigation.
	FilterTopics []string
	// FocusTopic, when non-empty, moves the cursor to this topic after load.
	FocusTopic string
	// RefreshInterval, when > 0, enables periodic auto-refresh.
	RefreshInterval time.Duration
	// Now is the injected clock (defaults to time.Now).
	Now func() time.Time
	// Styles overrides the theme palette (mostly for tests).
	Styles theme.Styles
}

// DefaultColumns is used when config does not override.
var DefaultColumns = []string{"name", "partitions", "replicas", "cleanup_policy", "messages", "size"}

// Model is the topics list screen.
type Model struct {
	svc      Service
	readOnly bool

	columns      []string
	filterNames  map[string]struct{}
	focusTopic   string
	allTopics    []kafka.TopicSummary
	hiddenIntern int
	showInternal bool

	// per-topic lazy data
	watermarks map[string]kafka.TopicWatermarks
	sizes      map[string]int64
	configs    map[string][]kafka.TopicConfig

	// shownWarnings deduplicates batch-fetch warnings across refresh ticks
	// so a permanent ACL/feature failure doesn't spam a toast every 5s.
	// Reset when the screen is reinstantiated (e.g. cluster switch).
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
	// manualRefresh is set when the user pressed `r` and is consumed by
	// handleLoaded to push a one-shot success toast (auto ticks stay silent).
	manualRefresh bool

	action Action

	now    func() time.Time
	styles theme.Styles
}

// pendingOp tracks a destructive action awaiting confirmation. An empty
// topic means no operation is pending; only the delete flow uses this
// today, so a single field is enough.
type pendingOp struct {
	topic string
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
	cols := opts.Columns
	if len(cols) == 0 {
		cols = append([]string(nil), DefaultColumns...)
	} else {
		cols = append([]string(nil), cols...)
	}

	tbl := components.NewTable(buildColumns(cols), components.WithStyles(styles))

	var filterSet map[string]struct{}
	if len(opts.FilterTopics) > 0 {
		filterSet = make(map[string]struct{}, len(opts.FilterTopics))
		for _, name := range opts.FilterTopics {
			filterSet[name] = struct{}{}
		}
	}

	return &Model{
		svc:           opts.Service,
		readOnly:      opts.ReadOnly,
		columns:       cols,
		filterNames:   filterSet,
		focusTopic:    opts.FocusTopic,
		watermarks:    map[string]kafka.TopicWatermarks{},
		sizes:         map[string]int64{},
		configs:       map[string][]kafka.TopicConfig{},
		shownWarnings: map[string]struct{}{},
		table:         tbl,
		toasts:        components.NewToasts(components.WithToastClock(now), components.WithToastStyles(styles)),
		now:           now,
		styles:        styles,
		refresher:     components.NewRefresher(opts.RefreshInterval, now),
	}
}

// Init returns a tea command that loads the initial topics list and
// schedules the first auto-refresh tick (when configured). Without
// scheduling here the recurring tick chain would never start, since
// HandleRefreshTick only fires from within RefreshTickMsg handling.
func (m *Model) Init() tea.Cmd {
	return tea.Batch(m.refreshCmd(), m.AutoRefreshTick())
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

// WantsRawInput reports true only while the create / clone form is
// actively editing a field (INSERT) or has a segmented popup open. In
// NORMAL the form ignores letter keys, so leaving raw-input off there
// lets the user open the help overlay with `?` and use other global
// shortcuts. Switching back to INSERT restores raw-input so literal
// `?`, `:`, `/` land in the field text.
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

// Toasts exposes the toast queue (for tests).
func (m *Model) Toasts() *components.Toasts { return m.toasts }

// LatestFlash returns the freshest live toast from this screen's queue.
func (m *Model) LatestFlash() (components.Toast, bool) {
	if m.toasts == nil {
		return components.Toast{}, false
	}
	return m.toasts.Latest()
}

// Topics returns the topics currently visible (after the internal-toggle
// filter), in declared order. For tests.
func (m *Model) Topics() []kafka.TopicSummary {
	return m.visibleTopics()
}

// AllTopics returns every topic loaded (including hidden internals). For tests.
func (m *Model) AllTopics() []kafka.TopicSummary {
	out := make([]kafka.TopicSummary, len(m.allTopics))
	copy(out, m.allTopics)
	return out
}

// Cursor returns the current table cursor position. For tests.
func (m *Model) Cursor() int { return m.table.Cursor() }

// Title returns the frame title rendered by the host: "Topics[<n>]" with an
// internal-hidden suffix when applicable.
func (m *Model) Title() string {
	visible := len(m.visibleTopics())
	body := fmt.Sprintf("Topics[%d]", visible)
	if q := m.table.Search(); q != "" {
		body = fmt.Sprintf("Topics[%d/%d] </%s>", m.table.FilteredCount(), visible, q)
	} else if m.hiddenIntern > 0 {
		body = fmt.Sprintf("Topics[%d, +%d internal hidden]", visible, m.hiddenIntern)
	}
	if m.loading {
		body += " (loading…)"
	}
	return body
}

// Breadcrumb returns the selected topic name (right-aligned in the frame).
func (m *Model) Breadcrumb() string {
	row, ok := m.table.SelectedRow()
	if !ok {
		return ""
	}
	return row.ID
}

// ShowInternal reports whether internal topics are currently visible.
func (m *Model) ShowInternal() bool { return m.showInternal }

// HiddenInternalCount returns the number of internal topics currently hidden.
func (m *Model) HiddenInternalCount() int { return m.hiddenIntern }

// CreateForm returns the active create form (or nil).
func (m *Model) CreateForm() *CreateForm { return m.create }

// CloneForm returns the active clone form (or nil).
func (m *Model) CloneForm() *CloneForm { return m.clone }

// CloneProgress returns the latest in-flight clone progress (zero when idle).
func (m *Model) CloneProgress() kafka.CloneProgress { return m.progress }

// SetSearch forwards a host-driven filter query to the underlying table.
func (m *Model) SetSearch(query string) { m.table.SetSearch(query) }

// ActiveFilter returns the table's current search query.
func (m *Model) ActiveFilter() string { return m.table.Search() }

// HasOverlay reports whether a modal (delete confirm, in-flight clone
// progress popup, or an open create/clone form) is on top of the list.
// Forms are included so the host's q/esc fallback yields to the form
// instead of popping the screen — the form's own dispatcher decides
// what `q` / `esc` mean inside its NORMAL/INSERT state machine.
func (m *Model) HasOverlay() bool {
	return m.confirm != nil ||
		m.mode == ModeCloning ||
		m.mode == ModeCreate ||
		m.mode == ModeClone
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
}

// KeyHints returns the screen-specific bottom-row hints, derived
// from whichever bindings table the current mode is dispatching from
// — so the hints can never drift from the real dispatcher.
func (m *Model) KeyHints() []layout.KeyHint {
	return layout.HintsFromBindings(m.activeBindings())
}

// HelpSections returns the categorized bindings shown in the `?`
// overlay. Same source as the dispatcher.
func (m *Model) HelpSections() []help.Section {
	return help.SectionsFromBindings(m.activeBindings())
}

// activeBindings picks the bindings slice that the dispatcher is
// currently consuming. Sub-mode dispatchers (create / clone / cloning
// / confirm) each consume their own slice — KeyHints and
// HelpSections mirror them.
func (m *Model) activeBindings() []keymap.Binding {
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

// listBindings is the single source of truth for list-mode shortcuts.
// Both the dispatcher (handleListKey) and the user-facing surfaces
// (KeyHints, HelpSections) consume this slice — adding a binding
// requires exactly one append.
func (m *Model) listBindings() []keymap.Binding {
	bs := []keymap.Binding{
		{Keys: []string{"enter", "m"}, Label: "browse messages", Category: "Topic", Hint: true, Handler: m.actMessages},
		{Keys: []string{"c"}, Label: "topic configs", Category: "Topic", Hint: true, Handler: m.actConfigs},
		{Keys: []string{"g"}, Label: "consumer groups for topic", Category: "Topic", Hint: true, Handler: m.actGroups},
		{Keys: []string{"i"}, Label: "toggle internal topics", Category: "Topic", Handler: m.actToggleInternal},
		{Keys: []string{"r"}, Label: "refresh now", Category: "Topic", Hint: true, Handler: m.actRefresh},
		{Keys: []string{"esc", "q"}, Label: "back / quit", Category: "Topic", Handler: m.actQuit},
	}
	mut := []keymap.Binding{
		{Keys: []string{"n"}, Label: "new topic", Category: "Mutating", Hint: true, Handler: m.actNewTopic},
		{Keys: []string{"y"}, Label: "clone topic", Category: "Mutating", Hint: true, Handler: m.actCloneTopic},
		{Keys: []string{"p"}, Label: "produce to topic", Category: "Mutating", Hint: true, Handler: m.actProduceTopic},
		{Keys: []string{"D"}, Label: "delete topic", Category: "Mutating", Hint: true, Handler: m.actDeleteTopic},
	}
	if m.readOnly {
		// keys still claimed (toast emitted) but hidden from hints / help —
		// the user shouldn't see actions that won't work.
		for i := range mut {
			mut[i].Category = ""
			mut[i].Hint = false
		}
	}
	bs = append(bs, mut...)
	// `/` and `ctrl+r` are globals handled by the host — listed here
	// (advertise-only, no Handler) so they appear in the chrome's hints
	// bar and the `?` overlay alongside screen-local actions.
	bs = append(bs,
		keymap.Binding{Keys: []string{"/"}, Label: "filter rows", Category: "Topic", Hint: true},
		keymap.Binding{Keys: []string{"ctrl+r"}, Label: "toggle auto-refresh", Category: "Topic", Hint: true},
	)
	return bs
}

// Update routes messages.
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
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	}
	return nil
}

func (m *Model) handleKey(key tea.KeyPressMsg) tea.Cmd {
	switch m.mode {
	case ModeList:
		// fall through to list-mode handling below.
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

// handleListKey routes a keystroke through the bindings table; keys
// not claimed by any binding fall through to the table component for
// row-navigation (j/k/arrow/pgup/pgdn).
func (m *Model) handleListKey(key tea.KeyPressMsg) tea.Cmd {
	if cmd, ok := keymap.Dispatch(m.listBindings(), key); ok {
		return cmd
	}
	tbl, _ := m.table.Update(key)
	m.table = tbl
	return nil
}

// --- list-mode binding handlers ---

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
	// skip duplicate loads — pressing `r` while a previous fetch is
	// still in flight would just queue redundant RPCs.
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

// blockedReadOnly emits the standard "cluster is read-only — X blocked"
// toast and returns a nil cmd so the caller can `return m.blocked(...)`.
// Centralizes the gate logic without coupling handlers to a magic key
// string the way an actMutating(key) factory would.
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

// openDeleteConfirm pops the delete-confirmation modal for the focused topic.
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

// createBindings is the source of truth for the create-topic form.
// esc has dual semantics: in INSERT or with a popup it's owned by the
// form (returns to NORMAL / closes popup); in plain NORMAL it closes
// the overlay. HandlerMsg routes the original keystroke into the form.
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

// Close releases any background resources owned by the screen. The host
// calls it before swapping the active screen, so an in-flight clone
// goroutine doesn't keep running (and holding a worker kgo.Client) until
// its outer context times out. Safe to call when nothing is in flight.
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

// PendingTopic returns the topic name currently awaiting confirmation (for
// tests).
func (m *Model) PendingTopic() string {
	return m.pending.topic
}

// ConfirmOpen reports whether a confirm dialog is currently visible (tests).
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
		// dedupe across ticks — a permanent failure should warn once.
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
		if len(m.filterNames) > 0 {
			if _, ok := m.filterNames[t.Name]; !ok {
				continue
			}
		}
		out = append(out, t)
	}
	m.hiddenIntern = hidden
	return out
}

// refreshTable rebuilds the underlying table rows from m.allTopics.
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

// rowValues returns the rendered cell values for a topic in the configured
// column order.
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

// View renders the screen body.
func (m *Model) View() string {
	switch m.mode {
	case ModeList:
		// fall through to default list rendering below.
	case ModeCreate:
		return m.create.View(m.width)
	case ModeClone:
		return m.clone.View(m.width)
	case ModeCloning:
		return m.renderCloningOverlay()
	}

	parts := []string{m.table.View()}
	if m.confirm != nil {
		parts = append(parts, m.confirm.View(m.width))
	}
	return strings.Join(parts, "\n")
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

// refreshCmd dispatches a topic list reload.
func (m *Model) refreshCmd() tea.Cmd {
	m.loading = true
	return loadCmd(m.svc)
}

// AutoRefreshTick returns a [tea.Cmd] that emits a tick for the configured
// refresh interval. Hosts that opt-in call this from Init.
func (m *Model) AutoRefreshTick() tea.Cmd { return m.refresher.Tick(RefreshTickMsg{}) }

// HandleRefreshTick triggers a reload + reschedules another tick. The reload
// itself is skipped while a previous load is in flight (`m.loading`) or the
// host has paused auto-refresh via [Model.SetRefreshPaused]; the ticker
// keeps running so resuming is instantaneous.
func (m *Model) HandleRefreshTick() tea.Cmd {
	next := m.refresher.Tick(RefreshTickMsg{})
	if next == nil || m.loading || m.refresher.Paused() {
		return next
	}
	return tea.Batch(m.refreshCmd(), next)
}

// RefreshInterval returns the screen's configured auto-refresh tick (0 if
// auto-refresh is disabled in config).
func (m *Model) RefreshInterval() time.Duration { return m.refresher.Interval() }

// SetRefreshPaused toggles the auto-refresh pause flag. The host calls it
// from the global ctrl+r handler so refresh state stays in sync with the
// chrome's Refresh: indicator.
func (m *Model) SetRefreshPaused(paused bool) { m.refresher.SetPaused(paused) }

// LastRefresh returns the wall-clock time of the most recent successful
// load, or zero when no load has completed yet.
func (m *Model) LastRefresh() time.Time { return m.refresher.LastRefresh() }

// ----- Messages -----

// TopicsLoadedMsg is dispatched after a refresh completes.
type TopicsLoadedMsg struct {
	Topics     []kafka.TopicSummary
	Watermarks []TopicWatermarkResult
	Sizes      []TopicSizeResult
	Configs    []TopicConfigResult
	// Warnings carries human-readable summaries of whole-category batch-RPC
	// failures (e.g. broker rejected DescribeConfigs entirely). Per-topic
	// errors inside an otherwise-successful batch are silently dropped.
	Warnings []string
	Err      error
}

// TopicWatermarkResult pairs a topic with its watermarks for batch loading.
type TopicWatermarkResult struct {
	Topic      string
	Watermarks kafka.TopicWatermarks
}

// TopicSizeResult pairs a topic with its on-disk size in bytes.
type TopicSizeResult struct {
	Topic string
	Size  int64
}

// TopicConfigResult pairs a topic with its resolved configs (cleanup.policy,
// retention.ms, etc.) so the list view can render them inline.
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

// CloneProgressMsg surfaces a clone-progress update.
type CloneProgressMsg struct {
	Progress kafka.CloneProgress
}

// RefreshTickMsg is the periodic auto-refresh tick.
type RefreshTickMsg struct{}

// loadCmd refreshes the topics list along with per-topic watermarks, sizes,
// and configs so the list view can render the full row up-front. Each
// category is one batch RPC against the broker (kadm folds the wire-level
// fan-out across brokers itself); the three batches run concurrently so
// the wall-clock load time is bound by the slowest single RPC, not by
// topic count. A whole-category failure is surfaced as one warning toast.
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

// batchFetchWarnings turns batch-level errors into user-facing toasts. A
// single batch RPC failure means *all* per-topic data in that category is
// missing, so we always surface it (no thresholding needed).
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

// cloneStartedMsg is the internal handoff from cloneStartCmd to the model:
// it carries the freshly-opened progress channel so the screen can keep
// streaming intermediate updates into the overlay.
type cloneStartedMsg struct {
	ch     <-chan kafka.CloneProgress
	cancel context.CancelFunc
}

// cloneStartCmd kicks off a clone. svc.CloneTopic returns a progress
// channel immediately; we hand it to the model via cloneStartedMsg so the
// model can drive a chain of clonePollCmds and surface every intermediate
// progress tick to the overlay.
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

// clonePollCmd reads one progress message from ch. When the channel is
// closed before a Done flag arrived, it synthesizes one so the screen
// always transitions back to ModeList.
func clonePollCmd(ch <-chan kafka.CloneProgress) tea.Cmd {
	return func() tea.Msg {
		p, ok := <-ch
		if !ok {
			return CloneProgressMsg{Progress: kafka.CloneProgress{Done: true}}
		}
		return CloneProgressMsg{Progress: p}
	}
}

// drainChannel pulls items from ch until it's closed. Used to release the
// clone goroutine when the user transitions away before the channel is
// fully drained.
func drainChannel(ch <-chan kafka.CloneProgress) {
	for range ch { //nolint:revive // intentional drain.
	}
}

// buildColumns maps the column-keys list into [components.Column] specs.
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

// formatThousands renders an integer with thousands separators.
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

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
