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
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
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
var DefaultColumns = []string{"name", "partitions", "replicas", "messages"}

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

	width, height   int
	loading         bool
	loadErr         string
	refreshInterval time.Duration

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
		svc:             opts.Service,
		readOnly:        opts.ReadOnly,
		columns:         cols,
		filterNames:     filterSet,
		focusTopic:      opts.FocusTopic,
		watermarks:      map[string]kafka.TopicWatermarks{},
		sizes:           map[string]int64{},
		configs:         map[string][]kafka.TopicConfig{},
		table:           tbl,
		toasts:          components.NewToasts(components.WithToastClock(now), components.WithToastStyles(styles)),
		now:             now,
		styles:          styles,
		refreshInterval: opts.RefreshInterval,
	}
}

// Init returns a tea command that loads the initial topics list.
func (m *Model) Init() tea.Cmd {
	return m.refreshCmd()
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

// WantsRawInput reports true while the create / clone form is open so the host
// routes typed characters straight into the form fields instead of activating
// global shortcuts.
func (m *Model) WantsRawInput() bool {
	return m.mode == ModeCreate || m.mode == ModeClone
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
	if m.hiddenIntern > 0 {
		body = fmt.Sprintf("Topics[%d, +%d internal hidden]", visible, m.hiddenIntern)
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

// KeyHints returns the screen-specific hints shown at the bottom row.
func (m *Model) KeyHints() []layout.KeyHint {
	hints := []layout.KeyHint{
		{Key: "enter/m", Label: "messages"},
		{Key: "c", Label: "configs"},
		{Key: "g", Label: "groups"},
		{Key: "/", Label: "search"},
	}
	if !m.readOnly {
		hints = append(hints,
			layout.KeyHint{Key: "n", Label: "new"},
			layout.KeyHint{Key: "D", Label: "delete"},
			layout.KeyHint{Key: "y", Label: "clone"},
			layout.KeyHint{Key: "p", Label: "produce"},
		)
	}
	hints = append(hints, layout.KeyHint{Key: "r", Label: "refresh"})
	return hints
}

// Update routes messages.
func (m *Model) Update(msg tea.Msg) (*Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.SetSize(msg.Width, msg.Height)
		return m, nil
	case TopicsLoadedMsg:
		m.handleLoaded(msg)
		return m, nil
	case TopicMutatedMsg:
		m.handleMutated(msg)
		cmd := m.refreshCmd()
		return m, cmd
	case cloneStartedMsg:
		m.cloneCh = msg.ch
		m.cloneCxl = msg.cancel
		return m, clonePollCmd(msg.ch)
	case CloneProgressMsg:
		cmd := m.handleCloneProgress(msg)
		return m, cmd
	case RefreshTickMsg:
		cmd := m.HandleRefreshTick()
		return m, cmd
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

func (m *Model) handleKey(key tea.KeyPressMsg) (*Model, tea.Cmd) {
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
	if m.table.SearchActive() {
		tbl, _ := m.table.Update(key)
		m.table = tbl
		return m, nil
	}
	return m.handleListKey(key)
}

// handleListKey handles all hotkeys in list mode (no overlay open).
func (m *Model) handleListKey(key tea.KeyPressMsg) (*Model, tea.Cmd) {
	switch key.String() {
	case "enter", "m":
		if row, ok := m.table.SelectedRow(); ok {
			m.action.Messages = row.ID
		}
		return m, nil
	case "c":
		if row, ok := m.table.SelectedRow(); ok {
			m.action.Configs = row.ID
		}
		return m, nil
	case "g":
		if row, ok := m.table.SelectedRow(); ok {
			m.action.Groups = row.ID
		}
		return m, nil
	case "i":
		m.showInternal = !m.showInternal
		m.refreshTable()
		return m, nil
	case "r":
		cmd := m.refreshCmd()
		return m, cmd
	case "n", "p", "y", "D":
		return m.handleMutatingKey(key.String())
	case "esc", "q":
		m.action.Quit = true
		return m, nil
	}
	tbl, _ := m.table.Update(key)
	m.table = tbl
	return m, nil
}

// handleMutatingKey gates write hotkeys behind the read-only flag.
func (m *Model) handleMutatingKey(key string) (*Model, tea.Cmd) {
	if m.readOnly {
		m.toasts.Push(components.ToastWarning, "cluster is read-only — "+actionLabel(key)+" blocked")
		return m, nil
	}
	switch key {
	case "n":
		m.openCreateForm()
		return m, nil
	case "p":
		if row, ok := m.table.SelectedRow(); ok {
			m.action.Produce = row.ID
		}
		return m, nil
	case "y":
		m.openCloneForm()
		return m, nil
	case "D":
		return m.openDeleteConfirm()
	}
	return m, nil
}

func actionLabel(key string) string {
	switch key {
	case "n":
		return "create"
	case "p":
		return "produce"
	case "y":
		return "clone"
	case "D":
		return "delete"
	}
	return key
}

// openDeleteConfirm pops the delete-confirmation modal for the focused topic.
func (m *Model) openDeleteConfirm() (*Model, tea.Cmd) {
	row, ok := m.table.SelectedRow()
	if !ok {
		return m, nil
	}
	m.pending = pendingOp{topic: row.ID}
	m.confirm = components.NewConfirm(
		"Delete topic",
		fmt.Sprintf("Delete topic %q? This cannot be undone.", row.ID),
		components.WithConfirmStyles(m.styles),
	)
	return m, nil
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

func (m *Model) handleCreateKey(key tea.KeyPressMsg) (*Model, tea.Cmd) {
	switch key.String() {
	case "esc":
		m.create = nil
		m.mode = ModeList
		return m, nil
	case "ctrl+s":
		spec, err := m.create.Spec()
		if err != nil {
			m.create.SetError(err.Error())
			return m, nil
		}
		m.create = nil
		m.mode = ModeList
		m.toasts.Push(components.ToastInfo, "creating "+spec.Name+"…")
		return m, createCmd(m.svc, spec)
	}
	c, _ := m.create.Update(key)
	m.create = c
	return m, nil
}

func (m *Model) handleCloneKey(key tea.KeyPressMsg) (*Model, tea.Cmd) {
	switch key.String() {
	case "esc":
		m.clone = nil
		m.mode = ModeList
		return m, nil
	case "ctrl+s":
		src, dst, err := m.clone.Submit()
		if err != nil {
			m.clone.SetError(err.Error())
			return m, nil
		}
		m.mode = ModeCloning
		m.progress = kafka.CloneProgress{}
		m.toasts.Push(components.ToastInfo, "cloning "+src+" → "+dst+"…")
		return m, cloneStartCmd(m.svc, src, dst, m.clone.Options())
	}
	c, _ := m.clone.Update(key)
	m.clone = c
	return m, nil
}

func (m *Model) handleCloningKey(key tea.KeyPressMsg) (*Model, tea.Cmd) {
	if key.String() == "esc" {
		// in-flight clone — ESC abandons the screen but the goroutine continues
		// in the background; we just return to the list.
		m.mode = ModeList
		return m, nil
	}
	return m, nil
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
		if op.topic != "" {
			cmd := deleteCmd(m.svc, op.topic)
			return m, cmd
		}
	case components.ConfirmNo:
		m.confirm = nil
		m.pending = pendingOp{}
	}
	return m, nil
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
		m.loadErr = msg.Err.Error()
		m.toasts.Push(components.ToastError, "load topics: "+msg.Err.Error())
		return
	}
	m.loadErr = ""
	m.allTopics = msg.Topics
	for _, w := range msg.Watermarks {
		m.watermarks[w.Topic] = w.Watermarks
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

	parts := []string{m.counterLine(), m.table.View()}
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

func (m *Model) counterLine() string {
	visible := len(m.visibleTopics())
	hidden := m.hiddenIntern
	body := fmt.Sprintf("%d topics", visible)
	if hidden > 0 {
		body += fmt.Sprintf(", %d internal hidden", hidden)
	}
	if m.loading {
		body += " (loading…)"
	}
	if m.loadErr != "" {
		body += "  " + m.styles.StatusErr.Render("error: "+m.loadErr)
	}
	return m.styles.StatusInfo.Render(body)
}

// refreshCmd dispatches a topic list reload.
func (m *Model) refreshCmd() tea.Cmd {
	m.loading = true
	return loadCmd(m.svc)
}

// AutoRefreshTick returns a [tea.Cmd] that emits a tick for the configured
// refresh interval. Hosts that opt-in call this from Init.
func (m *Model) AutoRefreshTick() tea.Cmd {
	if m.refreshInterval <= 0 {
		return nil
	}
	return tea.Tick(m.refreshInterval, func(time.Time) tea.Msg {
		return RefreshTickMsg{}
	})
}

// HandleRefreshTick triggers a reload + reschedules another tick.
func (m *Model) HandleRefreshTick() tea.Cmd {
	if m.refreshInterval <= 0 {
		return nil
	}
	return tea.Batch(m.refreshCmd(), m.AutoRefreshTick())
}

// ----- Messages -----

// TopicsLoadedMsg is dispatched after a refresh completes.
type TopicsLoadedMsg struct {
	Topics     []kafka.TopicSummary
	Watermarks []TopicWatermarkResult
	Err        error
}

// TopicWatermarkResult pairs a topic with its watermarks for batch loading.
type TopicWatermarkResult struct {
	Topic      string
	Watermarks kafka.TopicWatermarks
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

// loadCmd refreshes the topics list and per-topic watermarks. Watermarks are
// fetched lazily so the "messages" column can render incrementally.
func loadCmd(svc Service) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		topics, err := svc.ListTopics(ctx)
		if err != nil {
			return TopicsLoadedMsg{Err: err}
		}
		watermarks := make([]TopicWatermarkResult, 0, len(topics))
		for _, t := range topics {
			w, werr := svc.TopicWatermarks(ctx, t.Name)
			if werr != nil {
				continue
			}
			watermarks = append(watermarks, TopicWatermarkResult{Topic: t.Name, Watermarks: w})
		}
		return TopicsLoadedMsg{Topics: topics, Watermarks: watermarks}
	}
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
		return components.Column{Title: "Partitions", Width: 10, Align: lipgloss.Right, Sortable: true}
	case "replicas":
		return components.Column{Title: "Replicas", Width: 8, Align: lipgloss.Right, Sortable: true}
	case "messages":
		return components.Column{Title: "Messages", Width: 12, Align: lipgloss.Right, Sortable: true}
	case "size":
		return components.Column{Title: "Size", Width: 10, Align: lipgloss.Right, Sortable: true}
	case "cleanup_policy":
		return components.Column{Title: "Cleanup", Width: 12, Sortable: true}
	case "retention_ms":
		return components.Column{Title: "Retention", Width: 14, Sortable: true}
	case "min_isr":
		return components.Column{Title: "MinISR", Width: 7, Align: lipgloss.Right, Sortable: true}
	case "internal":
		return components.Column{Title: "Int", Width: 4, Align: lipgloss.Center, Sortable: false}
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
