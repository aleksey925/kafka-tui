package messages

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
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

// Service abstracts the Kafka operations the messages screen needs.
type Service interface {
	FetchLastN(ctx context.Context, topic string, n int, partitions []int32) ([]kafka.Message, error)
	FetchEarliest(ctx context.Context, topic string, n int, partitions []int32) ([]kafka.Message, error)
	FetchAtOffset(ctx context.Context, topic string, partition int32, offset int64, count int) ([]kafka.Message, error)
	FetchAtOffsets(ctx context.Context, topic string, offsets map[int32]int64, perPartition int) ([]kafka.Message, error)
	FetchAtTimestamp(ctx context.Context, topic string, ts time.Time, partitions []int32, count int) ([]kafka.Message, error)
	FetchEarlier(ctx context.Context, topic string, baseline map[int32]int64, count int, partitions []int32) ([]kafka.Message, error)
	FetchLater(ctx context.Context, topic string, baseline map[int32]int64, count int, partitions []int32) ([]kafka.Message, error)
	WatermarksFor(ctx context.Context, topic string, partitions []int32) (map[int32]kafka.PartitionWatermarks, error)
	OffsetsForTimestamp(ctx context.Context, topic string, ts time.Time, partitions []int32) (map[int32]int64, error)
	Follow(ctx context.Context, topic string, partitions []int32) (*kafka.FollowSession, error)
}

// ViewStateRepository persists per-(cluster, topic) seek+partition state
// between sessions. nil disables persistence.
type ViewStateRepository interface {
	LoadMessagesView(ctx context.Context, cluster, topic string) (ViewState, bool, error)
	SaveMessagesView(ctx context.Context, cluster, topic string, view ViewState) error
}

// ViewState is the persisted "where am I looking" state. Live mode is
// intentionally not representable so a restart returns to the last
// non-live position rather than re-entering live tail.
type ViewState struct {
	SeekMode   SeekMode
	Partition  int32
	Offset     int64
	Timestamp  time.Time
	HasPart    bool
	Partitions string
}

type Action struct {
	Back               bool
	Produce            string
	PrefillFromMessage *kafka.Message
}

type Mode int

const (
	ModeList Mode = iota
	ModeDetail
	ModeSeek
	ModePartitions
	ModeSmartFilter
)

// SeekMode order matches the digits 1..7 in the seek popup so digit
// shortcuts can index directly.
type SeekMode int

const (
	SeekLatest SeekMode = iota
	SeekEarliest
	SeekFromOffset
	SeekToOffset
	SeekFromTimestamp
	SeekToTimestamp
	SeekLive
)

func (s SeekMode) String() string {
	switch s {
	case SeekLatest:
		return "latest"
	case SeekEarliest:
		return "earliest"
	case SeekFromOffset:
		return "from offset"
	case SeekToOffset:
		return "to offset"
	case SeekFromTimestamp:
		return "from timestamp"
	case SeekToTimestamp:
		return "to timestamp"
	case SeekLive:
		return "live"
	}
	return "?"
}

var DefaultColumns = []string{"timestamp", "partition", "offset", "key", "headers", "value"}

const DefaultPageSize = 200

type Options struct {
	Service    Service
	Topic      string
	Cluster    string
	ReadOnly   bool
	Columns    []string
	PageSize   int
	Clipboard  Clipboard
	FileWriter FileWriter
	Pager      PagerOpener
	OutputDir  string
	ViewState  ViewStateRepository
	Now        func() time.Time
	Styles     theme.Styles
}

type Model struct {
	svc      Service
	topic    string
	cluster  string
	readOnly bool
	repo     ViewStateRepository

	columns   []string
	pageSize  int
	filter    []int32
	clipboard Clipboard
	writer    FileWriter
	pager     PagerOpener
	outputDir string

	messages []kafka.Message
	table    *components.Table
	toasts   *components.Toasts

	mode   Mode
	detail *DetailModel
	// wrap is held at this level so it survives detail re-opens.
	wrap bool

	follow *kafka.FollowSession
	// live is decoupled from m.follow so [Model.Following] reports true
	// during the dial window before the session attaches.
	live bool

	seek SeekState
	// captured edges of the active seek window so `[` / `]` can clamp.
	fromBoundary map[int32]int64
	toBoundary   map[int32]int64
	// fetchGen is bumped on every dispatchSeek / stopFollow; async cmds
	// stamp this on results, handlers drop messages with stale Gen so
	// late arrivals from a previous seek/live session can't leak onto
	// the screen after the user has moved on.
	fetchGen uint64

	// spinnerFrame advances on LiveTickMsg so the LIVE label always
	// shows movement on quiet topics.
	spinnerFrame int

	manualRefresh bool

	seekPopup       *seekPopup
	partitionsPopup *partitionsPopup
	smartFilterOpen bool

	width, height int

	loading bool

	action Action
	now    func() time.Time
	styles theme.Styles
}

type SeekState struct {
	Mode      SeekMode
	Partition int32
	Offset    int64
	Timestamp time.Time
	HasPart   bool
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
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = DefaultPageSize
	}
	tbl := components.NewTable(buildColumns(cols), components.WithStyles(styles))

	return &Model{
		svc:       opts.Service,
		topic:     opts.Topic,
		cluster:   opts.Cluster,
		readOnly:  opts.ReadOnly,
		repo:      opts.ViewState,
		columns:   cols,
		pageSize:  pageSize,
		clipboard: opts.Clipboard,
		writer:    opts.FileWriter,
		pager:     opts.Pager,
		outputDir: opts.OutputDir,
		table:     tbl,
		toasts:    components.NewToasts(components.WithToastClock(now), components.WithToastStyles(styles)),
		now:       now,
		styles:    styles,
		wrap:      true,
		seek:      SeekState{Mode: SeekLatest},
	}
}

// Init: with persisted state, restoration is two-phase (fetch watermarks,
// then clamp/drop stale fields). Otherwise dispatches the default seek.
func (m *Model) Init() tea.Cmd {
	if m.repo != nil && m.cluster != "" && m.topic != "" {
		view, ok, err := m.repo.LoadMessagesView(context.Background(), m.cluster, m.topic)
		if err == nil && ok {
			return restoreViewCmd(m.svc, m.topic, view)
		}
	}
	return m.dispatchSeek()
}

type viewRestoredMsg struct {
	raw        ViewState
	watermarks map[int32]kafka.PartitionWatermarks
	err        error
}

func restoreViewCmd(svc Service, topic string, raw ViewState) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		wm, err := svc.WatermarksFor(ctx, topic, nil)
		return viewRestoredMsg{raw: raw, watermarks: wm, err: err}
	}
}

// handleViewRestored clamps stale fields against fresh watermarks and
// dispatches. stopFollow runs first so a late FollowStartedMsg from a
// dial the user kicked off during the async restore can't attach to a
// session that doesn't match the restored seek state.
func (m *Model) handleViewRestored(msg viewRestoredMsg) tea.Cmd {
	m.stopFollow()
	if msg.err != nil {
		m.toasts.Push(components.ToastWarning, "restore view: "+msg.err.Error())
		return m.dispatchSeek()
	}
	v := msg.raw
	if v.SeekMode == SeekLive {
		v.SeekMode = SeekLatest
	}
	if v.Partitions != "" {
		if parts, err := kafka.ParsePartitionFilter(v.Partitions); err == nil {
			alive := make([]int32, 0, len(parts))
			for _, p := range parts {
				if _, ok := msg.watermarks[p]; ok {
					alive = append(alive, p)
				}
			}
			m.filter = alive
		}
	}
	state := SeekState{
		Mode:      v.SeekMode,
		Partition: v.Partition,
		Offset:    v.Offset,
		Timestamp: v.Timestamp,
		HasPart:   v.HasPart,
	}
	switch v.SeekMode {
	case SeekFromOffset, SeekToOffset:
		if v.HasPart {
			if w, ok := msg.watermarks[v.Partition]; ok {
				state.Offset = clampOffset(v.Offset, w.Low, w.High)
			}
		}
	case SeekLatest, SeekEarliest, SeekFromTimestamp, SeekToTimestamp, SeekLive:
		// timestamp clamping happens at fetch time.
	}
	m.seek = state
	return m.dispatchSeek()
}

func (m *Model) Topic() string { return m.topic }

func (m *Model) Action() Action { return m.action }

func (m *Model) ConsumeAction() Action {
	a := m.action
	m.action = Action{}
	return a
}

func (m *Model) CurrentMode() Mode { return m.mode }

func (m *Model) Detail() *DetailModel { return m.detail }

func (m *Model) Following() bool { return m.live }

func (m *Model) SeekState() SeekState { return m.seek }

// FetchGen is exported so tests can forge race-protected messages with
// the right Gen so the handler accepts them.
func (m *Model) FetchGen() uint64 { return m.fetchGen }

func (m *Model) PartitionFilter() []int32 {
	out := make([]int32, len(m.filter))
	copy(out, m.filter)
	return out
}

func (m *Model) Toasts() *components.Toasts { return m.toasts }

func (m *Model) LatestFlash() (components.Toast, bool) {
	if m.toasts == nil {
		return components.Toast{}, false
	}
	return m.toasts.Latest()
}

func (m *Model) Title() string {
	total := len(m.messages)
	body := fmt.Sprintf("Messages · %s [%d]", m.topic, total)
	if q := m.table.Search(); q != "" {
		body = fmt.Sprintf("Messages · %s [%d/%d] </%s>", m.topic, m.table.FilteredCount(), total, q)
	}
	if m.Following() {
		body += " " + liveSpinnerFrame(m.spinnerFrame) + " LIVE"
	}
	if m.loading {
		body += " (loading…)"
	}
	if m.mode == ModeDetail && m.detail != nil {
		body += m.detailTitleSuffix()
	}
	return body
}

func (m *Model) detailTitleSuffix() string {
	out := ""
	if first, last, total, ok := m.detail.ScrollSummary(); ok {
		out += fmt.Sprintf(" · L%d-%d/%d", first, last, total)
	}
	if m.detail.Wrap() {
		out += " · wrap"
	} else {
		out += " · nowrap"
	}
	return out
}

func (m *Model) Breadcrumb() string {
	if m.mode == ModeDetail && m.detail != nil {
		cur := m.detail.Current()
		return formatRowID(cur.Partition, cur.Offset)
	}
	row, ok := m.table.SelectedRow()
	if !ok {
		return ""
	}
	return row.ID
}

func (m *Model) Messages() []kafka.Message {
	out := make([]kafka.Message, len(m.messages))
	copy(out, m.messages)
	return out
}

func (m *Model) SearchAvailable() bool { return m.mode == ModeList }

func (m *Model) SetSearch(query string) {
	if m.mode != ModeList {
		return
	}
	m.table.SetSearch(query)
}

func (m *Model) ActiveFilter() string {
	if m.mode != ModeList {
		return ""
	}
	return m.table.Search()
}

func (m *Model) HasOverlay() bool {
	return m.mode == ModeDetail || m.mode == ModeSeek || m.mode == ModePartitions || m.mode == ModeSmartFilter
}

// popupChromeRows must be kept in sync with renderPartitionsPopup —
// the list area on big topics depends on it for scroll bounds.
const (
	chromeRows      = 8
	popupChromeRows = 12
)

func (m *Model) bodyHeight() int {
	if m.height <= 0 {
		return 0
	}
	return maxInt(1, m.height-chromeRows)
}

func (m *Model) SetSize(w, h int) {
	m.width, m.height = w, h
	if h > 0 {
		m.table.SetHeight(m.bodyHeight())
	}
	if w > 0 {
		m.table.SetTotalWidth(w)
	}
	if m.detail != nil {
		m.detail.SetSize(w, h)
	}
}

func (m *Model) KeyHints() []layout.KeyHint {
	return layout.HintsFromBindings(m.activeBindings())
}

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
	case ModeSeek:
		return m.seekBindings()
	case ModePartitions:
		return m.partitionsBindings()
	case ModeSmartFilter:
		return m.smartFilterBindings()
	}
	return m.listBindings()
}

func (m *Model) listBindings() []keymap.Binding {
	bs := []keymap.Binding{
		{Keys: []string{"enter"}, Label: "open message detail", Category: "Browse", Hint: true, Handler: func() tea.Cmd { m.openDetail(); return nil }},
		{Keys: []string{"["}, Label: "previous page", Category: "Browse", Hint: true, Handler: m.loadEarlier},
		{Keys: []string{"]"}, Label: "next page", Category: "Browse", Hint: true, Handler: m.loadLater},
		{Keys: []string{"r"}, Label: "refresh now", Category: "Browse", Hint: true, Handler: m.refresh},
		{Keys: []string{"esc", "q"}, Label: "back", Category: "Browse", Handler: m.actBack},

		{Keys: []string{"s"}, Label: "seek (offset / time / strategy)", Category: "Filtering", Hint: true, Handler: func() tea.Cmd { m.openSeek(); return nil }},
		{Keys: []string{"P"}, Label: "partition filter", Category: "Filtering", Hint: true, Handler: m.openPartitions},
		{Keys: []string{"f"}, Label: "smart filter (key/value/headers)", Category: "Filtering", Hint: true, Handler: func() tea.Cmd { m.openSmartFilter(); return nil }},
		// advertise-only: `/` is owned by the host.
		{Keys: []string{"/"}, Label: "live filter on visible rows", Category: "Filtering", Hint: true},
	}
	prod := []keymap.Binding{
		{Keys: []string{"p"}, Label: "produce new message", Category: "Produce", Hint: true, Handler: m.handleProduceKey},
		{Keys: []string{"R"}, Label: "resend selected message", Category: "Produce", Hint: true, Handler: func() tea.Cmd { m.handleResendKey(); return nil }},
	}
	if m.readOnly {
		for i := range prod {
			prod[i].Category = ""
			prod[i].Hint = false
		}
	}
	return append(bs, prod...)
}

func (m *Model) actBack() tea.Cmd {
	m.action.Back = true
	return nil
}

// seekBindings: the menu stage delegates to [components.Menu.Bindings]
// so the menu owns its own key set; the input stage dispatches esc/enter
// through this slice.
func (m *Model) seekBindings() []keymap.Binding {
	if m.seekPopup != nil && m.seekPopup.stage == stageInput {
		return []keymap.Binding{
			{Keys: []string{"enter"}, Label: "apply seek", Category: "Seek", Hint: true, Handler: m.actSeekApply},
			{Keys: []string{"esc"}, Label: "back to strategy menu", Category: "Seek", Hint: true, Handler: m.actSeekBackToMenu},
			{Keys: []string{"tab"}, Label: "next form field", Category: "Form"},
		}
	}
	if m.seekPopup != nil && m.seekPopup.menu != nil {
		return m.seekPopup.menu.Bindings("Seek")
	}
	return nil
}

func (m *Model) actSeekApply() tea.Cmd {
	pop := m.seekPopup
	state, err := m.parseSeekForm(pop.chosen, pop.form)
	if err != nil {
		m.toasts.Push(components.ToastError, err.Error())
		return nil
	}
	m.applySeek(state)
	m.closeSeek()
	return m.dispatchSeek()
}

func (m *Model) actSeekBackToMenu() tea.Cmd {
	pop := m.seekPopup
	pop.stage = stageMenu
	pop.form = nil
	pop.menu.Reset()
	return nil
}

func (m *Model) partitionsBindings() []keymap.Binding {
	bs := []keymap.Binding{
		{Keys: []string{"enter"}, Label: "apply partition filter", Category: "Partition filter", Hint: true, Handler: m.actPartApply},
		{Keys: []string{"esc"}, Label: "back", Category: "Partition filter", Hint: true, Handler: m.actPartCancel},
		{Keys: []string{"tab"}, Label: "switch focus (list ↔ input)", Category: "Partition filter", Hint: true, Handler: m.actPartToggleFocus},
	}
	// list-pane keys only fire when the list is focused; text-editing
	// keys for the input pane are universal and aren't surfaced here.
	if m.partitionsPopup != nil && m.partitionsPopup.focus == focusList {
		bs = append(bs,
			keymap.Binding{Keys: []string{"space", " "}, Label: "toggle partition", Category: "Partition filter", Handler: m.actPartToggle},
			keymap.Binding{Keys: []string{"a"}, Label: "toggle all", Category: "Partition filter", Handler: m.actPartToggleAll},
			keymap.Binding{Keys: []string{"up", "k"}, Label: "previous partition", Category: "Partition filter", Handler: m.actPartCursor(-1)},
			keymap.Binding{Keys: []string{"down", "j"}, Label: "next partition", Category: "Partition filter", Handler: m.actPartCursor(+1)},
			keymap.Binding{Keys: []string{"home"}, Label: "first partition", Category: "Partition filter", Handler: m.actPartCursorTo(0)},
			keymap.Binding{Keys: []string{"end"}, Label: "last partition", Category: "Partition filter", Handler: m.actPartCursorTo(-1)},
		)
	}
	return bs
}

func (m *Model) actPartApply() tea.Cmd {
	pop := m.partitionsPopup
	if pop.parseErr != "" {
		m.toasts.Push(components.ToastError, pop.parseErr)
		return nil
	}
	var parts []int32
	if pop.partitions != nil {
		parts = m.selectedPartitions()
		if len(parts) == len(pop.partitions) {
			parts = nil
		}
	} else {
		p, err := kafka.ParsePartitionFilter(pop.input)
		if err != nil {
			m.toasts.Push(components.ToastError, err.Error())
			return nil
		}
		parts = p
	}
	m.filter = parts
	m.partitionsPopup = nil
	m.mode = ModeList
	m.persistView()
	return m.dispatchSeek()
}

func (m *Model) actPartCancel() tea.Cmd {
	m.partitionsPopup = nil
	m.mode = ModeList
	return nil
}

func (m *Model) actPartToggleFocus() tea.Cmd {
	pop := m.partitionsPopup
	if pop.focus == focusList {
		pop.focus = focusInput
	} else {
		pop.focus = focusList
	}
	return nil
}

func (m *Model) actPartToggle() tea.Cmd {
	pop := m.partitionsPopup
	if len(pop.partitions) == 0 {
		return nil
	}
	p := pop.partitions[pop.listCursor]
	if pop.selected[p] {
		delete(pop.selected, p)
	} else {
		pop.selected[p] = true
	}
	m.syncInputFromSelection()
	return nil
}

func (m *Model) actPartToggleAll() tea.Cmd {
	pop := m.partitionsPopup
	if len(pop.partitions) == 0 {
		return nil
	}
	if len(pop.selected) == len(pop.partitions) {
		pop.selected = map[int32]bool{}
	} else {
		for _, p := range pop.partitions {
			pop.selected[p] = true
		}
	}
	m.syncInputFromSelection()
	return nil
}

func (m *Model) actPartCursor(delta int) func() tea.Cmd {
	return func() tea.Cmd {
		pop := m.partitionsPopup
		if len(pop.partitions) == 0 {
			return nil
		}
		n := len(pop.partitions)
		pop.listCursor = (pop.listCursor + delta + n) % n
		return nil
	}
}

func (m *Model) actPartCursorTo(idx int) func() tea.Cmd {
	return func() tea.Cmd {
		pop := m.partitionsPopup
		if len(pop.partitions) == 0 {
			return nil
		}
		if idx < 0 {
			idx = len(pop.partitions) - 1
		}
		pop.listCursor = idx
		return nil
	}
}

func (m *Model) smartFilterBindings() []keymap.Binding {
	return []keymap.Binding{
		{Keys: []string{"esc"}, Label: "close", Category: "Smart filter", Hint: true, Handler: m.actCloseSmartFilter},
	}
}

func (m *Model) actCloseSmartFilter() tea.Cmd {
	m.smartFilterOpen = false
	m.mode = ModeList
	return nil
}

func (m *Model) Update(msg tea.Msg) tea.Cmd {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.SetSize(msg.Width, msg.Height)
		return nil
	case MessagesLoadedMsg:
		m.handleLoaded(msg)
		return nil
	case viewRestoredMsg:
		return m.handleViewRestored(msg)
	case partitionsLoadedMsg:
		m.handlePartitionsLoaded(msg)
		return nil
	case MessagesAppendedMsg:
		m.handleAppended(msg)
		return nil
	case FollowStartedMsg:
		return m.handleFollowStarted(msg)
	case LiveTickMsg:
		if !m.live || msg.Gen != m.fetchGen {
			return nil // tick chain dies when live ends or its dispatch goes stale.
		}
		m.spinnerFrame++
		return liveTickCmd(m.fetchGen)
	case FollowChunkMsg:
		m.handleFollowChunk(msg)
		if msg.Closed {
			return nil
		}
		return m.followPollCmd()
	case FollowErrMsg:
		m.handleFollowErr(msg)
		return nil
	case tea.PasteMsg:
		// only the seek-input popup has a text buffer that can receive paste —
		// list/detail views drop it.
		if m.mode == ModeSeek && m.seekPopup != nil && m.seekPopup.stage == stageInput {
			m.seekPopup.form, _ = m.seekPopup.form.Update(msg)
		}
		return nil
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	}
	return nil
}

func (m *Model) handleKey(key tea.KeyPressMsg) tea.Cmd {
	if m.toasts != nil {
		_, _ = m.toasts.Update(key)
	}
	switch m.mode {
	case ModeList:
		return m.handleListKey(key)
	case ModeDetail:
		return m.handleDetailKey(key)
	case ModeSeek:
		return m.handleSeekKey(key)
	case ModePartitions:
		return m.handlePartitionsKey(key)
	case ModeSmartFilter:
		return m.handleSmartFilterKey(key)
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

func (m *Model) handleProduceKey() tea.Cmd {
	if m.readOnly {
		m.toasts.Push(components.ToastWarning, "cluster is read-only — produce blocked")
		return nil
	}
	m.action.Produce = m.topic
	m.action.PrefillFromMessage = nil
	return nil
}

func (m *Model) handleResendKey() {
	if m.readOnly {
		m.toasts.Push(components.ToastWarning, "cluster is read-only — resend blocked")
		return
	}
	if msg, ok := m.selected(); ok {
		dup := msg
		m.action.Produce = m.topic
		m.action.PrefillFromMessage = &dup
	}
}

func (m *Model) openDetail() {
	idx, ok := m.cursorIndex()
	if !ok {
		return
	}
	m.detail = NewDetailModel(DetailOptions{
		Messages:   m.messages,
		Index:      idx,
		ReadOnly:   m.readOnly,
		Clipboard:  m.clipboard,
		FileWriter: m.writer,
		Pager:      m.pager,
		OutputDir:  m.outputDir,
		Wrap:       m.wrap,
		Now:        m.now,
		Styles:     m.styles,
	})
	m.detail.SetSize(m.width, m.height)
	m.mode = ModeDetail
}

func (m *Model) handleDetailKey(key tea.KeyPressMsg) tea.Cmd {
	d, cmd := m.detail.Update(key)
	m.detail = d
	a := d.ConsumeAction()
	switch {
	case a.Back:
		m.wrap = d.Wrap()
		m.mode = ModeList
		m.detail = nil
	case a.Produce != "":
		m.wrap = d.Wrap()
		m.action.Produce = m.topic
		m.action.PrefillFromMessage = a.PrefillFromMessage
		m.mode = ModeList
		m.detail = nil
	case a.Toast != "":
		m.toasts.Push(components.ToastInfo, a.Toast)
	case a.Warn != "":
		m.toasts.Push(components.ToastWarning, a.Warn)
	}
	return cmd
}

func (m *Model) selected() (kafka.Message, bool) {
	idx, ok := m.cursorIndex()
	if !ok {
		return kafka.Message{}, false
	}
	return m.messages[idx], true
}

func (m *Model) cursorIndex() (int, bool) {
	row, ok := m.table.SelectedRow()
	if !ok {
		return 0, false
	}
	partition, offset, ok := parseRowID(row.ID)
	if !ok {
		return 0, false
	}
	for i, msg := range m.messages {
		if msg.Partition == partition && msg.Offset == offset {
			return i, true
		}
	}
	return 0, false
}

func (m *Model) handleLoaded(msg MessagesLoadedMsg) {
	if msg.Gen != m.fetchGen {
		return
	}
	m.loading = false
	if msg.Err != nil {
		m.toasts.Push(components.ToastError, "load messages: "+msg.Err.Error())
		m.manualRefresh = false
		return
	}
	m.messages = msg.Messages
	if msg.SetBoundary {
		m.fromBoundary = msg.FromBoundary
		m.toBoundary = msg.ToBoundary
	}
	m.refreshTable()
	if m.manualRefresh {
		m.toasts.Push(components.ToastSuccess, fmt.Sprintf(
			"refreshed · %d messages", len(m.messages),
		))
		m.manualRefresh = false
	}
}

func (m *Model) handleAppended(msg MessagesAppendedMsg) {
	if msg.Gen != m.fetchGen {
		return
	}
	m.loading = false
	if msg.Err != nil {
		m.toasts.Push(components.ToastError, "load messages: "+msg.Err.Error())
		return
	}
	if len(msg.Messages) == 0 {
		m.toasts.Push(components.ToastInfo, "no more messages "+msg.Direction)
		return
	}
	if msg.Prepend {
		m.messages = append(msg.Messages, m.messages...)
	} else {
		m.messages = append(m.messages, msg.Messages...)
	}
	m.refreshTable()
}

func (m *Model) handleFollowChunk(msg FollowChunkMsg) {
	if msg.Gen != m.fetchGen {
		return
	}
	if len(msg.Messages) > 0 {
		// follow yields newest records — prepend to keep newest-first ordering.
		m.messages = append(msg.Messages, m.messages...)
		m.refreshTable()
	}
	if msg.Closed {
		m.stopFollow()
	}
}

func (m *Model) handleFollowErr(msg FollowErrMsg) {
	if msg.Gen != m.fetchGen {
		return
	}
	m.toasts.Push(components.ToastError, "follow: "+msg.Err.Error())
	m.stopFollow()
}

func startFollowCmd(svc Service, topic string, parts []int32, gen uint64) tea.Cmd {
	return func() tea.Msg {
		sess, err := svc.Follow(context.Background(), topic, parts)
		return FollowStartedMsg{Session: sess, Gen: gen, Err: err}
	}
}

func (m *Model) handleFollowStarted(msg FollowStartedMsg) tea.Cmd {
	if msg.Gen != m.fetchGen {
		if msg.Session != nil {
			msg.Session.Close()
		}
		return nil
	}
	if msg.Err != nil {
		m.toasts.Push(components.ToastError, "follow: "+msg.Err.Error())
		m.live = false
		return nil
	}
	if !m.live {
		if msg.Session != nil {
			msg.Session.Close()
		}
		return nil
	}
	m.follow = msg.Session
	m.toasts.Push(components.ToastInfo, "live tail on")
	return m.followPollCmd()
}

func (m *Model) stopFollow() {
	if m.follow != nil {
		m.follow.Close()
		m.follow = nil
	}
	m.live = false
	// bump generation so in-flight chunks from the closed session are
	// dropped on arrival.
	m.fetchGen++
	// every load-context switch goes through stopFollow first, so clearing
	// here prevents a stale `r` flag (whose response never arrived) from
	// leaking a misleading "refreshed" toast onto a later dispatchSeek.
	// refresh() re-sets the flag *after* stopFollow.
	m.manualRefresh = false
}

// Close releases background resources before the host swaps screens, so
// an open follow session doesn't leak its kgo consumer / goroutine.
func (m *Model) Close() {
	m.stopFollow()
}

func (m *Model) followPollCmd() tea.Cmd {
	if m.follow == nil {
		return nil
	}
	sess := m.follow
	gen := m.fetchGen
	return func() tea.Msg {
		select {
		case msg, ok := <-sess.Messages:
			if !ok {
				return FollowChunkMsg{Closed: true, Gen: gen}
			}
			batch := []kafka.Message{msg}
			for {
				select {
				case extra, ok := <-sess.Messages:
					if !ok {
						return FollowChunkMsg{Messages: batch, Closed: true, Gen: gen}
					}
					batch = append(batch, extra)
				default:
					return FollowChunkMsg{Messages: batch, Gen: gen}
				}
			}
		case err, ok := <-sess.Errors:
			if !ok {
				return FollowChunkMsg{Closed: true, Gen: gen}
			}
			if err == nil {
				return FollowChunkMsg{Gen: gen}
			}
			return FollowErrMsg{Gen: gen, Err: err}
		}
	}
}

// loadEarlier handles `[`. `from-*` clamps at the captured left edge,
// `live` flips to latest before stepping.
func (m *Model) loadEarlier() tea.Cmd {
	if m.seek.Mode == SeekLive {
		m.toasts.Push(components.ToastInfo, "paused live to step — back to latest")
		m.stopFollow()
		m.seek = SeekState{Mode: SeekLatest}
		return m.dispatchSeek()
	}
	if len(m.messages) == 0 {
		return nil
	}
	if atFromBoundary(m.messages, m.fromBoundary) {
		m.toasts.Push(components.ToastInfo, "start of seek window")
		return nil
	}
	baseline := lowestOffsets(m.messages)
	m.loading = true
	return loadEarlierCmd(m.svc, m.topic, baseline, m.pageSize, partitionsFromBaseline(baseline), m.fetchGen)
}

func (m *Model) loadLater() tea.Cmd {
	if m.seek.Mode == SeekLive {
		m.toasts.Push(components.ToastInfo, "paused live to step — back to latest")
		m.stopFollow()
		m.seek = SeekState{Mode: SeekLatest}
		return m.dispatchSeek()
	}
	if m.seek.Mode == SeekToOffset || m.seek.Mode == SeekToTimestamp {
		if atToBoundary(m.messages, m.toBoundary) {
			m.toasts.Push(components.ToastInfo, "end of seek window")
			return nil
		}
	}
	if len(m.messages) == 0 {
		return nil
	}
	baseline := highestOffsets(m.messages)
	m.loading = true
	return loadLaterCmd(m.svc, m.topic, baseline, m.pageSize, partitionsFromBaseline(baseline), m.fetchGen)
}

// partitionsFromBaseline restricts paging to partitions the user has
// already seen — without this an explicit `from offset 3:500` would
// start showing tails of partitions 0, 1, 2... on the next `[`/`]`
// because the kafka layer falls back to watermark-loading otherwise.
func partitionsFromBaseline(baseline map[int32]int64) []int32 {
	out := make([]int32, 0, len(baseline))
	for p := range baseline {
		out = append(out, p)
	}
	slices.Sort(out)
	return out
}

// ----- seek popup -----

type seekStage int

const (
	stageMenu seekStage = iota
	stageInput
)

type seekPopup struct {
	stage  seekStage
	chosen SeekMode
	menu   *components.Menu
	form   *components.Form
}

func (m *Model) openSeek() {
	cursor := int(m.seek.Mode)
	items := []components.MenuItem{
		{Label: "latest"},
		{Label: "earliest"},
		{Label: "from offset"},
		{Label: "to offset"},
		{Label: "from timestamp"},
		{Label: "to timestamp"},
		{Label: "live"},
	}
	menu := components.NewMenu(items,
		components.WithMenuStyles(m.styles),
		components.WithMenuTitle("seek"),
		components.WithMenuCursor(cursor),
	)
	m.seekPopup = &seekPopup{stage: stageMenu, menu: menu}
	m.mode = ModeSeek
}

func (m *Model) handleSeekKey(key tea.KeyPressMsg) tea.Cmd {
	if m.seekPopup == nil {
		m.mode = ModeList
		return nil
	}
	if m.seekPopup.stage == stageInput {
		return m.handleSeekInput(key)
	}
	// stage 1: menu
	pop := m.seekPopup
	pop.menu, _ = pop.menu.Update(key)
	if pop.menu.Canceled() {
		m.closeSeek()
		return nil
	}
	if idx, _, ok := pop.menu.Selected(); ok {
		mode := SeekMode(idx)
		pop.chosen = mode
		switch mode {
		case SeekLatest, SeekEarliest, SeekLive:
			m.applySeek(SeekState{Mode: mode})
			m.closeSeek()
			return m.dispatchSeek()
		default:
			pop.stage = stageInput
			pop.form = m.buildSeekForm(mode)
		}
	}
	return nil
}

func (m *Model) handleSeekInput(key tea.KeyPressMsg) tea.Cmd {
	pop := m.seekPopup
	if cmd, ok := keymap.Dispatch(m.seekBindings(), key); ok {
		return cmd
	}
	pop.form, _ = pop.form.Update(key)
	return nil
}

func (m *Model) closeSeek() {
	m.seekPopup = nil
	m.mode = ModeList
}

func (m *Model) buildSeekForm(mode SeekMode) *components.Form {
	var label, prefill string
	switch mode {
	case SeekFromOffset, SeekToOffset:
		label = "offset (partition:offset or offset)"
		if msg, ok := m.selected(); ok {
			prefill = strconv.FormatInt(int64(msg.Partition), 10) + ":" + strconv.FormatInt(msg.Offset, 10)
		}
	case SeekFromTimestamp, SeekToTimestamp:
		label = "timestamp (RFC3339, '1h ago', 'today', …)"
		if msg, ok := m.selected(); ok && !msg.Timestamp.IsZero() {
			prefill = msg.Timestamp.UTC().Format(time.RFC3339)
		}
	case SeekLatest, SeekEarliest, SeekLive:
	}
	return components.NewForm(
		[]components.Field{{Key: "value", Label: label, Kind: components.FieldText, Value: prefill}},
		components.WithFormStyles(m.styles),
	)
}

func (m *Model) parseSeekForm(mode SeekMode, form *components.Form) (SeekState, error) {
	fld, _ := form.Field("value")
	raw := strings.TrimSpace(fld.Value)
	switch mode {
	case SeekFromOffset, SeekToOffset:
		p, off, hasPart, err := parseOffsetExpression(raw)
		if err != nil {
			return SeekState{}, err
		}
		return SeekState{Mode: mode, Partition: p, Offset: off, HasPart: hasPart}, nil
	case SeekFromTimestamp, SeekToTimestamp:
		ts, err := kafka.ParseTimestamp(raw, m.now())
		if err != nil {
			return SeekState{}, fmt.Errorf("invalid timestamp: %w", err)
		}
		return SeekState{Mode: mode, Timestamp: ts}, nil
	case SeekLatest, SeekEarliest, SeekLive:
	}
	return SeekState{Mode: mode}, nil
}

// parseOffsetExpression accepts `partition:offset` or `offset`.
func parseOffsetExpression(s string) (int32, int64, bool, error) {
	if s == "" {
		return 0, 0, false, errors.New("invalid offset: expected partition:offset or offset")
	}
	if strings.Contains(s, ":") {
		parts := strings.SplitN(s, ":", 2)
		p, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 32)
		if err != nil || p < 0 {
			return 0, 0, false, fmt.Errorf("invalid offset: bad partition %q", parts[0])
		}
		off, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil || off < 0 {
			return 0, 0, false, fmt.Errorf("invalid offset: bad offset %q", parts[1])
		}
		return int32(p), off, true, nil
	}
	off, err := strconv.ParseInt(s, 10, 64)
	if err != nil || off < 0 {
		return 0, 0, false, errors.New("invalid offset: expected partition:offset or offset")
	}
	return 0, off, false, nil
}

func (m *Model) applySeek(state SeekState) {
	m.stopFollow()
	m.seek = state
	m.fromBoundary = nil
	m.toBoundary = nil
	m.persistView()
}

// refresh re-issues the current seek. stopFollow first so refreshing while
// live doesn't leak the previous session's goroutine/broker connection.
// Live-mode toasts are surfaced immediately (no handleLoaded path);
// non-live mode defers the success toast to handleLoaded for the count.
func (m *Model) refresh() tea.Cmd {
	// capture live state before stopFollow flips m.live.
	wasLive := m.live
	m.stopFollow()
	if wasLive || m.seek.Mode == SeekLive {
		m.toasts.Push(components.ToastInfo, "restarting live tail…")
	} else {
		m.manualRefresh = true
	}
	return m.dispatchSeek()
}

// dispatchSeek bumps fetchGen so any in-flight fetch from the previous
// view is dropped, clears the table so stale records don't linger during
// the new fetch, then dispatches the command for the active seek state.
func (m *Model) dispatchSeek() tea.Cmd {
	m.fetchGen++
	gen := m.fetchGen
	m.messages = nil
	m.refreshTable()
	switch m.seek.Mode {
	case SeekLatest:
		m.loading = true
		return loadLastNCmd(m.svc, m.topic, m.pageSize, m.filter, gen)
	case SeekEarliest:
		m.loading = true
		return loadEarliestCmd(m.svc, m.topic, m.pageSize, m.filter, gen)
	case SeekFromOffset:
		return m.dispatchFromOffset(gen)
	case SeekToOffset:
		return m.dispatchToOffset(gen)
	case SeekFromTimestamp:
		return m.dispatchFromTimestamp(gen)
	case SeekToTimestamp:
		return m.dispatchToTimestamp(gen)
	case SeekLive:
		// no historical fetch — stream only new records, matching
		// kafbat-ui / AKHQ / kafka-console-consumer semantics.
		m.live = true
		return tea.Batch(
			startFollowCmd(m.svc, m.topic, m.filter, gen),
			liveTickCmd(gen),
		)
	}
	return nil
}

func (m *Model) dispatchFromOffset(gen uint64) tea.Cmd {
	if m.seek.HasPart {
		svc := m.svc
		topic := m.topic
		partition := m.seek.Partition
		offset := m.seek.Offset
		pageSize := m.pageSize
		boundary := map[int32]int64{partition: offset}
		m.loading = true
		return func() tea.Msg {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			msgs, err := svc.FetchAtOffset(ctx, topic, partition, offset, pageSize)
			return MessagesLoadedMsg{Messages: msgs, FromBoundary: boundary, SetBoundary: true, Gen: gen, Err: err}
		}
	}
	return m.dispatchOffsetClampedForward(gen)
}

// dispatchFromTimestamp captures per-partition starting offsets for `[`
// boundary checks, then forward-loads.
func (m *Model) dispatchFromTimestamp(gen uint64) tea.Cmd {
	svc := m.svc
	topic := m.topic
	ts := m.seek.Timestamp
	pageSize := m.pageSize
	parts := append([]int32(nil), m.filter...)
	m.loading = true
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		offsets, err := svc.OffsetsForTimestamp(ctx, topic, ts, parts)
		if err != nil {
			return MessagesLoadedMsg{Gen: gen, Err: err}
		}
		boundary := map[int32]int64{}
		fetch := map[int32]int64{}
		for p, o := range offsets {
			boundary[p] = o
			fetch[p] = o
		}
		per := perPartShare(pageSize, len(fetch))
		msgs, err := svc.FetchAtOffsets(ctx, topic, fetch, per)
		return MessagesLoadedMsg{Messages: msgs, FromBoundary: boundary, SetBoundary: true, Gen: gen, Err: err}
	}
}

func (m *Model) dispatchOffsetClampedForward(gen uint64) tea.Cmd {
	svc := m.svc
	topic := m.topic
	off := m.seek.Offset
	pageSize := m.pageSize
	parts := append([]int32(nil), m.filter...)
	m.loading = true
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		// FetchAtOffsets clamps internally; no upfront WatermarksFor
		// needed when the partition set is known. Without a filter we
		// still need the partition list — fall back to watermarks.
		offsets := map[int32]int64{}
		if len(parts) > 0 {
			for _, p := range parts {
				offsets[p] = off
			}
		} else {
			wm, err := svc.WatermarksFor(ctx, topic, nil)
			if err != nil {
				return MessagesLoadedMsg{Gen: gen, Err: err}
			}
			for p := range wm {
				offsets[p] = off
			}
		}
		per := perPartShare(pageSize, len(offsets))
		msgs, err := svc.FetchAtOffsets(ctx, topic, offsets, per)
		return MessagesLoadedMsg{Messages: msgs, FromBoundary: maps.Clone(offsets), SetBoundary: true, Gen: gen, Err: err}
	}
}

func (m *Model) dispatchToOffset(gen uint64) tea.Cmd {
	svc := m.svc
	topic := m.topic
	pageSize := m.pageSize
	parts := append([]int32(nil), m.filter...)
	m.loading = true
	if m.seek.HasPart {
		boundary := map[int32]int64{m.seek.Partition: m.seek.Offset + 1}
		partition := m.seek.Partition
		return func() tea.Msg {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			msgs, err := svc.FetchEarlier(ctx, topic, boundary, pageSize, []int32{partition})
			return MessagesLoadedMsg{Messages: msgs, ToBoundary: boundary, SetBoundary: true, Gen: gen, Err: err}
		}
	}
	off := m.seek.Offset
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		wm, err := svc.WatermarksFor(ctx, topic, parts)
		if err != nil {
			return MessagesLoadedMsg{Gen: gen, Err: err}
		}
		baseline := map[int32]int64{}
		for p, w := range wm {
			clamped := clampOffset(off, w.Low, w.High)
			baseline[p] = clamped + 1
		}
		var pSlice []int32
		for p := range baseline {
			pSlice = append(pSlice, p)
		}
		msgs, err := svc.FetchEarlier(ctx, topic, baseline, pageSize, pSlice)
		return MessagesLoadedMsg{Messages: msgs, ToBoundary: baseline, SetBoundary: true, Gen: gen, Err: err}
	}
}

func (m *Model) dispatchToTimestamp(gen uint64) tea.Cmd {
	svc := m.svc
	topic := m.topic
	pageSize := m.pageSize
	parts := append([]int32(nil), m.filter...)
	ts := m.seek.Timestamp
	m.loading = true
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		offsets, err := svc.OffsetsForTimestamp(ctx, topic, ts, parts)
		if err != nil {
			return MessagesLoadedMsg{Gen: gen, Err: err}
		}
		baseline := map[int32]int64{}
		var pSlice []int32
		for p, o := range offsets {
			baseline[p] = o + 1
			pSlice = append(pSlice, p)
		}
		msgs, err := svc.FetchEarlier(ctx, topic, baseline, pageSize, pSlice)
		return MessagesLoadedMsg{Messages: msgs, ToBoundary: baseline, SetBoundary: true, Gen: gen, Err: err}
	}
}

// ----- partitions popup -----

type partitionsFocus int

const (
	focusList partitionsFocus = iota
	focusInput
)

// partitionsPopup keeps checkbox list and input in sync — toggling a
// checkbox rewrites the input; valid edits re-tick checkboxes.
type partitionsPopup struct {
	loading      bool
	loadErr      string
	partitions   []int32
	selected     map[int32]bool
	listCursor   int
	listScroll   int
	focus        partitionsFocus
	input        string
	inputCursor  int
	parseErr     string
	allDiscarded bool // parsed ok but referenced unknown partitions
}

type partitionsLoadedMsg struct {
	partitions []int32
	err        error
}

func loadPartitionsCmd(svc Service, topic string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		wm, err := svc.WatermarksFor(ctx, topic, nil)
		if err != nil {
			return partitionsLoadedMsg{err: err}
		}
		out := make([]int32, 0, len(wm))
		for p := range wm {
			out = append(out, p)
		}
		slices.Sort(out)
		return partitionsLoadedMsg{partitions: out}
	}
}

func (m *Model) openPartitions() tea.Cmd {
	m.partitionsPopup = &partitionsPopup{
		loading:  true,
		selected: map[int32]bool{},
		input:    renderPartitionFilter(m.filter),
		focus:    focusList,
	}
	m.partitionsPopup.inputCursor = runeLen(m.partitionsPopup.input)
	m.mode = ModePartitions
	return loadPartitionsCmd(m.svc, m.topic)
}

func (m *Model) handlePartitionsLoaded(msg partitionsLoadedMsg) {
	if m.partitionsPopup == nil {
		return
	}
	pop := m.partitionsPopup
	pop.loading = false
	if msg.err != nil {
		pop.loadErr = msg.err.Error()
		return
	}
	pop.partitions = msg.partitions
	pop.selected = map[int32]bool{}
	if len(m.filter) == 0 {
		for _, p := range pop.partitions {
			pop.selected[p] = true
		}
	} else {
		want := map[int32]bool{}
		for _, p := range m.filter {
			want[p] = true
		}
		for _, p := range pop.partitions {
			if want[p] {
				pop.selected[p] = true
			}
		}
	}
	pop.input = m.canonicalSelection()
	pop.inputCursor = runeLen(pop.input)
}

func (m *Model) handlePartitionsKey(key tea.KeyPressMsg) tea.Cmd {
	if m.partitionsPopup == nil {
		m.mode = ModeList
		return nil
	}
	if cmd, ok := keymap.Dispatch(m.partitionsBindings(), key); ok {
		return cmd
	}
	if m.partitionsPopup.focus == focusInput {
		m.handlePartitionsInputKey(key)
	}
	return nil
}

func (m *Model) handlePartitionsInputKey(key tea.KeyPressMsg) {
	pop := m.partitionsPopup
	runes := []rune(pop.input)
	if pop.inputCursor > len(runes) {
		pop.inputCursor = len(runes)
	}
	switch key.String() {
	case "left":
		if pop.inputCursor > 0 {
			pop.inputCursor--
		}
	case "right":
		if pop.inputCursor < len(runes) {
			pop.inputCursor++
		}
	case "home":
		pop.inputCursor = 0
	case "end":
		pop.inputCursor = len(runes)
	case "backspace":
		if pop.inputCursor > 0 {
			pop.input = string(runes[:pop.inputCursor-1]) + string(runes[pop.inputCursor:])
			pop.inputCursor--
			m.syncSelectionFromInput()
		}
	case "delete":
		if pop.inputCursor < len(runes) {
			pop.input = string(runes[:pop.inputCursor]) + string(runes[pop.inputCursor+1:])
			m.syncSelectionFromInput()
		}
	default:
		if t := key.Text; t != "" {
			pop.input = string(runes[:pop.inputCursor]) + t + string(runes[pop.inputCursor:])
			pop.inputCursor += len([]rune(t))
			m.syncSelectionFromInput()
		}
	}
}

func (m *Model) selectedPartitions() []int32 {
	pop := m.partitionsPopup
	out := make([]int32, 0, len(pop.selected))
	for _, p := range pop.partitions {
		if pop.selected[p] {
			out = append(out, p)
		}
	}
	return out
}

// canonicalSelection: "all ticked" / "none ticked" both emit "" to match
// the "all partitions" convention.
func (m *Model) canonicalSelection() string {
	pop := m.partitionsPopup
	if len(pop.partitions) == 0 {
		return ""
	}
	picks := m.selectedPartitions()
	if len(picks) == len(pop.partitions) {
		return ""
	}
	return renderPartitionFilter(picks)
}

func (m *Model) syncInputFromSelection() {
	pop := m.partitionsPopup
	pop.input = m.canonicalSelection()
	pop.inputCursor = runeLen(pop.input)
	pop.parseErr = ""
	pop.allDiscarded = false
}

// syncSelectionFromInput keeps checkbox state stable on invalid input.
// References to unknown partitions are a soft warning (allDiscarded), not
// a block — the kafka layer silently drops them on fetch.
func (m *Model) syncSelectionFromInput() {
	pop := m.partitionsPopup
	if pop.partitions == nil {
		// metadata not yet loaded — validate syntax only.
		_, err := kafka.ParsePartitionFilter(pop.input)
		if err != nil {
			pop.parseErr = err.Error()
		} else {
			pop.parseErr = ""
		}
		return
	}
	parts, err := kafka.ParsePartitionFilter(pop.input)
	if err != nil {
		pop.parseErr = err.Error()
		return
	}
	pop.parseErr = ""
	pop.allDiscarded = false
	known := map[int32]bool{}
	for _, p := range pop.partitions {
		known[p] = true
	}
	pop.selected = map[int32]bool{}
	if len(parts) == 0 {
		for _, p := range pop.partitions {
			pop.selected[p] = true
		}
		return
	}
	unknownCount := 0
	for _, p := range parts {
		if known[p] {
			pop.selected[p] = true
		} else {
			unknownCount++
		}
	}
	if unknownCount > 0 && len(pop.selected) == 0 {
		pop.allDiscarded = true
	}
}

// ----- smart filter stub -----

const smartFilterDescription = `Smart filter — coming soon.

Will scan the entire topic (within current seek + partition scope)
applying a predicate over record.key, record.value, record.headers,
record.partition, record.offset, record.timestamp. Boolean operators
and string methods are supported. Results stream into the table as
matches are found.`

func (m *Model) openSmartFilter() {
	m.smartFilterOpen = true
	m.mode = ModeSmartFilter
}

func (m *Model) handleSmartFilterKey(key tea.KeyPressMsg) tea.Cmd {
	if key.String() == "esc" {
		m.smartFilterOpen = false
		m.mode = ModeList
	}
	return nil
}

func (m *Model) renderSmartFilter() string {
	title := m.styles.HelpTitle.Render("smart filter")
	body := m.styles.Command.Render(smartFilterDescription)
	hint := m.styles.HintLabel.Render("esc close")
	content := title + "\n\n" + body + "\n\n" + hint
	return lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		Padding(0, 2).
		Render(content)
}

// ----- persistence -----

func (m *Model) persistView() {
	if m.repo == nil || m.cluster == "" || m.topic == "" {
		return
	}
	// live mode is intentionally not persisted (see [ViewState]).
	if m.seek.Mode == SeekLive {
		return
	}
	view := ViewState{
		SeekMode:   m.seek.Mode,
		Partition:  m.seek.Partition,
		Offset:     m.seek.Offset,
		Timestamp:  m.seek.Timestamp,
		HasPart:    m.seek.HasPart,
		Partitions: renderPartitionFilter(m.filter),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.repo.SaveMessagesView(ctx, m.cluster, m.topic, view); err != nil {
		m.toasts.Push(components.ToastWarning, "save view state: "+err.Error())
	}
}

// ----- table refresh & rendering -----

func (m *Model) refreshTable() {
	rows := make([]components.Row, 0, len(m.messages))
	for _, msg := range m.messages {
		rows = append(rows, components.Row{
			ID:     formatRowID(msg.Partition, msg.Offset),
			Values: m.rowValues(msg),
		})
	}
	m.table.SetRows(rows)
}

func (m *Model) rowValues(msg kafka.Message) []string {
	out := make([]string, 0, len(m.columns))
	for _, col := range m.columns {
		out = append(out, m.cellFor(col, msg))
	}
	return out
}

func (m *Model) cellFor(col string, msg kafka.Message) string {
	switch col {
	case "timestamp":
		return FormatTimestamp(msg.Timestamp)
	case "partition":
		return strconv.FormatInt(int64(msg.Partition), 10)
	case "offset":
		return strconv.FormatInt(msg.Offset, 10)
	case "key":
		return PreviewLine(msg.Key, 32)
	case "value":
		return PreviewLine(msg.Value, valuePreviewWidth(m.width))
	case "headers":
		return headersPreview(msg.Headers)
	default:
		return ""
	}
}

func headersPreview(headers []kafka.Header) string {
	if len(headers) == 0 {
		return "—"
	}
	parts := make([]string, 0, len(headers))
	for _, h := range headers {
		parts = append(parts, h.Key+"="+strconv.Quote(string(h.Value)))
	}
	return strings.Join(parts, ", ")
}

func valuePreviewWidth(termWidth int) int {
	if termWidth <= 0 {
		return 60
	}
	if termWidth < 40 {
		return 20
	}
	return termWidth
}

func (m *Model) View() string {
	if m.mode == ModeDetail {
		return m.detail.View()
	}
	header := m.renderStateHeader()
	switch m.mode {
	case ModeList, ModeDetail:
	case ModeSeek:
		if m.seekPopup != nil {
			return header + "\n" + m.placePopupInBody(m.renderSeekPopup())
		}
	case ModePartitions:
		if m.partitionsPopup != nil {
			return header + "\n" + m.placePopupInBody(m.renderPartitionsPopup())
		}
	case ModeSmartFilter:
		if m.smartFilterOpen {
			return header + "\n" + m.placePopupInBody(m.renderSmartFilter())
		}
	}
	if m.live && len(m.messages) == 0 {
		return header + "\n" + m.placeWaitingForLive()
	}
	return header + "\n" + m.table.View()
}

func (m *Model) placeWaitingForLive() string {
	hint := m.styles.HintLabel.Render(liveSpinnerFrame(m.spinnerFrame) + " waiting for new records…")
	if m.width <= 0 {
		return hint
	}
	centered := lipgloss.PlaceHorizontal(m.width, lipgloss.Center, hint)
	h := m.bodyHeight()
	if h <= 0 {
		return centered
	}
	return lipgloss.PlaceVertical(h, lipgloss.Center, centered)
}

func (m *Model) renderStateHeader() string {
	parts := []string{
		"seek: " + m.describeSeek(),
		"partitions: " + describePartitions(m.filter),
		"smart filter: —",
	}
	line := strings.Join(parts, "  •  ")
	if m.width > 0 && lipgloss.Width(line) > m.width {
		line = ansiTrunc(line, m.width)
	}
	return m.styles.HintLabel.Render(line)
}

func (m *Model) describeSeek() string {
	switch m.seek.Mode {
	case SeekLatest, SeekEarliest, SeekLive:
		return m.seek.Mode.String()
	case SeekFromOffset, SeekToOffset:
		if m.seek.HasPart {
			return m.seek.Mode.String() + " " + strconv.FormatInt(int64(m.seek.Partition), 10) + ":" + strconv.FormatInt(m.seek.Offset, 10)
		}
		return m.seek.Mode.String() + " " + strconv.FormatInt(m.seek.Offset, 10)
	case SeekFromTimestamp, SeekToTimestamp:
		return m.seek.Mode.String() + " " + m.seek.Timestamp.UTC().Format(time.RFC3339)
	}
	return m.seek.Mode.String()
}

func describePartitions(filter []int32) string {
	if len(filter) == 0 {
		return "all"
	}
	return renderPartitionFilter(filter)
}

func ansiTrunc(s string, width int) string {
	if width <= 1 {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= width {
		return s
	}
	return string(runes[:width-1]) + "…"
}

func (m *Model) renderSeekPopup() string {
	if m.seekPopup.stage == stageMenu {
		return m.seekPopup.menu.View(0)
	}
	title := m.styles.HelpTitle.Render("seek · " + m.seekPopup.chosen.String())
	hint := m.styles.HintLabel.Render("enter ok   esc back")
	body := title + "\n\n" + m.seekPopup.form.View() + "\n\n" + hint
	return lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		Padding(0, 2).
		Render(body)
}

// placePopupInBody anchors the popup to the top of the table area so it
// sits below the state-header line; the reserved height matches
// table.SetHeight to keep chrome stable across mode switches.
func (m *Model) placePopupInBody(popup string) string {
	if m.width <= 0 {
		return popup
	}
	centered := lipgloss.PlaceHorizontal(m.width, lipgloss.Center, popup)
	h := m.bodyHeight()
	if h <= 0 {
		return centered
	}
	return lipgloss.PlaceVertical(h, lipgloss.Top, centered)
}

func (m *Model) renderPartitionsPopup() string {
	pop := m.partitionsPopup
	title := m.styles.HelpTitle.Render("partition filter")

	var listBlock string
	switch {
	case pop.loading:
		listBlock = "    " + m.styles.HintLabel.Render("loading partitions…")
	case pop.loadErr != "":
		listBlock = "    " + m.styles.StatusErr.Render("load failed: "+pop.loadErr)
	case len(pop.partitions) == 0:
		listBlock = "    " + m.styles.HintLabel.Render("(topic has no partitions)")
	default:
		maxRows := m.partitionsListWindow()
		m.clampPartitionsScroll(maxRows)
		first := pop.listScroll
		last := min(first+maxRows, len(pop.partitions))
		rows := make([]string, 0, last-first+2)
		if first > 0 {
			rows = append(rows, "    "+m.styles.HintLabel.Render(fmt.Sprintf("↑ %d more", first)))
		}
		for i := first; i < last; i++ {
			p := pop.partitions[i]
			marker := "[ ]"
			if pop.selected[p] {
				marker = "[×]"
			}
			prefix := "  "
			rowStyle := m.styles.Command
			if pop.focus == focusList && i == pop.listCursor {
				prefix = "▸ "
				rowStyle = m.styles.CommandHL
			}
			rows = append(rows, prefix+rowStyle.Render(fmt.Sprintf("%s %d", marker, p)))
		}
		if last < len(pop.partitions) {
			rows = append(rows, "    "+m.styles.HintLabel.Render(fmt.Sprintf("↓ %d more", len(pop.partitions)-last)))
		}
		listBlock = strings.Join(rows, "\n")
	}

	var listLabel string
	if pop.focus == focusList {
		listLabel = m.styles.HintKey.Render("▸ partitions")
	} else {
		listLabel = m.styles.HintLabel.Render("  partitions")
	}

	var inputLabel string
	if pop.focus == focusInput {
		inputLabel = m.styles.HintKey.Render("▸ filter")
	} else {
		inputLabel = m.styles.HintLabel.Render("  filter")
	}
	inputBody := m.renderPartitionsInputField()
	var inputErr string
	switch {
	case pop.parseErr != "":
		inputErr = "    " + m.styles.StatusErr.Render("invalid: "+pop.parseErr)
	case pop.allDiscarded:
		inputErr = "    " + m.styles.StatusWarn.Render("none of the listed partitions exist in this topic")
	}

	hint := m.styles.HintLabel.Render("tab switch   space toggle   a all/none   enter apply   esc back")

	parts := []string{
		title,
		"",
		listLabel,
		listBlock,
		"",
		inputLabel,
		inputBody,
	}
	if inputErr != "" {
		parts = append(parts, inputErr)
	}
	parts = append(parts, "", hint)
	return lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		Padding(0, 2).
		Render(strings.Join(parts, "\n"))
}

func (m *Model) partitionsListWindow() int {
	avail := m.bodyHeight() - popupChromeRows
	if avail < 3 {
		return 3
	}
	return avail
}

func (m *Model) clampPartitionsScroll(window int) {
	pop := m.partitionsPopup
	if window <= 0 || len(pop.partitions) == 0 {
		pop.listScroll = 0
		return
	}
	if pop.listCursor < pop.listScroll {
		pop.listScroll = pop.listCursor
	}
	if pop.listCursor >= pop.listScroll+window {
		pop.listScroll = pop.listCursor - window + 1
	}
	maxScroll := max(len(pop.partitions)-window, 0)
	if pop.listScroll > maxScroll {
		pop.listScroll = maxScroll
	}
	if pop.listScroll < 0 {
		pop.listScroll = 0
	}
}

func (m *Model) renderPartitionsInputField() string {
	pop := m.partitionsPopup
	if pop.focus != focusInput {
		if pop.input == "" {
			return "    " + m.styles.HintLabel.Render("(empty = all)")
		}
		return "    " + m.styles.Command.Render(pop.input)
	}
	runes := []rune(pop.input)
	cur := min(pop.inputCursor, len(runes))
	before := string(runes[:cur])
	var underCursor, after string
	if cur >= len(runes) {
		underCursor = " "
	} else {
		underCursor = string(runes[cur])
		after = string(runes[cur+1:])
	}
	return "    " + m.styles.Command.Render(before) + m.styles.Cursor.Render(underCursor) + m.styles.Command.Render(after)
}

// FormatTimestamp renders `YYYY-MM-DD HH:MM:SS.mmm` in the local timezone.
func FormatTimestamp(ts time.Time) string {
	if ts.IsZero() {
		return "—"
	}
	return ts.Local().Format("2006-01-02 15:04:05.000")
}

// ----- column helpers -----

func buildColumns(keys []string) []components.Column {
	out := make([]components.Column, 0, len(keys))
	for _, k := range keys {
		out = append(out, columnSpec(k))
	}
	return out
}

func columnSpec(key string) components.Column {
	switch key {
	case "timestamp":
		return components.Column{Title: "Timestamp", Width: 23, Sortable: true}
	case "partition":
		return components.Column{Title: "Partition", Width: 9, Sortable: true}
	case "offset":
		return components.Column{Title: "Offset", Width: 10, Sortable: true}
	case "key":
		return components.Column{Title: "Key", Width: 32, Sortable: true}
	case "value":
		return components.Column{Title: "Value", Flex: true, MinWidth: 20, Sortable: false}
	case "headers":
		return components.Column{Title: "Headers", Width: 28, Sortable: false}
	default:
		return components.Column{Title: key, Width: 10}
	}
}

func formatRowID(partition int32, offset int64) string {
	return "msg-" + strconv.FormatInt(int64(partition), 10) + "-" + strconv.FormatInt(offset, 10)
}

func parseRowID(id string) (int32, int64, bool) {
	const prefix = "msg-"
	if !strings.HasPrefix(id, prefix) {
		return 0, 0, false
	}
	rest := id[len(prefix):]
	dash := strings.IndexByte(rest, '-')
	if dash <= 0 {
		return 0, 0, false
	}
	p, err := strconv.ParseInt(rest[:dash], 10, 32)
	if err != nil {
		return 0, 0, false
	}
	o, err := strconv.ParseInt(rest[dash+1:], 10, 64)
	if err != nil || o < 0 {
		return 0, 0, false
	}
	return int32(p), o, true
}

func lowestOffsets(msgs []kafka.Message) map[int32]int64 {
	out := map[int32]int64{}
	for _, m := range msgs {
		if cur, ok := out[m.Partition]; !ok || m.Offset < cur {
			out[m.Partition] = m.Offset
		}
	}
	return out
}

func highestOffsets(msgs []kafka.Message) map[int32]int64 {
	out := map[int32]int64{}
	for _, m := range msgs {
		if cur, ok := out[m.Partition]; !ok || m.Offset > cur {
			out[m.Partition] = m.Offset
		}
	}
	return out
}

func atToBoundary(msgs []kafka.Message, boundary map[int32]int64) bool {
	if len(boundary) == 0 || len(msgs) == 0 {
		return false
	}
	high := highestOffsets(msgs)
	for p, b := range boundary {
		if h, ok := high[p]; ok && h >= b-1 {
			return true
		}
	}
	return false
}

func atFromBoundary(msgs []kafka.Message, boundary map[int32]int64) bool {
	if len(boundary) == 0 || len(msgs) == 0 {
		return false
	}
	low := lowestOffsets(msgs)
	for p, b := range boundary {
		if l, ok := low[p]; ok && l <= b {
			return true
		}
	}
	return false
}

// renderPartitionFilter inverts ParsePartitionFilter.
func renderPartitionFilter(parts []int32) string {
	if len(parts) == 0 {
		return ""
	}
	var b strings.Builder
	rangeStart := parts[0]
	prev := parts[0]
	flush := func(start, end int32) {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		if start == end {
			b.WriteString(strconv.FormatInt(int64(start), 10))
			return
		}
		b.WriteString(strconv.FormatInt(int64(start), 10))
		b.WriteByte('-')
		b.WriteString(strconv.FormatInt(int64(end), 10))
	}
	for i := 1; i < len(parts); i++ {
		if parts[i] == prev+1 {
			prev = parts[i]
			continue
		}
		flush(rangeStart, prev)
		rangeStart = parts[i]
		prev = parts[i]
	}
	flush(rangeStart, prev)
	return b.String()
}

func clampOffset(want, low, high int64) int64 {
	if want < low {
		return low
	}
	if want >= high {
		return high - 1
	}
	return want
}

func perPartShare(total, parts int) int {
	if parts <= 0 {
		return 0
	}
	return (total + parts - 1) / parts
}

func runeLen(s string) int { return len([]rune(s)) }

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ----- Messages -----

// MessagesLoadedMsg replaces the current window. Gen pins the message to
// the [Model.fetchGen] at dispatch time so handlers drop stale arrivals.
// SetBoundary flips the handler from "keep existing edges" to "replace".
type MessagesLoadedMsg struct {
	Messages     []kafka.Message
	FromBoundary map[int32]int64
	ToBoundary   map[int32]int64
	SetBoundary  bool
	Gen          uint64
	Err          error
}

type MessagesAppendedMsg struct {
	Messages  []kafka.Message
	Prepend   bool
	Direction string
	Gen       uint64
	Err       error
}

// FollowStartedMsg.Session is non-nil iff Err is nil.
type FollowStartedMsg struct {
	Session *kafka.FollowSession
	Gen     uint64
	Err     error
}

// FollowChunkMsg.Closed is true when the underlying session terminated.
type FollowChunkMsg struct {
	Messages []kafka.Message
	Closed   bool
	Gen      uint64
}

type FollowErrMsg struct {
	Gen uint64
	Err error
}

// LiveTickMsg.Gen pins the tick to its starter dispatch so a stale tick
// chain from a previous live session can't merge with a fresh one and
// multiply the spinner rate.
type LiveTickMsg struct{ Gen uint64 }

const liveSpinnerInterval = 120 * time.Millisecond

var liveSpinnerFrames = []rune("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")

func liveSpinnerFrame(i int) string {
	// +len wraparound covers math.MinInt where -i overflows negative.
	idx := ((i % len(liveSpinnerFrames)) + len(liveSpinnerFrames)) % len(liveSpinnerFrames)
	return string(liveSpinnerFrames[idx])
}

func liveTickCmd(gen uint64) tea.Cmd {
	return tea.Tick(liveSpinnerInterval, func(time.Time) tea.Msg { return LiveTickMsg{Gen: gen} })
}

func loadLastNCmd(svc Service, topic string, n int, parts []int32, gen uint64) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		msgs, err := svc.FetchLastN(ctx, topic, n, parts)
		return MessagesLoadedMsg{Messages: msgs, Gen: gen, Err: err}
	}
}

func loadEarliestCmd(svc Service, topic string, n int, parts []int32, gen uint64) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		msgs, err := svc.FetchEarliest(ctx, topic, n, parts)
		return MessagesLoadedMsg{Messages: msgs, Gen: gen, Err: err}
	}
}

func loadEarlierCmd(svc Service, topic string, baseline map[int32]int64, n int, parts []int32, gen uint64) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		msgs, err := svc.FetchEarlier(ctx, topic, baseline, n, parts)
		return MessagesAppendedMsg{Messages: msgs, Prepend: true, Direction: "earlier", Gen: gen, Err: err}
	}
}

func loadLaterCmd(svc Service, topic string, baseline map[int32]int64, n int, parts []int32, gen uint64) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		msgs, err := svc.FetchLater(ctx, topic, baseline, n, parts)
		return MessagesAppendedMsg{Messages: msgs, Direction: "later", Gen: gen, Err: err}
	}
}
