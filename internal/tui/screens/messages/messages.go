package messages

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// Service abstracts the Kafka read/produce operations the messages screen
// needs. Production code wires this to a real *kafka.Client; tests pass a
// fake.
type Service interface {
	FetchLastN(ctx context.Context, topic string, n int, partitions []int32) ([]kafka.Message, error)
	FetchAtOffset(ctx context.Context, topic string, partition int32, offset int64, count int) ([]kafka.Message, error)
	FetchAtTimestamp(ctx context.Context, topic string, ts time.Time, partitions []int32, count int) ([]kafka.Message, error)
	FetchEarlier(ctx context.Context, topic string, baseline map[int32]int64, count int, partitions []int32) ([]kafka.Message, error)
	FetchLater(ctx context.Context, topic string, baseline map[int32]int64, count int, partitions []int32) ([]kafka.Message, error)
	Follow(ctx context.Context, topic string, partitions []int32) (*kafka.FollowSession, error)
}

// Action describes the screen's pending intent for the host (router).
type Action struct {
	// Back signals the user pressed Esc/q with no detail view open.
	Back bool
	// Produce, when non-empty, requests the produce form prefilled from the
	// selected message ("resend"). When PrefillFromMessage is non-nil it
	// holds the source message; otherwise this is a fresh produce.
	Produce            string
	PrefillFromMessage *kafka.Message
}

// Mode is the screen's current sub-mode.
type Mode int

const (
	// ModeList: messages table is visible (default).
	ModeList Mode = iota
	// ModeDetail: detail view is overlaid for the focused message.
	ModeDetail
)

// DefaultColumns is used when config does not override.
var DefaultColumns = []string{"timestamp", "partition", "offset", "key", "value"}

// DefaultPageSize is the number of messages fetched on initial load and per
// `[`/`]` window step.
const DefaultPageSize = 200

// Options configure a [Model].
type Options struct {
	Service Service
	Topic   string
	// ReadOnly disables produce/resend hotkeys.
	ReadOnly bool
	// Columns lists the column keys to render, in order.
	Columns []string
	// PageSize bounds how many records are fetched per request.
	PageSize int
	// Clipboard is forwarded to the detail view for copy hotkeys.
	Clipboard Clipboard
	// FileWriter is forwarded to the detail view for save hotkeys.
	FileWriter FileWriter
	// Pager is forwarded to the detail view for the pager hotkey.
	Pager PagerOpener
	// OutputDir is forwarded to the detail view for save targets.
	OutputDir string
	// Now is the injected clock (defaults to time.Now).
	Now func() time.Time
	// Styles overrides the theme palette (mostly for tests).
	Styles theme.Styles
}

// Model is the messages list + detail screen.
type Model struct {
	svc      Service
	topic    string
	readOnly bool

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

	follow         *kafka.FollowSession
	following      bool
	highlightUntil time.Time

	gPrimed       bool
	width, height int

	loading bool
	loadErr string

	action Action
	now    func() time.Time
	styles theme.Styles
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
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = DefaultPageSize
	}
	tbl := components.NewTable(buildColumns(cols), components.WithStyles(styles))

	return &Model{
		svc:       opts.Service,
		topic:     opts.Topic,
		readOnly:  opts.ReadOnly,
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
	}
}

// Init returns the initial load command.
func (m *Model) Init() tea.Cmd {
	m.loading = true
	return loadLastNCmd(m.svc, m.topic, m.pageSize, m.filter)
}

// Topic returns the topic this screen is bound to.
func (m *Model) Topic() string { return m.topic }

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

// Detail returns the detail view (or nil) for tests.
func (m *Model) Detail() *DetailModel { return m.detail }

// Following reports whether follow-mode is active.
func (m *Model) Following() bool { return m.following }

// Toasts exposes the toast queue (for tests).
func (m *Model) Toasts() *components.Toasts { return m.toasts }

// Messages returns the loaded messages in display order (newest first).
func (m *Model) Messages() []kafka.Message {
	out := make([]kafka.Message, len(m.messages))
	copy(out, m.messages)
	return out
}

// SetSize updates width/height.
func (m *Model) SetSize(w, h int) {
	m.width, m.height = w, h
	if h > 0 {
		m.table.SetHeight(maxInt(1, h-7))
	}
}

// KeyHints returns the screen-specific hints shown at the bottom row.
func (m *Model) KeyHints() []layout.KeyHint {
	if m.mode == ModeDetail {
		return m.detail.KeyHints()
	}
	hints := []layout.KeyHint{
		{Key: "Enter", Label: "detail"},
		{Key: "f", Label: "follow"},
		{Key: "[/]", Label: "earlier/later"},
		{Key: "/", Label: "search"},
	}
	if !m.readOnly {
		hints = append(hints, layout.KeyHint{Key: "p", Label: "produce"})
	}
	hints = append(hints, layout.KeyHint{Key: "Esc/q", Label: "back"})
	return hints
}

// Update routes messages.
func (m *Model) Update(msg tea.Msg) (*Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.SetSize(msg.Width, msg.Height)
		return m, nil
	case MessagesLoadedMsg:
		m.handleLoaded(msg)
		return m, nil
	case MessagesAppendedMsg:
		m.handleAppended(msg)
		return m, nil
	case FollowChunkMsg:
		m.handleFollowChunk(msg)
		if msg.Closed {
			return m, nil
		}
		cmd := m.followPollCmd()
		return m, cmd
	case FollowErrMsg:
		m.handleFollowErr(msg)
		return m, nil
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

func (m *Model) handleKey(key tea.KeyPressMsg) (*Model, tea.Cmd) {
	if m.mode == ModeDetail {
		return m.handleDetailKey(key)
	}
	if m.toasts != nil {
		_, _ = m.toasts.Update(key)
	}
	if m.table.SearchActive() {
		tbl, _ := m.table.Update(key)
		m.table = tbl
		return m, nil
	}

	// vim-style "g <x>" jump prefix.
	if m.gPrimed {
		return m.handleGPrefix(key)
	}

	switch key.String() {
	case "esc", "q":
		m.action.Back = true
		return m, nil
	case "enter":
		m.openDetail()
		return m, nil
	case "f":
		cmd := m.toggleFollow()
		return m, cmd
	case "[":
		cmd := m.loadEarlier()
		return m, cmd
	case "]":
		cmd := m.loadLater()
		return m, cmd
	case "p":
		return m.handleProduceKey()
	case "r":
		m.handleResendKey()
		return m, nil
	case "g":
		m.gPrimed = true
		return m, nil
	case "G":
		// jump to most recent (cursor 0 because newest-first sort).
		// the table itself handles the cursor move; nothing to do here.
		tbl, _ := m.table.Update(key)
		m.table = tbl
		return m, nil
	}
	tbl, _ := m.table.Update(key)
	m.table = tbl
	return m, nil
}

// handleGPrefix consumes the second key of a `g <x>` sequence.
func (m *Model) handleGPrefix(key tea.KeyPressMsg) (*Model, tea.Cmd) {
	m.gPrimed = false
	switch key.String() {
	case "g":
		// gg → top
		tbl, _ := m.table.Update(key)
		m.table = tbl
		return m, nil
	case "o":
		m.openJumpForm(jumpOffset)
		return m, nil
	case "t":
		m.openJumpForm(jumpTimestamp)
		return m, nil
	case "p":
		m.openJumpForm(jumpPartition)
		return m, nil
	}
	return m, nil
}

func (m *Model) handleProduceKey() (*Model, tea.Cmd) {
	if m.readOnly {
		m.toasts.Push(components.ToastWarning, "cluster is read-only — produce blocked")
		return m, nil
	}
	m.action.Produce = m.topic
	m.action.PrefillFromMessage = nil
	return m, nil
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

// openDetail enters the detail view for the focused row.
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
		Now:        m.now,
		Styles:     m.styles,
	})
	m.mode = ModeDetail
}

func (m *Model) handleDetailKey(key tea.KeyPressMsg) (*Model, tea.Cmd) {
	d, cmd := m.detail.Update(key)
	m.detail = d
	a := d.ConsumeAction()
	switch {
	case a.Back:
		m.mode = ModeList
		m.detail = nil
	case a.Produce != "":
		m.action.Produce = m.topic
		m.action.PrefillFromMessage = a.PrefillFromMessage
		m.mode = ModeList
		m.detail = nil
	case a.Toast != "":
		m.toasts.Push(components.ToastInfo, a.Toast)
	case a.Warn != "":
		m.toasts.Push(components.ToastWarning, a.Warn)
	}
	return m, cmd
}

// selected returns the message under the table cursor, or false if empty.
func (m *Model) selected() (kafka.Message, bool) {
	idx, ok := m.cursorIndex()
	if !ok {
		return kafka.Message{}, false
	}
	return m.messages[idx], true
}

// cursorIndex returns the index into m.messages for the focused row.
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
	m.loading = false
	if msg.Err != nil {
		m.loadErr = msg.Err.Error()
		m.toasts.Push(components.ToastError, "load messages: "+msg.Err.Error())
		return
	}
	m.loadErr = ""
	m.messages = msg.Messages
	m.refreshTable()
}

func (m *Model) handleAppended(msg MessagesAppendedMsg) {
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
	if len(msg.Messages) > 0 {
		// follow yields newest records — prepend to keep newest-first ordering.
		m.messages = append(msg.Messages, m.messages...)
		m.highlightUntil = m.now().Add(3 * time.Second)
		m.refreshTable()
	}
	if msg.Closed {
		m.stopFollow()
	}
}

func (m *Model) handleFollowErr(msg FollowErrMsg) {
	m.toasts.Push(components.ToastError, "follow: "+msg.Err.Error())
	m.stopFollow()
}

// toggleFollow opens or closes the follow session.
func (m *Model) toggleFollow() tea.Cmd {
	if m.following {
		m.stopFollow()
		return nil
	}
	return m.startFollow()
}

func (m *Model) startFollow() tea.Cmd {
	sess, err := m.svc.Follow(context.Background(), m.topic, m.filter)
	if err != nil {
		m.toasts.Push(components.ToastError, "follow: "+err.Error())
		return nil
	}
	m.follow = sess
	m.following = true
	m.toasts.Push(components.ToastInfo, "follow mode on")
	return m.followPollCmd()
}

func (m *Model) stopFollow() {
	if m.follow != nil {
		m.follow.Close()
		m.follow = nil
	}
	if m.following {
		m.toasts.Push(components.ToastInfo, "follow mode off")
	}
	m.following = false
}

func (m *Model) followPollCmd() tea.Cmd {
	if !m.following || m.follow == nil {
		return nil
	}
	sess := m.follow
	return func() tea.Msg {
		select {
		case msg, ok := <-sess.Messages:
			if !ok {
				return FollowChunkMsg{Closed: true}
			}
			batch := []kafka.Message{msg}
			// drain non-blocking to coalesce a tick.
			for {
				select {
				case extra, ok := <-sess.Messages:
					if !ok {
						return FollowChunkMsg{Messages: batch, Closed: true}
					}
					batch = append(batch, extra)
				default:
					return FollowChunkMsg{Messages: batch}
				}
			}
		case err, ok := <-sess.Errors:
			if !ok {
				return FollowChunkMsg{Closed: true}
			}
			if err == nil {
				return FollowChunkMsg{}
			}
			return FollowErrMsg{Err: err}
		}
	}
}

func (m *Model) loadEarlier() tea.Cmd {
	if len(m.messages) == 0 {
		return nil
	}
	baseline := lowestOffsets(m.messages)
	m.loading = true
	return loadEarlierCmd(m.svc, m.topic, baseline, m.pageSize, m.filter)
}

func (m *Model) loadLater() tea.Cmd {
	if len(m.messages) == 0 {
		return nil
	}
	baseline := highestOffsets(m.messages)
	m.loading = true
	return loadLaterCmd(m.svc, m.topic, baseline, m.pageSize, m.filter)
}

// jumpKind enumerates the `g <x>` jump variants.
type jumpKind int

const (
	jumpOffset jumpKind = iota
	jumpTimestamp
	jumpPartition
)

// openJumpForm is a placeholder hook: a follow-up Task wires a real prompt
// form. For now, jump intents are recorded so tests can verify the prefix
// recognition, and an info toast tells the user the form is unimplemented.
func (m *Model) openJumpForm(kind jumpKind) {
	switch kind {
	case jumpOffset:
		m.toasts.Push(components.ToastInfo, "jump-to-offset prompt: pending")
	case jumpTimestamp:
		m.toasts.Push(components.ToastInfo, "jump-to-timestamp prompt: pending")
	case jumpPartition:
		m.toasts.Push(components.ToastInfo, "jump-to-partition prompt: pending")
	}
}

// JumpToOffset programmatically performs the `g o` jump (used by tests and
// for command-bar bindings).
func (m *Model) JumpToOffset(partition int32, offset int64) tea.Cmd {
	m.loading = true
	return loadAtOffsetCmd(m.svc, m.topic, partition, offset, m.pageSize)
}

// JumpToTimestamp performs the `g t` jump.
func (m *Model) JumpToTimestamp(ts time.Time) tea.Cmd {
	m.loading = true
	return loadAtTimestampCmd(m.svc, m.topic, ts, m.filter, m.pageSize)
}

// JumpToPartition narrows the partition filter and reloads the window.
func (m *Model) JumpToPartition(partitions []int32) tea.Cmd {
	m.filter = append([]int32(nil), partitions...)
	m.loading = true
	return loadLastNCmd(m.svc, m.topic, m.pageSize, m.filter)
}

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
		return FormatTimestamp(msg.Timestamp, m.now())
	case "partition":
		return strconv.FormatInt(int64(msg.Partition), 10)
	case "offset":
		return strconv.FormatInt(msg.Offset, 10)
	case "key":
		return PreviewLine(msg.Key, 32)
	case "value":
		return PreviewLine(msg.Value, valuePreviewWidth(m.width))
	case "headers":
		return strconv.Itoa(len(msg.Headers))
	default:
		return ""
	}
}

// valuePreviewWidth picks how many runes fit in the value column given the
// terminal width. Falls back to a sensible default when width is unknown.
func valuePreviewWidth(termWidth int) int {
	if termWidth <= 0 {
		return 60
	}
	// reserved space for: ts(13), partition(3), offset(8), key(32) plus 4
	// inter-column gaps of 2 chars each.
	const reserved = 13 + 3 + 8 + 32 + 4*2
	w := termWidth - reserved - 4
	if w < 20 {
		return 20
	}
	return w
}

// View renders the screen body.
func (m *Model) View() string {
	if m.mode == ModeDetail {
		return m.detail.View(m.width, m.height)
	}
	parts := []string{m.headerLine(), m.table.View()}
	if t := m.toasts.View(); t != "" {
		parts = append(parts, t)
	}
	return strings.Join(parts, "\n")
}

// headerLine summarizes the loaded window: count, follow indicator, "← NEW"
// flash for 3s after follow chunks arrive.
func (m *Model) headerLine() string {
	body := fmt.Sprintf("%d messages on %s", len(m.messages), m.topic)
	if m.following {
		body += "  " + m.styles.HintKey.Render("● LIVE")
	}
	if m.loading {
		body += "  (loading…)"
	}
	if m.loadErr != "" {
		body += "  " + m.styles.StatusErr.Render("error: "+m.loadErr)
	}
	if !m.highlightUntil.IsZero() && m.now().Before(m.highlightUntil) {
		body += "  " + m.styles.HintKey.Render("← NEW")
	}
	return m.styles.StatusInfo.Render(body)
}

// FormatTimestamp implements §7.3 timestamp formatting:
//
//	HH:MM:SS.mmm  for the current day in the same location
//	MM-DD HH:MM:SS for any other day
func FormatTimestamp(ts, now time.Time) string {
	if ts.IsZero() {
		return "—"
	}
	if sameDay(ts, now) {
		return ts.Format("15:04:05.000")
	}
	return ts.Format("01-02 15:04:05")
}

func sameDay(a, b time.Time) bool {
	ay, am, ad := a.Date()
	by, bm, bd := b.Date()
	return ay == by && am == bm && ad == bd
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
		return components.Column{Title: "Timestamp", Width: 13, Sortable: true}
	case "partition":
		return components.Column{Title: "P", Width: 3, Sortable: true}
	case "offset":
		return components.Column{Title: "Offset", Width: 8, Sortable: true}
	case "key":
		return components.Column{Title: "Key", Width: 32, Sortable: true}
	case "value":
		return components.Column{Title: "Value", Width: 0, Sortable: false}
	case "headers":
		return components.Column{Title: "H", Width: 3, Sortable: true}
	default:
		return components.Column{Title: key, Width: 10}
	}
}

// formatRowID renders a stable row identifier from a record's partition
// and offset. Using a content-based ID (rather than the slice index) lets
// the table preserve cursor focus when follow-mode prepends new rows or
// when the slice is otherwise reordered.
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

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ----- Messages -----

// MessagesLoadedMsg replaces the current window with a fresh batch.
type MessagesLoadedMsg struct {
	Messages []kafka.Message
	Err      error
}

// MessagesAppendedMsg appends or prepends a batch to the existing window.
type MessagesAppendedMsg struct {
	Messages  []kafka.Message
	Prepend   bool   // true when the batch is older than the current window
	Direction string // human-readable direction word for empty-result toast
	Err       error
}

// FollowChunkMsg surfaces one batch of records produced by a follow session.
// Closed is true when the underlying session terminated cleanly.
type FollowChunkMsg struct {
	Messages []kafka.Message
	Closed   bool
}

// FollowErrMsg surfaces an error from a follow session. The session is closed
// before this message is sent.
type FollowErrMsg struct {
	Err error
}

func loadLastNCmd(svc Service, topic string, n int, parts []int32) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		msgs, err := svc.FetchLastN(ctx, topic, n, parts)
		return MessagesLoadedMsg{Messages: msgs, Err: err}
	}
}

func loadEarlierCmd(svc Service, topic string, baseline map[int32]int64, n int, parts []int32) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		msgs, err := svc.FetchEarlier(ctx, topic, baseline, n, parts)
		return MessagesAppendedMsg{Messages: msgs, Prepend: true, Direction: "earlier", Err: err}
	}
}

func loadLaterCmd(svc Service, topic string, baseline map[int32]int64, n int, parts []int32) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		msgs, err := svc.FetchLater(ctx, topic, baseline, n, parts)
		return MessagesAppendedMsg{Messages: msgs, Direction: "later", Err: err}
	}
}

func loadAtOffsetCmd(svc Service, topic string, partition int32, offset int64, n int) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		msgs, err := svc.FetchAtOffset(ctx, topic, partition, offset, n)
		return MessagesLoadedMsg{Messages: msgs, Err: err}
	}
}

func loadAtTimestampCmd(svc Service, topic string, ts time.Time, parts []int32, n int) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		msgs, err := svc.FetchAtTimestamp(ctx, topic, ts, parts, n)
		return MessagesLoadedMsg{Messages: msgs, Err: err}
	}
}
