// Package produce implements the produce form (§7.5) — the screen for
// sending a single record to a topic. The form lets the user pick a partition
// (auto/manual), compression codec, key, headers, and value, and supports
// resending a previously-received message with one click.
//
// Send & close (ctrl+s) submits the record and signals the host to leave;
// Send & keep (ctrl+shift+s) submits without leaving — the form stays open
// for repeated produces. ctrl+p / ctrl+n walk through history; ctrl+r clears
// every field.
package produce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
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

// Service abstracts the single Kafka call the produce form needs. Tests
// inject a fake; production wires this to *kafka.Client.
type Service interface {
	Produce(ctx context.Context, spec kafka.ProduceSpec) (kafka.ProduceResult, error)
}

// History persists past produces and surfaces them for prefill / ctrl+p /
// ctrl+n. Production code wires this to the SQLite-backed store from
// Task 18; tests pass an in-memory implementation.
type History interface {
	// LastForTopic returns the most recent entry for `topic` (used to prefill
	// when the form is freshly opened). The bool is false when no history
	// exists for the topic.
	LastForTopic(topic string) (Entry, bool)
	// Recent returns up to n entries across all topics, newest-first. The
	// produce form walks this slice with ctrl+p / ctrl+n.
	Recent(n int) []Entry
	// Add records a successful produce.
	Add(entry Entry)
}

// Entry captures the form payload for history persistence. It mirrors the
// columns the Task 18 SQLite schema will own (`produce_history`).
type Entry struct {
	Cluster     string
	Topic       string
	Key         []byte
	Value       []byte
	Headers     []kafka.Header
	Partition   int32
	Compression kafka.Compression
	Timestamp   time.Time
}

// PagerOpener opens the value field in `$EDITOR`. Returns the edited bytes.
// Tests inject a fake; production uses [DefaultPagerOpener].
type PagerOpener interface {
	Edit(initial []byte) ([]byte, error)
}

// PagerOpenerFunc adapts a function into a [PagerOpener].
type PagerOpenerFunc func(initial []byte) ([]byte, error)

// Edit calls f.
func (f PagerOpenerFunc) Edit(initial []byte) ([]byte, error) { return f(initial) }

// Action describes the screen's pending intent for the host (router).
type Action struct {
	// Back signals the user pressed esc OR completed a "send & close".
	Back bool
	// Sent is non-nil after a successful produce. The host uses this to flash
	// a success toast and trigger a refresh of the messages screen when the
	// form was opened from there.
	Sent *kafka.ProduceResult
}

// Options configure a [Model].
type Options struct {
	// Service is the Kafka producer abstraction. Required.
	Service Service
	// Cluster is the active cluster name (used when persisting history).
	Cluster string
	// Topic is the topic the form is bound to. May be edited inline (resend).
	Topic string
	// ReadOnly disables the send hotkeys and surfaces a warning.
	ReadOnly bool
	// HistorySize bounds the number of entries returned by [History.Recent].
	// Defaults to 10 (matches `produce.history_size` default).
	HistorySize int
	// History is the optional persistent backing store. nil disables history.
	History History
	// Pager is the $EDITOR opener for the value field. nil disables ctrl+e.
	Pager PagerOpener
	// PrefillFromMessage, when set, populates the form from the source message
	// (resend mode). Partition is reset to auto.
	PrefillFromMessage *kafka.Message
	// Now is the injected clock (defaults to time.Now).
	Now func() time.Time
	// Styles overrides the theme palette (mostly for tests).
	Styles theme.Styles
}

// Model is the produce form screen.
type Model struct {
	svc     Service
	cluster string
	topic   string

	readOnly bool
	hist     History
	pager    PagerOpener

	histSize int
	histPos  int // -1 = no history slot active
	histBuf  []Entry

	form       *components.Form
	toasts     *components.Toasts
	err        string
	sending    bool
	fullscreen bool
	mode       Mode

	width, height int
	action        Action

	now    func() time.Time
	styles theme.Styles
}

// Field keys. Topic is intentionally not a form field — it's fixed by the
// caller (header shows "Produce → <topic>") and shouldn't be editable
// from inside the produce form.
const (
	fieldPartition   = "partition"
	fieldCompression = "compression"
	fieldKey         = "key"
	fieldHeaders     = "headers"
	fieldValue       = "value"
)

// Mode tracks vim-style edit modes for the produce form. NORMAL is the
// default — keys act as commands and field navigation; INSERT is entered
// via Enter on a text-like field and turns keys into literal input.
type Mode int

const (
	// ModeNormal — commands and navigation. Letters/digits are ignored,
	// `tab`/`shift+tab` move between fields, `enter` enters INSERT (or
	// opens a popup on segmented fields).
	ModeNormal Mode = iota
	// ModeInsert — typing inserts into the focused field. `tab` in
	// textarea inserts `\t`; `esc` returns to NORMAL.
	ModeInsert
)

// DefaultHistorySize matches the `produce.history_size` config default (§3.2).
const DefaultHistorySize = 10

// New constructs a fresh produce form.
func New(opts Options) *Model {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	styles := opts.Styles
	if styles.Palette.Foreground == nil {
		styles = theme.DefaultStyles()
	}
	histSize := opts.HistorySize
	if histSize <= 0 {
		histSize = DefaultHistorySize
	}

	m := &Model{
		svc:      opts.Service,
		cluster:  opts.Cluster,
		topic:    opts.Topic,
		readOnly: opts.ReadOnly,
		hist:     opts.History,
		pager:    opts.Pager,
		histSize: histSize,
		histPos:  -1,
		toasts:   components.NewToasts(components.WithToastClock(now), components.WithToastStyles(styles)),
		now:      now,
		styles:   styles,
	}
	m.form = m.buildForm()
	m.form.SetEditing(false) // NORMAL by default — caret hidden

	if opts.PrefillFromMessage != nil {
		m.applyMessage(*opts.PrefillFromMessage, true)
	} else if m.hist != nil {
		if last, ok := m.hist.LastForTopic(m.topic); ok {
			m.applyEntry(last, false)
		}
	}
	return m
}

// buildForm returns the canonical field layout. Used both at construction time
// and by ctrl+r to reset the entire form.
func (m *Model) buildForm() *components.Form {
	fields := []components.Field{
		{Key: fieldPartition, Label: "Partition (auto/<n>)", Kind: components.FieldText, Value: "auto"},
		{
			Key:     fieldCompression,
			Label:   "Compression",
			Kind:    components.FieldSegmented,
			Options: compressionOptions(),
			Value:   string(kafka.CompressionNone),
		},
		{Key: fieldKey, Label: "Key", Kind: components.FieldText},
		{Key: fieldHeaders, Label: "Headers (key=value)", Kind: components.FieldList},
		{Key: fieldValue, Label: "Value", Kind: components.FieldTextarea},
	}
	return components.NewForm(fields, components.WithFormStyles(m.styles))
}

func compressionOptions() []string {
	out := make([]string, 0, len(kafka.AllCompressions))
	for _, c := range kafka.AllCompressions {
		out = append(out, string(c))
	}
	return out
}

// Init satisfies the screen contract — nothing to load asynchronously.
func (m *Model) Init() tea.Cmd { return nil }

// Topic returns the topic the form is currently bound to. Topic isn't an
// editable form field — it's set on construction or updated by resend /
// history prefill.
func (m *Model) Topic() string { return m.topic }

// Form exposes the underlying form component (for tests).
func (m *Model) Form() *components.Form { return m.form }

// Toasts exposes the toast queue (for tests).
func (m *Model) Toasts() *components.Toasts { return m.toasts }

// Action returns the current pending action.
func (m *Model) Action() Action { return m.action }

// ConsumeAction returns the pending action and clears it.
func (m *Model) ConsumeAction() Action {
	a := m.action
	m.action = Action{}
	return a
}

// Sending reports whether a produce call is currently in flight.
func (m *Model) Sending() bool { return m.sending }

// WantsRawInput reports that the produce form is always editing text and wants
// global shortcut keys (`:`, `/`, `?`, `ctrl+r`) routed to its fields as
// literals instead of triggering the host-level handlers.
func (m *Model) WantsRawInput() bool { return true }

// SetSize updates width/height (used by the layout chrome).
func (m *Model) SetSize(w, h int) { m.width, m.height = w, h }

// KeyHints returns the screen-specific hints shown at the bottom row.
func (m *Model) KeyHints() []layout.KeyHint {
	hints := []layout.KeyHint{
		{Key: "enter", Label: "edit"},
		{Key: "tab", Label: "next field"},
		{Key: "+/_", Label: "fullscreen"},
		{Key: "ctrl+s", Label: "send"},
		{Key: "ctrl+shift+s", Label: "send & keep"},
		{Key: "ctrl+e", Label: "$EDITOR"},
		{Key: "ctrl+p/n", Label: "history"},
		{Key: "ctrl+r", Label: "clear"},
		{Key: "esc", Label: "cancel"},
	}
	return hints
}

// Fullscreen reports whether the form is currently in fullscreen mode.
func (m *Model) Fullscreen() bool { return m.fullscreen }

// Mode returns the current edit mode (NORMAL / INSERT).
func (m *Model) Mode() Mode { return m.mode }

// setMode flips between NORMAL and INSERT, keeping form editing state in
// sync (caret rendering follows from `editing`). The focused field also
// gains an `[EDIT]` suffix in INSERT so the active mode is visible right
// next to the field being edited.
func (m *Model) setMode(target Mode) {
	m.mode = target
	m.form.SetEditing(target == ModeInsert)
	if target == ModeInsert {
		m.form.SetFocusedSuffix("[EDIT]")
	} else {
		m.form.SetFocusedSuffix("")
	}
}

// setFullscreen toggles between mode A and mode B. In mode B the segmented
// Compression field is forced into expanded popup view (vertical list); in
// mode A it returns to the compact slider.
func (m *Model) setFullscreen(on bool) {
	m.fullscreen = on
	m.form.SetSegmentedPopup(fieldCompression, on)
}

// Update routes incoming messages.
func (m *Model) Update(msg tea.Msg) (*Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.SetSize(msg.Width, msg.Height)
		return m, nil
	case ProduceResultMsg:
		m.handleResult(msg)
		return m, nil
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

func (m *Model) handleKey(key tea.KeyPressMsg) (*Model, tea.Cmd) {
	if m.toasts != nil {
		_, _ = m.toasts.Update(key)
	}
	// list shortcuts on Headers in INSERT take priority over global ones
	// (e.g. ctrl+n means "add row" here, not "history next").
	if m.mode == ModeInsert && m.form.FocusedField().Kind == components.FieldList {
		if m.handleInsertListShortcut(key) {
			return m, nil
		}
	}
	// mode-agnostic global shortcuts work in both NORMAL and INSERT.
	if mm, cmd, ok := m.handleGlobalShortcut(key); ok {
		return mm, cmd
	}
	if m.mode == ModeInsert {
		return m.handleInsert(key)
	}
	return m.handleNormal(key)
}

// handleGlobalShortcut covers send/clear/history/editor — they should fire
// regardless of mode so the user doesn't need to esc-into-NORMAL just to
// send. Returns ok=true when the key was consumed.
func (m *Model) handleGlobalShortcut(key tea.KeyPressMsg) (*Model, tea.Cmd, bool) {
	switch key.String() {
	case "ctrl+s":
		mm, cmd := m.send(true)
		return mm, cmd, true
	case "ctrl+shift+s":
		mm, cmd := m.send(false)
		return mm, cmd, true
	case "ctrl+e":
		m.openEditor()
		return m, nil, true
	case "ctrl+r":
		m.clear()
		return m, nil, true
	case "ctrl+p":
		m.historyStep(+1)
		return m, nil, true
	case "ctrl+n":
		m.historyStep(-1)
		return m, nil, true
	}
	return m, nil, false
}

// handleNormal is the default mode: tab/shift+tab navigate, enter is
// contextual (INSERT for text/list, popup for segmented), `+`/`_` toggle
// fullscreen, on Headers `=` adds a row and `-` removes. Letters/digits
// are ignored — they only do work in INSERT.
func (m *Model) handleNormal(key tea.KeyPressMsg) (*Model, tea.Cmd) {
	switch key.String() {
	case "esc":
		return m.handleEscNormal(key)
	case "+", "_", "shift++", "shift+-":
		// fullscreen toggle — `+` is shift+`=` (and `_` is shift+`-`) on
		// US/RU layouts; the kitty-protocol `shift++` / `shift+-` strings
		// also mapped here for completeness.
		m.setFullscreen(!m.fullscreen)
		return m, nil
	case "tab", "down":
		m.form.FocusNext()
		return m, nil
	case "shift+tab", "up":
		m.form.FocusPrev()
		return m, nil
	case "enter":
		return m.enterInsertOnFocused(key)
	}
	// segmented fields are "interactive without INSERT" — left/right and
	// hjkl cycle the value live, so let the form handle them in NORMAL.
	if m.form.FocusedField().Kind == components.FieldSegmented {
		f, cmd := m.form.Update(key)
		m.form = f
		return m, cmd
	}
	// any other NORMAL-mode keystroke is ignored.
	return m, nil
}

// handleEscNormal implements the esc cascade: popup → close popup;
// fullscreen → split; otherwise → close form.
func (m *Model) handleEscNormal(key tea.KeyPressMsg) (*Model, tea.Cmd) {
	if m.form.PopupActive() && !m.fullscreen {
		f, cmd := m.form.Update(key)
		m.form = f
		return m, cmd
	}
	if m.fullscreen {
		m.setFullscreen(false)
		return m, nil
	}
	m.action.Back = true
	return m, nil
}

// enterInsertOnFocused decides what `enter` does in NORMAL based on the
// focused field's kind. Segmented opens a popup (its native behavior);
// list with no rows gets a fresh empty row first; everything else flips
// the mode flag and lets INSERT handle the next keystroke.
func (m *Model) enterInsertOnFocused(key tea.KeyPressMsg) (*Model, tea.Cmd) {
	fld := m.form.FocusedField()
	switch fld.Kind {
	case components.FieldSegmented:
		f, cmd := m.form.Update(key)
		m.form = f
		return m, cmd
	case components.FieldList:
		if len(fld.List) == 0 {
			m.form.AppendListRow()
		}
		m.setMode(ModeInsert)
		return m, nil
	default:
		m.setMode(ModeInsert)
		return m, nil
	}
}

// handleInsert is INSERT mode: typing inserts into the focused field;
// special keys (tab, shift+tab, enter, esc) implement commit / navigate /
// newline / leave-mode semantics.
func (m *Model) handleInsert(key tea.KeyPressMsg) (*Model, tea.Cmd) {
	switch key.String() {
	case "esc":
		// if the segmented popup is open, esc reverts/closes it but stays
		// in INSERT; otherwise it returns to NORMAL on the same field.
		if m.form.PopupActive() {
			f, cmd := m.form.Update(key)
			m.form = f
			return m, cmd
		}
		m.setMode(ModeNormal)
		return m, nil
	case "tab":
		return m.handleInsertTab()
	case "shift+tab":
		m.form.FocusPrev()
		m.setMode(ModeNormal)
		return m, nil
	case "enter":
		return m.handleInsertEnter(key)
	}
	f, cmd := m.form.Update(key)
	m.form = f
	return m, cmd
}

// handleInsertTab implements the textarea-vs-single-line tab split: in a
// textarea the tab is inserted as a literal `\t`; everywhere else it
// commits and navigates to the next field, returning to NORMAL.
func (m *Model) handleInsertTab() (*Model, tea.Cmd) {
	if m.form.FocusedField().Kind == components.FieldTextarea {
		m.form.InsertAtCursor("\t")
		return m, nil
	}
	m.form.FocusNext()
	m.setMode(ModeNormal)
	return m, nil
}

// handleInsertEnter implements the per-kind Enter semantics in INSERT:
//   - textarea: insert newline at cursor (stay in INSERT).
//   - list (Headers): chained-entry idiom — Enter on a non-empty row
//     commits and adds a fresh empty row to keep filling; Enter on an
//     empty row exits to NORMAL (signals "done adding").
//   - single-line text: commit and return to NORMAL on the same field.
func (m *Model) handleInsertEnter(key tea.KeyPressMsg) (*Model, tea.Cmd) {
	fld := m.form.FocusedField()
	switch fld.Kind {
	case components.FieldTextarea:
		f, cmd := m.form.Update(key)
		m.form = f
		return m, cmd
	case components.FieldList:
		// pressing enter on an empty row finishes the add-many loop.
		if entry, _, ok := m.form.FocusedListEntry(); ok && entry == "" {
			m.setMode(ModeNormal)
			return m, nil
		}
		// otherwise commit-and-continue: add a new empty row and stay in
		// INSERT for sequential header entry.
		m.form.AppendListRow()
		return m, nil
	default:
		m.setMode(ModeNormal)
		return m, nil
	}
}

// handleInsertListShortcut covers the headers-only `ctrl+n` / `ctrl+x`
// shortcuts in INSERT: `ctrl+n` (new) jumps to the end of the list and
// starts a new empty row; `ctrl+x` (cut) deletes the focused row (and
// exits INSERT if the list becomes empty). Returns ok=true when the key
// was consumed. These take priority over the global history shortcut
// when the focused field is a list.
func (m *Model) handleInsertListShortcut(key tea.KeyPressMsg) (consumed bool) {
	switch key.String() {
	case "ctrl+n":
		m.form.AppendListRow()
		return true
	case "ctrl+x":
		m.form.RemoveListRow()
		if _, _, ok := m.form.FocusedListEntry(); !ok {
			// list became empty — leave INSERT, nothing left to edit.
			m.setMode(ModeNormal)
		}
		return true
	}
	return false
}

// send validates and dispatches a produce. closeAfter=true → ctrl+s (send &
// close); closeAfter=false → ctrl+shift+s (send & keep).
func (m *Model) send(closeAfter bool) (*Model, tea.Cmd) {
	if m.readOnly {
		m.toasts.Push(components.ToastWarning, "cluster is read-only — produce blocked")
		return m, nil
	}
	spec, err := m.spec()
	if err != nil {
		m.err = err.Error()
		return m, nil
	}
	m.err = ""
	m.sending = true
	return m, produceCmd(m.svc, spec, closeAfter)
}

func (m *Model) handleResult(msg ProduceResultMsg) {
	m.sending = false
	if msg.Err != nil {
		m.err = msg.Err.Error()
		m.toasts.Push(components.ToastError, "produce: "+msg.Err.Error())
		return
	}
	r := msg.Result
	m.toasts.Push(components.ToastSuccess, fmt.Sprintf(
		"Sent to %s P%d:%d (%dms)",
		r.Topic, r.Partition, r.Offset, r.Duration.Milliseconds(),
	))
	m.recordHistory(msg.Spec)
	m.action.Sent = &r
	if msg.Close {
		m.action.Back = true
	}
}

// spec validates the current form contents and returns a kafka.ProduceSpec.
// Topic is taken directly from the model (it isn't a form field — see the
// constant block) and must be non-empty.
func (m *Model) spec() (kafka.ProduceSpec, error) {
	get := func(key string) string {
		fld, _ := m.form.Field(key)
		return strings.TrimSpace(fld.Value)
	}
	topic := strings.TrimSpace(m.topic)
	if topic == "" {
		return kafka.ProduceSpec{}, errors.New("topic is required")
	}
	partition, err := parsePartition(get(fieldPartition))
	if err != nil {
		return kafka.ProduceSpec{}, err
	}
	codec, err := kafka.ParseCompression(get(fieldCompression))
	if err != nil {
		return kafka.ProduceSpec{}, fmt.Errorf("compression: %w", err)
	}

	headersField, _ := m.form.Field(fieldHeaders)
	headers, err := parseHeaders(headersField.List)
	if err != nil {
		return kafka.ProduceSpec{}, err
	}

	keyField, _ := m.form.Field(fieldKey)
	valField, _ := m.form.Field(fieldValue)

	spec := kafka.ProduceSpec{
		Topic:       topic,
		Partition:   partition,
		Key:         []byte(keyField.Value),
		Value:       []byte(valField.Value),
		Headers:     headers,
		Compression: codec,
	}
	return spec, nil
}

// parsePartition turns "auto" / "" / "<n>" into the int32 partition slot.
// Returns an error for non-numeric, non-auto inputs.
func parsePartition(raw string) (int32, error) {
	switch strings.ToLower(raw) {
	case "", "auto":
		return kafka.PartitionAuto, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("partition must be 'auto' or a non-negative integer (got %q)", raw)
	}
	if n < 0 {
		return 0, fmt.Errorf("partition must be a non-negative integer (got %d)", n)
	}
	if n > (1<<31 - 1) {
		return 0, fmt.Errorf("partition out of int32 range (got %d)", n)
	}
	return int32(n), nil //nolint:gosec // bounded above
}

// parseHeaders converts the FieldList entries (each "key=value") into
// kafka.Header. Empty entries are skipped; entries without "=" yield an error
// so the user knows to fix them.
func parseHeaders(entries []string) ([]kafka.Header, error) {
	out := make([]kafka.Header, 0, len(entries))
	for _, e := range entries {
		entry := strings.TrimSpace(e)
		if entry == "" {
			continue
		}
		idx := strings.IndexByte(entry, '=')
		if idx < 0 {
			return nil, fmt.Errorf("header %q must be key=value", entry)
		}
		out = append(out, kafka.Header{
			Key:   strings.TrimSpace(entry[:idx]),
			Value: []byte(entry[idx+1:]),
		})
	}
	return out, nil
}

// recordHistory persists the just-sent payload into the history backend and
// keeps the in-memory cache in sync so ctrl+p/ctrl+n find it instantly.
func (m *Model) recordHistory(spec kafka.ProduceSpec) {
	entry := Entry{
		Cluster:     m.cluster,
		Topic:       spec.Topic,
		Key:         append([]byte(nil), spec.Key...),
		Value:       append([]byte(nil), spec.Value...),
		Headers:     append([]kafka.Header(nil), spec.Headers...),
		Partition:   spec.Partition,
		Compression: spec.Compression,
		Timestamp:   m.now(),
	}
	if m.hist != nil {
		m.hist.Add(entry)
	}
	// invalidate the in-memory cursor so the next ctrl+p refetches.
	m.histBuf = nil
	m.histPos = -1
}

// clear resets every field back to its default state (ctrl+r).
func (m *Model) clear() {
	m.form = m.buildForm()
	// re-apply mode to the fresh form so caret/[EDIT] match m.mode.
	m.setMode(m.mode)
	m.err = ""
	m.histPos = -1
	m.histBuf = nil
}

// historyStep moves the history cursor by `delta`. +1 = older (ctrl+p), -1 =
// newer (ctrl+n). Loads the History snapshot lazily on first use.
func (m *Model) historyStep(delta int) {
	if m.hist == nil {
		m.toasts.Push(components.ToastInfo, "history disabled")
		return
	}
	if m.histBuf == nil {
		m.histBuf = m.hist.Recent(m.histSize)
	}
	if len(m.histBuf) == 0 {
		m.toasts.Push(components.ToastInfo, "no history yet")
		return
	}
	pos := max(m.histPos+delta, -1)
	if pos >= len(m.histBuf) {
		pos = len(m.histBuf) - 1
	}
	m.histPos = pos
	if pos < 0 {
		// ctrl+n stepped past the newest — reset to the empty form.
		m.form = m.buildForm()
		m.setMode(m.mode)
		return
	}
	m.applyEntry(m.histBuf[pos], false)
}

// applyEntry overwrites the form fields with the entry's payload.
// resetPartitionToAuto matches the resend rule from §7.5: "partition resets
// to auto" so the user picks a destination explicitly.
func (m *Model) applyEntry(entry Entry, resetPartitionToAuto bool) {
	m.topic = entry.Topic
	if resetPartitionToAuto {
		m.form.SetValue(fieldPartition, "auto")
	} else {
		m.form.SetValue(fieldPartition, formatPartition(entry.Partition))
	}
	m.form.SetValue(fieldCompression, string(entry.Compression))
	m.form.SetValue(fieldKey, string(entry.Key))
	m.form.SetList(fieldHeaders, formatHeaderList(entry.Headers))
	m.form.SetValue(fieldValue, string(entry.Value))
}

// applyMessage prefills the form from a kafka.Message in resend mode. In
// resend the partition is reset to auto so the user re-selects.
func (m *Model) applyMessage(msg kafka.Message, resetPartitionToAuto bool) {
	m.topic = msg.Topic
	if resetPartitionToAuto {
		m.form.SetValue(fieldPartition, "auto")
	} else {
		m.form.SetValue(fieldPartition, strconv.FormatInt(int64(msg.Partition), 10))
	}
	m.form.SetValue(fieldKey, string(msg.Key))
	m.form.SetList(fieldHeaders, formatHeaderList(msg.Headers))
	m.form.SetValue(fieldValue, string(msg.Value))
}

func formatPartition(p int32) string {
	if p < 0 {
		return "auto"
	}
	return strconv.FormatInt(int64(p), 10)
}

func formatHeaderList(headers []kafka.Header) []string {
	out := make([]string, 0, len(headers))
	for _, h := range headers {
		out = append(out, h.Key+"="+string(h.Value))
	}
	return out
}

// openEditor pipes the current value field through `$EDITOR`, replacing the
// value with the editor's output on success.
func (m *Model) openEditor() {
	if m.pager == nil {
		m.toasts.Push(components.ToastWarning, "editor: no $EDITOR opener configured")
		return
	}
	val, _ := m.form.Field(fieldValue)
	edited, err := m.pager.Edit([]byte(val.Value))
	if err != nil {
		m.toasts.Push(components.ToastError, "editor: "+err.Error())
		return
	}
	m.form.SetValue(fieldValue, string(edited))
	m.form.FocusKey(fieldValue)
}

// View renders the form body wrapped in the standard rounded box. There are
// two layouts: mode A (default) is a single column listing all fields and
// stretching to the full available area; mode B (fullscreen) shows a tab
// strip across the top and the active field below.
func (m *Model) View() string {
	header := m.styles.HelpTitle.Render("Produce → " + m.topic)
	var hintText string
	switch {
	case m.mode == ModeInsert:
		hintText = "type to edit  tab next  enter commit/newline  esc back to NORMAL  on headers: ctrl+n add row  ctrl+x remove row"
	case m.fullscreen:
		hintText = "tab/shift+tab cycle field  enter edit  +/_ exit fullscreen  ctrl+s send  esc back to split"
	default:
		hintText = "tab/shift+tab navigate  enter edit  +/_ fullscreen  ctrl+s send  esc cancel"
	}
	hint := m.styles.HintLabel.Render(hintText)

	parts := []string{header}
	if m.err != "" {
		parts = append(parts, m.styles.StatusErr.Render(m.err))
	}
	if m.sending {
		parts = append(parts, m.styles.StatusInfo.Render("sending…"))
	}

	var body string
	if m.fullscreen {
		body = m.renderFullscreen()
	} else {
		body = m.form.View()
	}
	parts = append(parts, body, "", hint)
	if t := m.toasts.View(); t != "" {
		parts = append(parts, t)
	}
	rendered := strings.Join(parts, "\n")

	box := lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).Padding(0, 2)
	// Stretch the box to fill the available area so the form occupies the
	// whole screen instead of shrinking to its content.
	if m.width > 4 {
		box = box.Width(m.width - 2)
	}
	if m.height > 4 {
		box = box.Height(m.height - 2)
	}
	return box.Render(rendered)
}

// fieldOrder is the canonical tab order for navigation and tab-strip rendering.
var fieldOrder = []string{
	fieldPartition, fieldCompression, fieldKey, fieldHeaders, fieldValue,
}

// fieldLabel maps internal keys to short labels used by the fullscreen tab strip.
var fieldLabel = map[string]string{
	fieldPartition:   "Partition",
	fieldCompression: "Compression",
	fieldKey:         "Key",
	fieldHeaders:     "Headers",
	fieldValue:       "Value",
}

// renderFullscreen renders the tab strip plus the active field below it.
func (m *Model) renderFullscreen() string {
	active := m.form.FocusedField().Key
	return m.renderTabs() + "\n\n" + m.form.RenderField(active)
}

func (m *Model) renderTabs() string {
	active := m.form.FocusedField().Key
	parts := make([]string, 0, len(fieldOrder))
	for _, k := range fieldOrder {
		label := fieldLabel[k]
		if k == active {
			parts = append(parts, m.styles.HintKey.Render("[ "+label+" ]"))
		} else {
			parts = append(parts, m.styles.HintLabel.Render("  "+label+"  "))
		}
	}
	return strings.Join(parts, " ")
}

// ----- Messages -----

// ProduceResultMsg is dispatched after a produce call returns. Close is true
// when the request originated from ctrl+s (send & close); false for
// ctrl+shift+s (send & keep).
type ProduceResultMsg struct {
	Spec   kafka.ProduceSpec
	Result kafka.ProduceResult
	Close  bool
	Err    error
}

func produceCmd(svc Service, spec kafka.ProduceSpec, closeAfter bool) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		res, err := svc.Produce(ctx, spec)
		return ProduceResultMsg{Spec: spec, Result: res, Close: closeAfter, Err: err}
	}
}

// DefaultPagerOpener returns a [PagerOpener] that runs `$EDITOR <tmpfile>`,
// then reads the result back. Falls back to `vi` when $EDITOR is unset.
// Unsuitable for unit tests; tests inject [PagerOpenerFunc].
func DefaultPagerOpener() PagerOpener {
	return PagerOpenerFunc(func(initial []byte) ([]byte, error) {
		editor := strings.TrimSpace(os.Getenv("EDITOR"))
		if editor == "" {
			editor = "vi"
		}
		tmp, err := os.CreateTemp("", "kafka-tui-produce-*.txt")
		if err != nil {
			return nil, fmt.Errorf("editor: create temp: %w", err)
		}
		path := tmp.Name()
		defer os.Remove(path)
		if _, werr := tmp.Write(initial); werr != nil {
			_ = tmp.Close()
			return nil, fmt.Errorf("editor: write temp: %w", werr)
		}
		if cerr := tmp.Close(); cerr != nil {
			return nil, fmt.Errorf("editor: close temp: %w", cerr)
		}
		parts := strings.Fields(editor)
		args := make([]string, 0, len(parts))
		args = append(args, parts[1:]...)
		args = append(args, path)
		cmd := exec.CommandContext(context.Background(), parts[0], args...) //nolint:gosec // user-controlled $EDITOR
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if rerr := cmd.Run(); rerr != nil {
			return nil, fmt.Errorf("editor: run: %w", rerr)
		}
		out, rerr := os.ReadFile(path) //nolint:gosec // path is the tmpfile we just created
		if rerr != nil {
			return nil, fmt.Errorf("editor: read result: %w", rerr)
		}
		return out, nil
	})
}
