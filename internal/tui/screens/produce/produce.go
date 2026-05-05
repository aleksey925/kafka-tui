// Package produce implements the produce form (§7.5) — the screen for
// sending a single record to a topic. The form lets the user pick a partition
// (auto or any of the topic's partitions, via the segmented picker),
// compression codec, key, headers, and value, and supports resending a
// previously-received message with one click.
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
	"slices"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/help"
	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// Service abstracts the Kafka calls the produce form needs. Tests inject a
// fake; production wires this to *kafka.Client.
type Service interface {
	Produce(ctx context.Context, spec kafka.ProduceSpec) (kafka.ProduceResult, error)
	// TopicPartitions returns the partition metadata for `topic`. Only the
	// partition IDs are consumed by the produce screen — they populate the
	// segmented partition picker.
	TopicPartitions(ctx context.Context, topic string) ([]kafka.PartitionDetail, error)
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

	// partitionsTopic is the topic whose partition list is currently loaded
	// into the segmented picker. When `topic` diverges (resend / history
	// jump to another topic), the options reset to {auto} until a fresh
	// fetch lands.
	partitionsTopic string

	// partition type-to-jump: digits typed while the partition field is
	// focused accumulate in `partitionTypeBuf` and select the matching
	// option live; the buffer auto-clears after partitionTypeIdle of
	// inactivity. `partitionTypeGen` invalidates stale tick callbacks.
	partitionTypeBuf string
	partitionTypeGen int

	action Action

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

	// partitionAuto is the segmented-picker option mapped to
	// kafka.PartitionAuto. Numeric options use FormatInt of the partition id.
	partitionAuto = "auto"

	// partitionTypeIdle is how long an accumulated digit buffer survives
	// without further input before being cleared.
	partitionTypeIdle = 700 * time.Millisecond
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
		{
			Key:     fieldPartition,
			Label:   "Partition",
			Kind:    components.FieldSegmented,
			Options: []string{partitionAuto},
			Value:   partitionAuto,
		},
		{
			Key:     fieldCompression,
			Label:   "Compression",
			Kind:    components.FieldSegmented,
			Options: compressionOptions(),
			Value:   string(kafka.CompressionNone),
		},
		{Key: fieldKey, Label: "Key", Kind: components.FieldText},
		{Key: fieldHeaders, Label: "Headers (key=value)", Kind: components.FieldList, Validator: validateHeader},
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

// Init kicks off async metadata for the partition picker. The form starts
// with just `auto` and grows once the partitions arrive (or the fetch fails,
// in which case the picker stays auto-only and a toast surfaces the error).
func (m *Model) Init() tea.Cmd {
	return m.reloadPartitionsIfTopicChanged()
}

// partitionsLoadedMsg carries the partition list returned by the async
// metadata fetch.
type partitionsLoadedMsg struct {
	topic      string
	partitions []int32
	err        error
}

func loadPartitionsCmd(svc Service, topic string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		details, err := svc.TopicPartitions(ctx, topic)
		if err != nil {
			return partitionsLoadedMsg{topic: topic, err: err}
		}
		ids := make([]int32, len(details))
		for i, d := range details {
			ids[i] = d.Partition
		}
		return partitionsLoadedMsg{topic: topic, partitions: ids}
	}
}

func (m *Model) handlePartitionsLoaded(msg partitionsLoadedMsg) {
	// stale response (topic changed via resend before metadata arrived)
	if msg.topic != m.topic {
		return
	}
	if msg.err != nil {
		m.toasts.Push(components.ToastWarning, "partitions: "+msg.err.Error())
		return
	}
	m.form.SetOptions(fieldPartition, partitionOptions(msg.partitions))
	m.partitionsTopic = msg.topic
}

// reloadPartitionsIfTopicChanged returns a cmd to refresh the partition
// picker when `m.topic` differs from the currently-loaded topic. When the
// previous load was for a different (non-empty) topic, the options are
// reset to {auto} so the user doesn't pick from stale partitions; on the
// initial fetch the existing options are kept so a prefilled value isn't
// clobbered before the real list arrives. Returns nil when the picker is
// already in sync.
func (m *Model) reloadPartitionsIfTopicChanged() tea.Cmd {
	if m.topic == m.partitionsTopic {
		return nil
	}
	if m.partitionsTopic != "" {
		m.form.SetOptions(fieldPartition, []string{partitionAuto})
	}
	m.resetPartitionTypeBuf()
	// clear immediately so a subsequent topic switch back to a previously
	// loaded topic still re-triggers a fetch instead of trusting the stale
	// `partitionsTopic` (the options were just wiped above).
	m.partitionsTopic = ""
	if m.topic == "" {
		return nil
	}
	return loadPartitionsCmd(m.svc, m.topic)
}

func partitionOptions(ids []int32) []string {
	out := make([]string, 0, len(ids)+1)
	out = append(out, partitionAuto)
	for _, id := range ids {
		out = append(out, strconv.FormatInt(int64(id), 10))
	}
	return out
}

// Topic returns the topic the form is currently bound to. Topic isn't an
// editable form field — it's set on construction or updated by resend /
// history prefill.
func (m *Model) Topic() string { return m.topic }

// Form exposes the underlying form component (for tests).
func (m *Model) Form() *components.Form { return m.form }

// Toasts exposes the toast queue (for tests).
func (m *Model) Toasts() *components.Toasts { return m.toasts }

// LatestFlash returns the freshest live toast from this screen's queue.
func (m *Model) LatestFlash() (components.Toast, bool) {
	if m.toasts == nil {
		return components.Toast{}, false
	}
	return m.toasts.Latest()
}

// Title returns the frame title rendered by the host.
func (m *Model) Title() string {
	return "Produce → " + m.topic
}

// Breadcrumb is unused for the produce form (returns empty).
func (m *Model) Breadcrumb() string { return "" }

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

// WantsRawInput reports true only while the form is actively editing
// a field (INSERT). In NORMAL the form ignores letter / digit keys
// anyway, so leaving raw-input off there lets the user reach global
// shortcuts (`:`, `/`, `?`) — the help overlay especially.
// Switching back to INSERT (with `enter` on a focused field) restores
// raw-input so literal `?`, `/`, `:` etc. land in the field text.
func (m *Model) WantsRawInput() bool { return m.mode == ModeInsert }

// HasOverlay reports that the produce form is always overlay-like:
// it sits on top of the screen the user opened it from (topics or
// messages). The host uses this to suppress the q/esc quit-fallback
// inside the form, so a stray `q` in NORMAL doesn't pop the screen
// out from under the user mid-edit. The screen's own esc handler
// (`handleEscNormal`) raises action.Back to close the form cleanly.
func (m *Model) HasOverlay() bool { return true }

// SetSize satisfies the Screen interface. The produce form is rendered by
// the host frame, which manages width/height itself, so the screen ignores
// the values.
func (m *Model) SetSize(_, _ int) {}

// KeyHints derives the bottom-row entries from the global-shortcut
// bindings table.
func (m *Model) KeyHints() []layout.KeyHint {
	return layout.HintsFromBindings(m.bindings())
}

// HelpSections derives the `?`-overlay sections from the same source.
func (m *Model) HelpSections() []help.Section {
	return help.SectionsFromBindings(m.bindings())
}

// bindings concatenates the always-on globals with the NORMAL-mode
// commands so help/hints surface every screen-level shortcut. The
// dispatcher consumes the two slices separately because globals fire
// in both NORMAL and INSERT, while normal-mode keys must not steal
// editing characters in INSERT.
func (m *Model) bindings() []keymap.Binding {
	return append(m.globalBindings(), m.normalBindings()...)
}

// globalBindings are mode-agnostic: send / clear / history / editor.
// They fire in both NORMAL and INSERT so the user doesn't have to
// esc-out before sending.
func (m *Model) globalBindings() []keymap.Binding {
	return []keymap.Binding{
		{Keys: []string{"ctrl+s"}, Label: "send (close form)", Category: "Produce", Hint: true, Handler: func() tea.Cmd { return m.send(true) }},
		{Keys: []string{"ctrl+shift+s"}, Label: "send & keep open", Category: "Produce", Hint: true, Handler: func() tea.Cmd { return m.send(false) }},
		{Keys: []string{"ctrl+e"}, Label: "open value in $EDITOR", Category: "Produce", Hint: true, Handler: m.actEditor},
		{Keys: []string{"ctrl+r"}, Label: "clear form", Category: "Produce", Hint: true, Handler: m.actClear},
		{Keys: []string{"ctrl+p"}, Label: "history older", Category: "Produce", Hint: true, Handler: m.actHistoryOlder},
		{Keys: []string{"ctrl+n"}, Label: "history newer", Category: "Produce", Hint: true, Handler: m.actHistoryNewer},
	}
}

// normalBindings are NORMAL-mode only — fullscreen, navigation,
// enter-insert, esc — and intentionally NOT consulted in INSERT so
// `tab`, `enter`, `esc` retain their text-editing meaning there.
func (m *Model) normalBindings() []keymap.Binding {
	return []keymap.Binding{
		{Keys: []string{"+", "_", "shift++", "shift+-"}, Label: "toggle fullscreen", Category: "Form", Hint: true, Handler: m.actToggleFullscreen},
		{Keys: []string{"tab", "down"}, Label: "next field", Category: "Form", Hint: true, Handler: m.actFocusNext},
		{Keys: []string{"shift+tab", "up"}, Label: "previous field", Category: "Form", Handler: m.actFocusPrev},
		{Keys: []string{"enter"}, Label: "edit focused field", Category: "Form", Hint: true, HandlerMsg: m.enterInsertOnFocused},
		{Keys: []string{"esc"}, Label: "cancel edit / close form", Category: "Form", Hint: true, HandlerMsg: m.handleEscNormal},
	}
}

func (m *Model) actToggleFullscreen() tea.Cmd {
	m.setFullscreen(!m.fullscreen)
	return nil
}

func (m *Model) actFocusNext() tea.Cmd {
	m.resetPartitionTypeBuf()
	m.form.FocusNext()
	return nil
}

func (m *Model) actFocusPrev() tea.Cmd {
	m.resetPartitionTypeBuf()
	m.form.FocusPrev()
	return nil
}

func (m *Model) actEditor() tea.Cmd { m.openEditor(); return nil }
func (m *Model) actClear() tea.Cmd  { m.clear(); return nil }

func (m *Model) actHistoryOlder() tea.Cmd {
	m.historyStep(+1)
	return m.reloadPartitionsIfTopicChanged()
}

func (m *Model) actHistoryNewer() tea.Cmd {
	m.historyStep(-1)
	return m.reloadPartitionsIfTopicChanged()
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
func (m *Model) Update(msg tea.Msg) tea.Cmd {
	switch msg := msg.(type) {
	case ProduceResultMsg:
		m.handleResult(msg)
		return nil
	case partitionsLoadedMsg:
		m.handlePartitionsLoaded(msg)
		return nil
	case partitionTypeTickMsg:
		m.handlePartitionTypeTick(msg)
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
	// list shortcuts on Headers in INSERT take priority over global ones
	// (e.g. ctrl+n means "add row" here, not "history next").
	if m.mode == ModeInsert && m.form.FocusedField().Kind == components.FieldList {
		if m.handleInsertListShortcut(key) {
			return nil
		}
	}
	// mode-agnostic global shortcuts work in both NORMAL and INSERT.
	if cmd, ok := m.handleGlobalShortcut(key); ok {
		return cmd
	}
	if m.mode == ModeInsert {
		return m.handleInsert(key)
	}
	return m.handleNormal(key)
}

// handleGlobalShortcut covers send/clear/history/editor — they should fire
// regardless of mode so the user doesn't need to esc-into-NORMAL just to
// send. Routed via the global slice of the bindings table for parity
// with help/hints.
func (m *Model) handleGlobalShortcut(key tea.KeyPressMsg) (tea.Cmd, bool) {
	return keymap.Dispatch(m.globalBindings(), key)
}

// handleNormal is the default mode: tab/shift+tab navigate, enter is
// contextual (INSERT for text/list, popup for segmented), `+`/`_` toggle
// fullscreen, on Headers `=` adds a row and `-` removes. Letters/digits
// are ignored — they only do work in INSERT.
//
// Priority: when a segmented popup is open, navigation keys (enter, arrows,
// hjkl, tab) are routed to the popup *first*. esc and the fullscreen
// toggles (`shift+-`/`_`) deliberately keep their NORMAL semantics and
// fall through to the switch below, so esc cascade closes the popup via
// handleEscNormal and `_` collapses fullscreen.
func (m *Model) handleNormal(key tea.KeyPressMsg) tea.Cmd {
	if m.form.PopupActive() && popupNavKey(key) {
		f, cmd := m.form.Update(key)
		m.form = f
		return cmd
	}
	if cmd, ok := keymap.Dispatch(m.normalBindings(), key); ok {
		return cmd
	}
	// segmented fields are "interactive without INSERT" — left/right and
	// hjkl cycle the value live, so let the form handle them in NORMAL.
	if m.form.FocusedField().Kind == components.FieldSegmented {
		if cmd, handled := m.handlePartitionTypeJump(key); handled {
			return cmd
		}
		f, cmd := m.form.Update(key)
		m.form = f
		return cmd
	}
	// any other NORMAL-mode keystroke is ignored.
	return nil
}

// handlePartitionTypeJump implements type-to-jump on the partition picker:
// digits typed while the partition field is focused accumulate into a
// short-lived buffer and live-select the matching option ("4" then "7" →
// "47"). The new digit is reconciled against the option set in three steps:
// (1) extend the running buffer if some option still starts with it,
// otherwise (2) restart from the digit alone if any option starts with it,
// otherwise (3) eat the keystroke without changing buffer or value (so the
// picker doesn't blink on out-of-range numbers). Non-digit keys clear the
// buffer so subsequent arrow cycling starts from the current value. Each
// consumed digit reschedules the idle timer. Returns (cmd, true) when the
// key was consumed.
func (m *Model) handlePartitionTypeJump(key tea.KeyPressMsg) (tea.Cmd, bool) {
	if m.form.FocusedField().Key != fieldPartition {
		return nil, false
	}
	s := key.String()
	if len(s) != 1 || s[0] < '0' || s[0] > '9' {
		// any non-digit on the partition field invalidates the buffer; the
		// key itself is not consumed (left/right/etc. still cycle).
		m.resetPartitionTypeBuf()
		return nil, false
	}
	opts := m.form.FocusedField().Options
	candidate := m.partitionTypeBuf + s
	switch {
	case slices.ContainsFunc(opts, func(o string) bool { return strings.HasPrefix(o, candidate) }):
		m.partitionTypeBuf = candidate
	case slices.ContainsFunc(opts, func(o string) bool { return strings.HasPrefix(o, s) }):
		// the new digit breaks the running buffer — restart from this digit.
		candidate = s
		m.partitionTypeBuf = candidate
	default:
		// digit matches no option as either continuation or fresh prefix;
		// eat it without touching buffer/value, but still bump the idle
		// timer so it tracks "time since last keystroke", not "time since
		// last successful prefix match".
		m.partitionTypeGen++
		return partitionTypeTickCmd(m.partitionTypeGen), true
	}
	if slices.Contains(opts, candidate) {
		m.form.SetValue(fieldPartition, candidate)
	}
	m.partitionTypeGen++
	return partitionTypeTickCmd(m.partitionTypeGen), true
}

func (m *Model) resetPartitionTypeBuf() {
	if m.partitionTypeBuf == "" {
		return
	}
	m.partitionTypeBuf = ""
	m.partitionTypeGen++
}

// partitionTypeTickMsg fires after partitionTypeIdle to clear the digit
// buffer if no further input arrived. `gen` is captured at scheduling time
// so stale ticks (superseded by newer keystrokes) are ignored.
type partitionTypeTickMsg struct{ gen int }

func partitionTypeTickCmd(gen int) tea.Cmd {
	return tea.Tick(partitionTypeIdle, func(time.Time) tea.Msg {
		return partitionTypeTickMsg{gen: gen}
	})
}

func (m *Model) handlePartitionTypeTick(msg partitionTypeTickMsg) {
	if msg.gen != m.partitionTypeGen {
		return
	}
	m.partitionTypeBuf = ""
}

// popupNavKey reports whether key belongs to the segmented popup while it
// is open. tab/shift+tab are included so the popup is fully modal — they'd
// otherwise close it as a side effect of FocusNext/Prev. esc and the
// fullscreen toggles are deliberately excluded so they keep their
// NORMAL-mode meaning.
func popupNavKey(key tea.KeyPressMsg) bool {
	switch key.String() {
	case "enter", "up", "down", "left", "right", "j", "k", "h", "l", "tab", "shift+tab":
		return true
	}
	return false
}

// handleEscNormal implements the esc cascade: popup → close popup;
// fullscreen → split; otherwise → close form.
func (m *Model) handleEscNormal(key tea.KeyPressMsg) tea.Cmd {
	if m.form.PopupActive() && !m.fullscreen {
		f, cmd := m.form.Update(key)
		m.form = f
		return cmd
	}
	if m.fullscreen {
		m.setFullscreen(false)
		return nil
	}
	m.action.Back = true
	return nil
}

// enterInsertOnFocused decides what `enter` does in NORMAL based on the
// focused field's kind. Segmented opens a popup (its native behavior);
// list with no rows gets a fresh empty row first; everything else flips
// the mode flag and lets INSERT handle the next keystroke.
func (m *Model) enterInsertOnFocused(key tea.KeyPressMsg) tea.Cmd {
	fld := m.form.FocusedField()
	switch fld.Kind {
	case components.FieldSegmented:
		f, cmd := m.form.Update(key)
		m.form = f
		return cmd
	case components.FieldList:
		if len(fld.List) == 0 {
			m.form.AppendListRow()
		}
		m.setMode(ModeInsert)
		return nil
	default:
		m.setMode(ModeInsert)
		return nil
	}
}

// handleInsert is INSERT mode: typing inserts into the focused field;
// special keys (tab, shift+tab, enter, esc) implement commit / navigate /
// newline / leave-mode semantics.
func (m *Model) handleInsert(key tea.KeyPressMsg) tea.Cmd {
	switch key.String() {
	case "esc":
		// if the segmented popup is open, esc reverts/closes it but stays
		// in INSERT; otherwise it returns to NORMAL on the same field.
		if m.form.PopupActive() {
			f, cmd := m.form.Update(key)
			m.form = f
			return cmd
		}
		m.setMode(ModeNormal)
		return nil
	case "tab":
		return m.handleInsertTab()
	case "shift+tab":
		m.form.FocusPrev()
		m.setMode(ModeNormal)
		return nil
	case "enter":
		return m.handleInsertEnter(key)
	}
	f, cmd := m.form.Update(key)
	m.form = f
	// invariant for list editing: while INSERT is on a list, never let it
	// drop to zero rows — backspace on the only empty row would otherwise
	// leave the user "in INSERT but with nothing to edit". Re-seed an
	// empty row so they can keep typing without a surprise mode change.
	// Only `backspace` can shrink a list at the form level; other keys
	// can't, so don't pay the check cost on every keystroke.
	if key.String() == "backspace" {
		m.ensureListNotEmpty()
	}
	return cmd
}

// ensureListNotEmpty re-creates an empty row when the focused list became
// empty as a side effect of editing in INSERT. Only the explicit Enter on
// an empty row exits INSERT — implicit removals (ctrl+x, backspace on the
// last empty row) keep the user typing.
func (m *Model) ensureListNotEmpty() {
	if m.mode != ModeInsert || m.form.FocusedField().Kind != components.FieldList {
		return
	}
	if _, _, ok := m.form.FocusedListEntry(); !ok {
		m.form.AppendListRow()
	}
}

// handleInsertTab implements the textarea-vs-single-line tab split: in a
// textarea the tab is inserted as a literal `\t`; everywhere else it
// commits and navigates to the next field, returning to NORMAL.
func (m *Model) handleInsertTab() tea.Cmd {
	if m.form.FocusedField().Kind == components.FieldTextarea {
		m.form.InsertAtCursor("\t")
		return nil
	}
	m.form.FocusNext()
	m.setMode(ModeNormal)
	return nil
}

// handleInsertEnter implements the per-kind Enter semantics in INSERT:
//   - textarea: insert newline at cursor (stay in INSERT).
//   - list (Headers): chained-entry idiom — Enter on a non-empty row
//     commits and adds a fresh empty row to keep filling; Enter on an
//     empty row exits to NORMAL (signals "done adding").
//   - single-line text: commit and return to NORMAL on the same field.
func (m *Model) handleInsertEnter(key tea.KeyPressMsg) tea.Cmd {
	fld := m.form.FocusedField()
	switch fld.Kind {
	case components.FieldTextarea:
		f, cmd := m.form.Update(key)
		m.form = f
		return cmd
	case components.FieldList:
		// pressing enter on an empty row finishes the add-many loop.
		if entry, _, ok := m.form.FocusedListEntry(); ok && entry == "" {
			m.setMode(ModeNormal)
			return nil
		}
		// invalid current row blocks the chain — surface the reason as a
		// toast and stay in INSERT on the broken row so the user fixes it.
		if err := m.form.ValidateFocusedListEntry(); err != nil {
			m.toasts.Push(components.ToastWarning, "header invalid: "+err.Error())
			return nil
		}
		// otherwise commit-and-continue: add a new empty row and stay in
		// INSERT for sequential header entry.
		m.form.AppendListRow()
		return nil
	default:
		m.setMode(ModeNormal)
		return nil
	}
}

// handleInsertListShortcut covers the headers-only `ctrl+n` / `ctrl+x`
// shortcuts in INSERT: `ctrl+n` (new) jumps to the end of the list and
// starts a new empty row; `ctrl+x` (cut) deletes the focused row, then
// re-seeds an empty row when the list becomes empty so the user keeps
// editing instead of being kicked into NORMAL. Returns ok=true when the
// key was consumed. These take priority over the global history shortcut
// when the focused field is a list.
func (m *Model) handleInsertListShortcut(key tea.KeyPressMsg) (consumed bool) {
	switch key.String() {
	case "ctrl+n":
		m.form.AppendListRow()
		return true
	case "ctrl+x":
		m.form.RemoveListRow()
		m.ensureListNotEmpty()
		return true
	}
	return false
}

// send validates and dispatches a produce. closeAfter=true → ctrl+s (send &
// close); closeAfter=false → ctrl+shift+s (send & keep).
func (m *Model) send(closeAfter bool) tea.Cmd {
	if m.readOnly {
		m.toasts.Push(components.ToastWarning, "cluster is read-only — produce blocked")
		return nil
	}
	spec, err := m.spec()
	if err != nil {
		m.err = err.Error()
		return nil
	}
	m.err = ""
	m.sending = true
	return produceCmd(m.svc, spec, closeAfter)
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
// kafka.Header. Empty entries are skipped; invalid entries (validateHeader
// fails) yield an error so the user knows to fix them.
func parseHeaders(entries []string) ([]kafka.Header, error) {
	out := make([]kafka.Header, 0, len(entries))
	for _, e := range entries {
		entry := strings.TrimSpace(e)
		if entry == "" {
			continue
		}
		if err := validateHeader(entry); err != nil {
			return nil, err
		}
		idx := strings.IndexByte(entry, '=')
		out = append(out, kafka.Header{
			Key:   strings.TrimSpace(entry[:idx]),
			Value: []byte(entry[idx+1:]),
		})
	}
	return out, nil
}

// validateHeader is the per-entry rule shared by the inline form indicator
// and the send-time validation in parseHeaders. A header is valid when it
// has the shape `key=value` with a non-empty key (after trimming).
func validateHeader(entry string) error {
	trimmed := strings.TrimSpace(entry)
	idx := strings.IndexByte(trimmed, '=')
	if idx < 0 {
		return errors.New("must be key=value")
	}
	if strings.TrimSpace(trimmed[:idx]) == "" {
		return errors.New("key is empty")
	}
	return nil
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

// View renders the form body. The host wraps the screen in the standard
// frame (with `Title()` shown in the top border), so this method only emits
// the inner content: status lines, the form (or fullscreen tab strip), and
// the bottom hint.
func (m *Model) View() string {
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

	var parts []string
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
	return strings.Join(parts, "\n")
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
