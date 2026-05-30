// Package produce implements the produce form for sending one record to a topic.
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

	"github.com/aleksey925/kafka-tui/internal/config"
	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/help"
	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/recordfmt"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// Service abstracts the Kafka calls the produce form needs.
type Service interface {
	Produce(ctx context.Context, spec kafka.ProduceSpec) (kafka.ProduceResult, error)
	TopicPartitions(ctx context.Context, topic string) ([]kafka.PartitionDetail, error)
}

// PagerOpener launches an external editor on the record (Key + Headers +
// Value, serialized via [encodeEditorBuffer]). Edit returns a [tea.Cmd]
// (not the edited bytes directly) so the real implementation can route
// through [tea.ExecProcess] — the only safe way to spawn a full-screen
// child process from inside bubbletea. A blocking exec.Cmd.Run() corrupts
// the terminal because the parent's raw mode / alt-screen / mouse tracking
// are not released, and the child fights bubbletea for stdin.
//
// The returned Cmd must eventually post an [EditorEditedMsg] back to the
// program.
type PagerOpener interface {
	Edit(initial []byte) tea.Cmd
}

type PagerOpenerFunc func(initial []byte) tea.Cmd

func (f PagerOpenerFunc) Edit(initial []byte) tea.Cmd { return f(initial) }

// EditorEditedMsg is the result of an external editor invocation. Exactly one
// of Content / Err is set: Content carries the edited bytes on success, Err
// carries any failure that occurred at any stage (tmpfile creation, exec
// failure, tmpfile read-back).
type EditorEditedMsg struct {
	Content []byte
	Err     error
}

type Action struct {
	Back bool
	Sent *kafka.ProduceResult
}

type Options struct {
	Service  Service
	Topic    string
	Cluster  string
	ReadOnly bool
	Pager    PagerOpener
	// PrefillFromMessage activates resend mode (partition is reset to auto).
	PrefillFromMessage *kafka.Message
	Now                func() time.Time
	Styles             theme.Styles
}

type Model struct {
	svc     Service
	topic   string
	cluster string

	readOnly bool
	pager    PagerOpener

	form        *components.Form
	toasts      *components.Toasts
	sendConfirm *components.SendConfirm
	err         string
	sending     bool
	fullscreen  bool
	mode        Mode

	// partition type-to-jump: digits accumulate in partitionTypeBuf to
	// select the matching option live; partitionTypeGen invalidates
	// stale tick callbacks.
	partitionTypeBuf string
	partitionTypeGen int

	// width/height of the area the host gives this screen, propagated to
	// the form so bounded fields (Value textarea, Headers list) can size
	// their viewports against the terminal instead of relying on a hardcoded
	// row count.
	width, height int

	action Action

	styles theme.Styles
}

// Topic is intentionally not a form field — it's fixed by the caller
// (header shows "Produce · <topic>").
const (
	fieldPartition   = "partition"
	fieldCompression = "compression"
	fieldKey         = "key"
	fieldHeaders     = "headers"
	fieldValue       = "value"

	partitionAuto = "auto"

	partitionTypeIdle = 700 * time.Millisecond
)

// Mode tracks vim-style edit modes for the produce form.
type Mode int

const (
	ModeNormal Mode = iota
	ModeInsert
)

func New(opts Options) *Model {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	styles := opts.Styles
	if styles.Palette.Foreground == nil {
		styles = theme.DefaultStyles()
	}

	m := &Model{
		svc:      opts.Service,
		topic:    opts.Topic,
		cluster:  opts.Cluster,
		readOnly: opts.ReadOnly,
		pager:    opts.Pager,
		toasts:   components.NewToasts(components.WithToastClock(now), components.WithToastStyles(styles)),
		styles:   styles,
	}
	m.form = m.buildForm()
	m.form.SetEditing(false)

	if opts.PrefillFromMessage != nil {
		m.applyMessage(*opts.PrefillFromMessage)
	}
	return m
}

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
		{Key: fieldHeaders, Label: "Headers (key=value)", Kind: components.FieldList, Validator: recordfmt.ValidateHeaderRow},
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

func (m *Model) Init() tea.Cmd {
	if m.topic == "" {
		return nil
	}
	return loadPartitionsCmd(m.svc, m.topic)
}

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
	if msg.topic != m.topic {
		return
	}
	if msg.err != nil {
		m.toasts.Push(components.ToastWarning, "partitions: "+msg.err.Error())
		return
	}
	m.form.SetOptions(fieldPartition, partitionOptions(msg.partitions))
}

func partitionOptions(ids []int32) []string {
	out := make([]string, 0, len(ids)+1)
	out = append(out, partitionAuto)
	for _, id := range ids {
		out = append(out, strconv.FormatInt(int64(id), 10))
	}
	return out
}

func (m *Model) Topic() string { return m.topic }

func (m *Model) Form() *components.Form { return m.form }

func (m *Model) Toasts() *components.Toasts { return m.toasts }

func (m *Model) LatestFlash() (components.Toast, bool) {
	if m.toasts == nil {
		return components.Toast{}, false
	}
	return m.toasts.Latest()
}

func (m *Model) Title() string {
	return "Produce · " + m.topic
}

func (m *Model) Breadcrumb() string { return "" }

func (m *Model) Action() Action { return m.action }

func (m *Model) ConsumeAction() Action {
	a := m.action
	m.action = Action{}
	return a
}

func (m *Model) Sending() bool { return m.sending }

// WantsRawInput is true only in INSERT — NORMAL ignores letters/digits
// anyway, so leaving raw-input off there preserves global shortcuts
// (:, /, ?). INSERT restores raw-input for literal text input.
func (m *Model) WantsRawInput() bool { return m.mode == ModeInsert }

// SendConfirmOpen reports whether the send confirm modal is currently
// mounted. Exposed for tests.
func (m *Model) SendConfirmOpen() bool { return m.sendConfirm != nil }

// HasOverlay is always true so the host's q/esc fallback yields to the
// form (a stray `q` in NORMAL must not pop the screen mid-edit). The
// form's own esc handler raises action.Back to close cleanly.
func (m *Model) HasOverlay() bool { return true }

func (m *Model) SetSize(w, h int) {
	m.width, m.height = w, h
}

func (m *Model) KeyHints() []layout.KeyHint {
	return layout.HintsFromBindings(m.bindings())
}

func (m *Model) HelpSections() []help.Section {
	return help.SectionsFromBindings(m.bindings())
}

// bindings returns the keymap surfaced to help/hints. While the send
// confirm modal is open, only its keys are advertised so the bar matches
// what actually fires.
func (m *Model) bindings() []keymap.Binding {
	if m.sendConfirm != nil {
		return m.sendConfirm.Bindings("Send")
	}
	return m.normalBindings()
}

// normalBindings are NOT consulted in INSERT so tab/enter/esc retain
// their text-editing meaning. Letter shortcuts (e, s) live here because
// in INSERT they are literal text; ctrl+u lives here too because in
// INSERT it is the readline kill-to-line-start handled by the
// lineedit-backed form.
func (m *Model) normalBindings() []keymap.Binding {
	return []keymap.Binding{
		{Keys: []string{"+", "_", "shift++", "shift+-"}, DisplayKeys: []string{"+", "_"}, Label: "toggle fullscreen", Category: "Form", Hint: true, Handler: m.actToggleFullscreen},
		{Keys: []string{"tab", "down", "j"}, Label: "next field", Category: "Form", Hint: true, Handler: m.actFocusNext},
		{Keys: []string{"shift+tab", "up", "k"}, Label: "previous field", Category: "Form", Hint: true, Handler: m.actFocusPrev},
		{Keys: []string{"ctrl+u"}, Label: "clear form", Category: "Form", Hint: true, Handler: m.actClear},
		{Keys: []string{"s"}, Label: "send", Category: "Produce", Hint: true, Handler: m.actOpenSendConfirm},
		{Keys: []string{"e"}, Label: "open record in $EDITOR", Category: "Produce", Hint: true, Handler: m.actEditor},
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

func (m *Model) actEditor() tea.Cmd { return m.openEditor() }

// actOpenSendConfirm gates the produce: read-only clusters short-circuit
// with a toast, an invalid spec surfaces inline so the user fixes it
// before being asked to confirm, and a send already in flight blocks
// re-entry. Only after these guards do we mount the modal that lets the
// user choose between send-and-close and send-and-keep.
func (m *Model) actOpenSendConfirm() tea.Cmd {
	if m.readOnly {
		m.toasts.Push(components.ToastWarning, "cluster is read-only — produce blocked")
		return nil
	}
	if m.sending {
		return nil
	}
	if _, err := m.spec(); err != nil {
		m.err = err.Error()
		return nil
	}
	m.err = ""
	m.sendConfirm = components.NewSendConfirm(m.cluster, m.topic, components.WithSendConfirmStyles(m.styles))
	return nil
}

// actClear yields to an open segmented popup — clearing the form under it
// would silently wipe both the popup choice and every other field the user
// already filled in. The user can close the popup (esc) and clear after.
func (m *Model) actClear() tea.Cmd {
	if m.form.PopupActive() {
		return nil
	}
	m.clear()
	return nil
}

func (m *Model) Fullscreen() bool { return m.fullscreen }

func (m *Model) Mode() Mode { return m.mode }

func (m *Model) setMode(target Mode) {
	m.mode = target
	m.form.SetEditing(target == ModeInsert)
	if target == ModeInsert {
		m.form.SetFocusedSuffix("[EDIT]")
	} else {
		m.form.SetFocusedSuffix("")
	}
}

// setFullscreen forces the Compression segmented field into popup view
// while fullscreen, returning to the compact slider on exit.
func (m *Model) setFullscreen(on bool) {
	m.fullscreen = on
	m.form.SetSegmentedPopup(fieldCompression, on)
}

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
	case EditorEditedMsg:
		m.handleEditorResult(msg)
		return nil
	case tea.PasteMsg:
		m.handlePaste(msg)
		return nil
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	}
	return nil
}

// handlePaste injects the pasted text into the focused text field, auto
// switching from NORMAL to INSERT so the user lands in the field they just
// pasted into. Non-text fields (segmented) silently drop the paste — its
// content has no meaning for an option picker.
//
// While the send confirm modal is up the form is uneditable: the modal
// owns input, and a paste leaking through would silently corrupt the
// record the user is about to confirm.
func (m *Model) handlePaste(msg tea.PasteMsg) {
	if m.sendConfirm != nil {
		return
	}
	kind := m.form.FocusedField().Kind
	if kind != components.FieldText && kind != components.FieldTextarea && kind != components.FieldList {
		return
	}
	f, _ := m.form.Update(msg)
	m.form = f
	if m.mode != ModeInsert {
		m.setMode(ModeInsert)
	}
}

func (m *Model) handleKey(key tea.KeyPressMsg) tea.Cmd {
	if m.sendConfirm != nil {
		return m.handleSendConfirmKey(key)
	}
	if m.toasts != nil {
		_, _ = m.toasts.Update(key)
	}
	// list shortcuts (ctrl+n add row, ctrl+x remove row) live only on the
	// Headers list in INSERT.
	if m.mode == ModeInsert && m.form.FocusedField().Kind == components.FieldList {
		if m.handleInsertListShortcut(key) {
			return nil
		}
	}
	if m.mode == ModeInsert {
		return m.handleInsert(key)
	}
	return m.handleNormal(key)
}

// handleSendConfirmKey routes one keypress to the open send confirm
// modal; y/k commit the produce (close form / keep open), n or esc
// dismiss the modal without sending.
func (m *Model) handleSendConfirmKey(key tea.KeyPressMsg) tea.Cmd {
	c, _ := m.sendConfirm.Update(key)
	m.sendConfirm = c
	switch c.Result() {
	case components.SendConfirmPending:
		return nil
	case components.SendConfirmYesClose:
		m.sendConfirm = nil
		return m.send(true)
	case components.SendConfirmYesKeep:
		m.sendConfirm = nil
		return m.send(false)
	case components.SendConfirmNo:
		m.sendConfirm = nil
	}
	return nil
}

// handleNormal: when a segmented popup is open, nav keys (enter, arrows,
// hjkl, tab) go to the popup first; esc and fullscreen toggles keep
// NORMAL semantics so esc cascade closes the popup and `_` collapses
// fullscreen.
func (m *Model) handleNormal(key tea.KeyPressMsg) tea.Cmd {
	if m.form.PopupActive() && popupNavKey(key) {
		f, cmd := m.form.Update(key)
		m.form = f
		return cmd
	}
	if cmd, ok := keymap.Dispatch(m.normalBindings(), key); ok {
		return cmd
	}
	// segmented fields are interactive without INSERT — left/right/hjkl
	// cycle the value live.
	kind := m.form.FocusedField().Kind
	if kind == components.FieldSegmented {
		if cmd, handled := m.handlePartitionTypeJump(key); handled {
			return cmd
		}
		f, cmd := m.form.Update(key)
		m.form = f
		return cmd
	}
	// bounded fields: in NORMAL the remaining scroll keys (pgup/pgdn,
	// ctrl+b/f, home/end, h/l) pan the visible window without entering
	// INSERT. j/k/up/down/tab/shift+tab are claimed by normalBindings above
	// for field-nav, so they never reach the field's viewport here.
	if kind == components.FieldTextarea || kind == components.FieldList {
		if m.form.HandleViewportKey(key) {
			return nil
		}
	}
	return nil
}

// handlePartitionTypeJump: digits accumulate to live-select the matching
// option. Reconciliation is (1) extend buffer if some option still starts
// with it; (2) restart from the digit alone if any option starts with it;
// (3) eat the keystroke (so the picker doesn't blink on out-of-range
// numbers). Returns (cmd, true) when the key was consumed.
func (m *Model) handlePartitionTypeJump(key tea.KeyPressMsg) (tea.Cmd, bool) {
	if m.form.FocusedField().Key != fieldPartition {
		return nil, false
	}
	s := key.String()
	if len(s) != 1 || s[0] < '0' || s[0] > '9' {
		// non-digit invalidates the buffer; left/right/etc. still cycle.
		m.resetPartitionTypeBuf()
		return nil, false
	}
	opts := m.form.FocusedField().Options
	candidate := m.partitionTypeBuf + s
	switch {
	case slices.ContainsFunc(opts, func(o string) bool { return strings.HasPrefix(o, candidate) }):
		m.partitionTypeBuf = candidate
	case slices.ContainsFunc(opts, func(o string) bool { return strings.HasPrefix(o, s) }):
		candidate = s
		m.partitionTypeBuf = candidate
	default:
		// bump the idle timer so it tracks "time since last keystroke",
		// not "time since last successful match".
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

// partitionTypeTickMsg.gen is captured at scheduling time so stale ticks
// (superseded by newer keystrokes) are ignored.
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

// popupNavKey: tab/shift+tab are included so the popup is fully modal
// (otherwise FocusNext/Prev would close it). esc/fullscreen are excluded
// so they keep their NORMAL-mode meaning.
func popupNavKey(key tea.KeyPressMsg) bool {
	switch key.String() {
	case "enter", "up", "down", "left", "right", "j", "k", "h", "l", "tab", "shift+tab":
		return true
	}
	return false
}

// handleEscNormal: popup → close popup; fullscreen → split; else → close form.
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

func (m *Model) handleInsert(key tea.KeyPressMsg) tea.Cmd {
	switch key.String() {
	case "esc":
		// segmented popup open: esc closes it but stays in INSERT.
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
	// invariant: never let a focused list in INSERT drop to zero rows;
	// backspace on the last empty row would leave INSERT with nothing to
	// edit. Only backspace can shrink a list at this level.
	if key.String() == "backspace" {
		m.ensureListNotEmpty()
	}
	return cmd
}

// ensureListNotEmpty: only an explicit Enter on an empty row exits INSERT;
// implicit removals (ctrl+x, backspace) keep the user typing.
func (m *Model) ensureListNotEmpty() {
	if m.mode != ModeInsert || m.form.FocusedField().Kind != components.FieldList {
		return
	}
	if _, _, ok := m.form.FocusedListEntry(); !ok {
		m.form.AppendListRow()
	}
}

func (m *Model) handleInsertTab() tea.Cmd {
	if m.form.FocusedField().Kind == components.FieldTextarea {
		m.form.InsertAtCursor("\t")
		return nil
	}
	m.form.FocusNext()
	m.setMode(ModeNormal)
	return nil
}

// handleInsertEnter:
//   - textarea: insert newline (stay in INSERT).
//   - list: chained entry — non-empty row commits and adds a fresh row;
//     empty row exits to NORMAL.
//   - text: commit and return to NORMAL.
func (m *Model) handleInsertEnter(key tea.KeyPressMsg) tea.Cmd {
	fld := m.form.FocusedField()
	switch fld.Kind {
	case components.FieldTextarea:
		f, cmd := m.form.Update(key)
		m.form = f
		return cmd
	case components.FieldList:
		if entry, _, ok := m.form.FocusedListEntry(); ok && entry == "" {
			m.setMode(ModeNormal)
			return nil
		}
		// invalid row blocks the chain — surface the reason as a toast.
		if err := m.form.ValidateFocusedListEntry(); err != nil {
			m.toasts.Push(components.ToastWarning, "header invalid: "+err.Error())
			return nil
		}
		m.form.AppendListRow()
		return nil
	default:
		m.setMode(ModeNormal)
		return nil
	}
}

// handleInsertListShortcut covers the headers-only ctrl+n (new row) and
// ctrl+x (cut) in INSERT.
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
	m.action.Sent = &r
	if msg.Close {
		m.action.Back = true
	}
}

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
	rawCompression := get(fieldCompression)
	normCompression := rawCompression
	if rawCompression != "" {
		norm, ok := config.NormalizeEnum(rawCompression, config.AllowedCompressions)
		if !ok {
			return kafka.ProduceSpec{}, fmt.Errorf("compression: unknown value %q", rawCompression)
		}
		normCompression = norm
	}
	codec, err := kafka.ParseCompression(normCompression)
	if err != nil {
		return kafka.ProduceSpec{}, fmt.Errorf("compression: %w", err)
	}

	headersField, _ := m.form.Field(fieldHeaders)
	headers, err := recordfmt.ParseHeaderRows(headersField.List)
	if err != nil {
		return kafka.ProduceSpec{}, fmt.Errorf("headers: %w", err)
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

func (m *Model) clear() {
	m.form.Reset()
	m.setMode(m.mode)
	m.resetPartitionTypeBuf()
	m.err = ""
}

// applyMessage seeds the form from a resent record. Partition resets to
// "auto" rather than copying msg.Partition — the source partition is rarely
// the right destination, so we force an explicit pick.
func (m *Model) applyMessage(msg kafka.Message) {
	m.topic = msg.Topic
	m.form.SetValue(fieldPartition, "auto")
	m.form.SetValue(fieldKey, string(msg.Key))
	m.form.SetList(fieldHeaders, formatHeaderList(msg.Headers))
	m.form.SetValue(fieldValue, string(msg.Value))
}

func formatHeaderList(headers []kafka.Header) []string {
	out := make([]string, 0, len(headers))
	for _, h := range headers {
		out = append(out, h.Key+"="+string(h.Value))
	}
	return out
}

// openEditor produces the handoff Cmd; the result arrives later as
// [EditorEditedMsg], handled by [handleEditorResult]. The buffer carries
// the full record (Key + Headers + Value) — see [encodeEditorBuffer].
// Invalid header rows in the form are surfaced as a toast instead of
// being smuggled into the editor session.
func (m *Model) openEditor() tea.Cmd {
	if m.pager == nil {
		m.toasts.Push(components.ToastWarning, "editor: no $EDITOR opener configured")
		return nil
	}
	keyFld, _ := m.form.Field(fieldKey)
	headersFld, _ := m.form.Field(fieldHeaders)
	valFld, _ := m.form.Field(fieldValue)
	headers, err := recordfmt.ParseHeaderRows(headersFld.List)
	if err != nil {
		m.toasts.Push(components.ToastError, "editor: invalid header: "+err.Error())
		return nil
	}
	buf := recordfmt.Encode(keyFld.Value, headers, []byte(valFld.Value))
	return m.pager.Edit(buf)
}

func (m *Model) handleEditorResult(msg EditorEditedMsg) {
	if msg.Err != nil {
		m.toasts.Push(components.ToastError, "editor: "+msg.Err.Error())
		return
	}
	key, headers, value, err := recordfmt.Parse(msg.Content)
	if err != nil {
		m.toasts.Push(components.ToastError, "editor: parse failed: "+err.Error())
		return
	}
	m.form.SetValue(fieldKey, key)
	m.form.SetList(fieldHeaders, formatHeaderList(headers))
	m.form.SetValue(fieldValue, string(value))
}

func (m *Model) View() string {
	if m.sendConfirm != nil {
		return m.sendConfirm.View(m.width, m.height)
	}
	hint := components.HintLine(m.styles, m.hintEntries()...)

	chromeAbove := 0
	if m.err != "" {
		chromeAbove++
	}
	if m.sending {
		chromeAbove++
	}
	// chrome below the body: blank line + hint line.
	const chromeBelow = 2
	formHeight := max(m.height-chromeAbove-chromeBelow, 1)
	// fullscreen draws its own tab row + spacer above the focused field; the
	// form area shrinks by 2 so the single rendered field gets accurate
	// elastic height (tabs are part of the screen chrome, not the form).
	if m.fullscreen {
		m.form.SetSize(m.width, formHeight-2)
	} else {
		m.form.SetSize(m.width, formHeight)
	}

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

var fieldOrder = []string{
	fieldPartition, fieldCompression, fieldKey, fieldHeaders, fieldValue,
}

var fieldLabel = map[string]string{
	fieldPartition:   "Partition",
	fieldCompression: "Compression",
	fieldKey:         "Key",
	fieldHeaders:     "Headers",
	fieldValue:       "Value",
}

func (m *Model) hintEntries() []components.Hint {
	switch {
	case m.mode == ModeInsert:
		return []components.Hint{
			{Label: "type to edit"},
			{Key: "tab", Label: "next"},
			{Key: "enter", Label: "commit/newline"},
			{Key: "esc", Label: "back to NORMAL"},
			{Label: "readline:"},
			{Key: "ctrl+a/e"},
			{Key: "ctrl+u/k"},
			{Key: "ctrl+w"},
			{Label: "on headers:"},
			{Key: "ctrl+n", Label: "add row"},
			{Key: "ctrl+x", Label: "remove row"},
		}
	case m.fullscreen:
		return []components.Hint{
			{Key: "tab/shift+tab", Label: "cycle field"},
			{Key: "enter", Label: "edit"},
			{Key: "+/_", Label: "exit fullscreen"},
			{Key: "s", Label: "send"},
			{Key: "ctrl+u", Label: "clear"},
			{Key: "e", Label: "$EDITOR"},
			{Key: "esc", Label: "back to split"},
		}
	default:
		return []components.Hint{
			{Key: "tab/shift+tab", Label: "navigate"},
			{Key: "enter", Label: "edit"},
			{Key: "+/_", Label: "fullscreen"},
			{Key: "s", Label: "send"},
			{Key: "ctrl+u", Label: "clear form"},
			{Key: "e", Label: "$EDITOR"},
			{Key: "esc", Label: "cancel"},
		}
	}
}

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

// ProduceResultMsg.Close is true for the "send & close" confirm answer
// (y), false for "send & keep form open" (k).
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

// DefaultPagerOpener writes the current value into a tmpfile and hands the
// terminal off to $EDITOR via the contract defined on [PagerOpener].
//
// I/O wiring (stdin/stdout/stderr) is intentionally NOT set here — bubbletea
// fills in the program's own streams when they are unset.
func DefaultPagerOpener() PagerOpener {
	return PagerOpenerFunc(func(initial []byte) tea.Cmd {
		editor := strings.TrimSpace(os.Getenv("EDITOR"))
		if editor == "" {
			editor = "vi"
		}
		tmp, err := os.CreateTemp("", "kafka-tui-produce-*.txt")
		if err != nil {
			return editorErrorCmd(fmt.Errorf("create temp: %w", err))
		}
		path := tmp.Name()
		if _, werr := tmp.Write(initial); werr != nil {
			_ = tmp.Close()
			_ = os.Remove(path)
			return editorErrorCmd(fmt.Errorf("write temp: %w", werr))
		}
		if cerr := tmp.Close(); cerr != nil {
			_ = os.Remove(path)
			return editorErrorCmd(fmt.Errorf("close temp: %w", cerr))
		}
		parts := strings.Fields(editor)
		args := append([]string(nil), parts[1:]...)
		args = append(args, path)
		execCmd := exec.CommandContext(context.Background(), parts[0], args...) //nolint:gosec // user-controlled $EDITOR
		return tea.ExecProcess(execCmd, func(runErr error) tea.Msg {
			defer os.Remove(path)
			if runErr != nil {
				return EditorEditedMsg{Err: fmt.Errorf("run: %w", runErr)}
			}
			out, rerr := os.ReadFile(path) //nolint:gosec // path is the tmpfile we just created
			if rerr != nil {
				return EditorEditedMsg{Err: fmt.Errorf("read result: %w", rerr)}
			}
			return EditorEditedMsg{Content: out}
		})
	})
}

// editorErrorCmd posts an EditorEditedMsg carrying err on the next tick so
// callers can keep returning a tea.Cmd from preparation paths that failed
// before exec.
func editorErrorCmd(err error) tea.Cmd {
	return func() tea.Msg { return EditorEditedMsg{Err: err} }
}
