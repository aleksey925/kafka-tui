// Package produce implements the produce form (§7.5) — the screen for
// sending a single record to a topic. The form lets the user pick a partition
// (auto/manual), compression codec, key, headers, and value, and supports
// resending a previously-received message with one click.
//
// Send & close (Ctrl+S) submits the record and signals the host to leave;
// Send & keep (Ctrl+Shift+S) submits without leaving — the form stays open
// for repeated produces. Ctrl+P / Ctrl+N walk through history; Ctrl+R clears
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

// History persists past produces and surfaces them for prefill / Ctrl+P /
// Ctrl+N. Production code wires this to the SQLite-backed store from
// Task 18; tests pass an in-memory implementation.
type History interface {
	// LastForTopic returns the most recent entry for `topic` (used to prefill
	// when the form is freshly opened). The bool is false when no history
	// exists for the topic.
	LastForTopic(topic string) (Entry, bool)
	// Recent returns up to n entries across all topics, newest-first. The
	// produce form walks this slice with Ctrl+P / Ctrl+N.
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
	// Back signals the user pressed Esc OR completed a "send & close".
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
	// Pager is the $EDITOR opener for the value field. nil disables Ctrl+E.
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

	form    *components.Form
	toasts  *components.Toasts
	err     string
	sending bool

	width, height int
	action        Action

	now    func() time.Time
	styles theme.Styles
}

// Field keys.
const (
	fieldTopic       = "topic"
	fieldPartition   = "partition"
	fieldCompression = "compression"
	fieldKey         = "key"
	fieldHeaders     = "headers"
	fieldValue       = "value"
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
// and by Ctrl+R to reset the entire form.
func (m *Model) buildForm() *components.Form {
	fields := []components.Field{
		{Key: fieldTopic, Label: "Topic", Kind: components.FieldText, Value: m.topic},
		{Key: fieldPartition, Label: "Partition (auto/<n>)", Kind: components.FieldText, Value: "auto"},
		{
			Key:     fieldCompression,
			Label:   "Compression",
			Kind:    components.FieldDropdown,
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

// Topic returns the topic the form is currently bound to.
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

// SetSize updates width/height (used by the layout chrome).
func (m *Model) SetSize(w, h int) { m.width, m.height = w, h }

// KeyHints returns the screen-specific hints shown at the bottom row.
func (m *Model) KeyHints() []layout.KeyHint {
	hints := []layout.KeyHint{
		{Key: "Tab", Label: "next field"},
		{Key: "Ctrl+S", Label: "send"},
		{Key: "Ctrl+Shift+S", Label: "send & keep"},
		{Key: "Ctrl+E", Label: "$EDITOR"},
		{Key: "Ctrl+P/N", Label: "history"},
		{Key: "Ctrl+R", Label: "clear"},
		{Key: "Esc", Label: "cancel"},
	}
	return hints
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
	switch key.String() {
	case "esc":
		m.action.Back = true
		return m, nil
	case "ctrl+s":
		return m.send(true)
	case "ctrl+shift+s":
		return m.send(false)
	case "ctrl+e":
		m.openEditor()
		return m, nil
	case "ctrl+r":
		m.clear()
		return m, nil
	case "ctrl+p":
		m.historyStep(+1)
		return m, nil
	case "ctrl+n":
		m.historyStep(-1)
		return m, nil
	}
	f, cmd := m.form.Update(key)
	m.form = f
	return m, cmd
}

// send validates and dispatches a produce. closeAfter=true → Ctrl+S (send &
// close); closeAfter=false → Ctrl+Shift+S (send & keep).
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
func (m *Model) spec() (kafka.ProduceSpec, error) {
	get := func(key string) string {
		fld, _ := m.form.Field(key)
		return strings.TrimSpace(fld.Value)
	}
	topic := get(fieldTopic)
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
// keeps the in-memory cache in sync so Ctrl+P/Ctrl+N find it instantly.
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
	// invalidate the in-memory cursor so the next Ctrl+P refetches.
	m.histBuf = nil
	m.histPos = -1
}

// clear resets every field back to its default state (Ctrl+R).
func (m *Model) clear() {
	m.form = m.buildForm()
	m.err = ""
	m.histPos = -1
	m.histBuf = nil
}

// historyStep moves the history cursor by `delta`. +1 = older (Ctrl+P), -1 =
// newer (Ctrl+N). Loads the History snapshot lazily on first use.
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
		// Ctrl+N stepped past the newest — reset to the empty form.
		m.form = m.buildForm()
		return
	}
	m.applyEntry(m.histBuf[pos], false)
}

// applyEntry overwrites the form fields with the entry's payload.
// resetPartitionToAuto matches the resend rule from §7.5: "partition resets
// to auto" so the user picks a destination explicitly.
func (m *Model) applyEntry(entry Entry, resetPartitionToAuto bool) {
	m.form.SetValue(fieldTopic, entry.Topic)
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
	m.form.SetValue(fieldTopic, msg.Topic)
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

// View renders the form body wrapped in the standard rounded box.
func (m *Model) View() string {
	header := m.styles.HelpTitle.Render("Produce → " + m.topic)
	hint := m.styles.HintLabel.Render("Tab navigate  Ctrl+S send  Ctrl+Shift+S send&keep  Ctrl+E editor  Ctrl+R clear  Esc cancel")
	parts := []string{header}
	if m.err != "" {
		parts = append(parts, m.styles.StatusErr.Render(m.err))
	}
	if m.sending {
		parts = append(parts, m.styles.StatusInfo.Render("sending…"))
	}
	parts = append(parts, m.form.View(), "", hint)
	if t := m.toasts.View(); t != "" {
		parts = append(parts, t)
	}
	body := strings.Join(parts, "\n")
	box := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		Padding(0, 2).
		Render(body)
	if m.width <= 0 {
		return box
	}
	return lipgloss.PlaceHorizontal(m.width, lipgloss.Center, box)
}

// ----- Messages -----

// ProduceResultMsg is dispatched after a produce call returns. Close is true
// when the request originated from Ctrl+S (send & close); false for
// Ctrl+Shift+S (send & keep).
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
		editor := os.Getenv("EDITOR")
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
		cmd := exec.CommandContext(context.Background(), editor, path) //nolint:gosec // user-controlled $EDITOR
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
