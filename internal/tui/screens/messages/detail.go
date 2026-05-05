package messages

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/help"
	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// Clipboard abstracts the OSC 52 / native clipboard subsystem.
type Clipboard interface {
	Copy(ctx context.Context, payload string) error
}

// FileWriter abstracts atomic file writes used by the save hotkeys.
type FileWriter interface {
	Write(path string, data []byte) error
}

type FileWriterFunc func(path string, data []byte) error

func (f FileWriterFunc) Write(path string, data []byte) error { return f(path, data) }

// PagerOpener opens a temp file in $EDITOR.
type PagerOpener interface {
	Open(path string) error
}

type PagerOpenerFunc func(path string) error

func (f PagerOpenerFunc) Open(path string) error { return f(path) }

// DetailAction is the host-facing intent of the detail view.
type DetailAction struct {
	Back               bool
	Produce            string
	PrefillFromMessage *kafka.Message
	Toast              string
	Warn               string
}

type DetailOptions struct {
	Messages   []kafka.Message
	Index      int
	ReadOnly   bool
	Clipboard  Clipboard
	FileWriter FileWriter
	Pager      PagerOpener
	OutputDir  string
	// Wrap is the initial soft-wrap mode; parent passes its remembered
	// value so user preference survives detail re-opens.
	Wrap   bool
	Now    func() time.Time
	Styles theme.Styles
}

type DetailModel struct {
	messages  []kafka.Message
	index     int
	readOnly  bool
	clipboard Clipboard
	writer    FileWriter
	pager     PagerOpener
	outputDir string
	view      ValueView
	action    DetailAction
	styles    theme.Styles
	now       func() time.Time

	width, height int
	wrap          bool
	vScroll       int
	hScroll       int
	gPrimed       bool
	// cached layout geometry refreshed by [layout]; consumed by scroll math
	// so jumps work without first calling View().
	totalLines   int
	maxLineWidth int
}

func NewDetailModel(opts DetailOptions) *DetailModel {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	styles := opts.Styles
	if styles.Palette.Foreground == nil {
		styles = theme.DefaultStyles()
	}
	writer := opts.FileWriter
	if writer == nil {
		writer = FileWriterFunc(defaultWriteFile)
	}
	pager := opts.Pager
	if pager == nil {
		pager = DefaultPagerOpener()
	}
	dir := opts.OutputDir
	if dir == "" {
		dir, _ = os.Getwd()
	}
	idx := max(opts.Index, 0)
	if idx >= len(opts.Messages) {
		idx = len(opts.Messages) - 1
	}
	idx = max(idx, 0)
	return &DetailModel{
		messages:  append([]kafka.Message(nil), opts.Messages...),
		index:     idx,
		readOnly:  opts.ReadOnly,
		clipboard: opts.Clipboard,
		writer:    writer,
		pager:     pager,
		outputDir: dir,
		view:      ViewAuto,
		styles:    styles,
		now:       now,
		wrap:      opts.Wrap,
	}
}

func (d *DetailModel) SetSize(w, h int) {
	d.width, d.height = w, h
	d.layout()
}

func (d *DetailModel) Wrap() bool { return d.wrap }

func (d *DetailModel) ScrollOffset() int { return d.vScroll }

func (d *DetailModel) HScrollOffset() int { return d.hScroll }

// ScrollSummary returns 1-based first/last visible line plus total. ok=false
// when geometry is unknown.
func (d *DetailModel) ScrollSummary() (first, last, total int, ok bool) {
	if d.totalLines == 0 || d.height <= 0 {
		return 0, 0, 0, false
	}
	last = min(d.vScroll+d.height, d.totalLines)
	return d.vScroll + 1, last, d.totalLines, true
}

func (d *DetailModel) Action() DetailAction { return d.action }

func (d *DetailModel) ConsumeAction() DetailAction {
	a := d.action
	d.action = DetailAction{}
	return a
}

func (d *DetailModel) Index() int { return d.index }

func (d *DetailModel) ViewMode() ValueView { return d.view }

func (d *DetailModel) Current() kafka.Message {
	if len(d.messages) == 0 {
		return kafka.Message{}
	}
	return d.messages[d.index]
}

func (d *DetailModel) KeyHints() []layout.KeyHint {
	return layout.HintsFromBindings(d.bindings())
}

func (d *DetailModel) HelpSections() []help.Section {
	return help.SectionsFromBindings(d.bindings())
}

func (d *DetailModel) bindings() []keymap.Binding {
	bs := []keymap.Binding{
		{Keys: []string{"n"}, Label: "next message", Category: "Browse", Hint: true, Handler: d.actNext},
		{Keys: []string{"p"}, Label: "previous message", Category: "Browse", Hint: true, Handler: d.actPrev},
		{Keys: []string{"esc", "q"}, Label: "back", Category: "Browse", Handler: d.actBack},

		{Keys: []string{"1"}, Label: "JSON view", Category: "View", Hint: true, Handler: d.actViewJSON},
		{Keys: []string{"2"}, Label: "raw view", Category: "View", Hint: true, Handler: d.actViewRaw},
		{Keys: []string{"3"}, Label: "hex view", Category: "View", Hint: true, Handler: d.actViewHex},
		{Keys: []string{"w"}, Label: "toggle wrap", Category: "View", Hint: true, Handler: d.actToggleWrap},

		{Keys: []string{"j", "down"}, Label: "scroll down", Category: "Movement", Handler: d.actScrollDown},
		{Keys: []string{"k", "up"}, Label: "scroll up", Category: "Movement", Handler: d.actScrollUp},
		{Keys: []string{"ctrl+f", "pgdown"}, Label: "page down", Category: "Movement", Handler: d.actPageDown},
		{Keys: []string{"ctrl+b", "pgup"}, Label: "page up", Category: "Movement", Handler: d.actPageUp},
		// `gg` is a two-key chord — first press arms, second fires.
		{Keys: []string{"g"}, Label: "scroll to top (gg)", Category: "Movement", Handler: d.actChordG},
		{Keys: []string{"G", "end"}, Label: "scroll to bottom", Category: "Movement", Handler: d.actScrollBottom},
		{Keys: []string{"home"}, Label: "scroll to top", Category: "Movement", Handler: d.actScrollTop},
		{Keys: []string{"h", "left"}, Label: "scroll left (no-wrap)", Category: "Movement", Handler: d.actHScrollLeft},
		{Keys: []string{"l", "right"}, Label: "scroll right (no-wrap)", Category: "Movement", Handler: d.actHScrollRight},

		{Keys: []string{"y"}, Label: "copy record", Category: "Export", Hint: true, Handler: d.actCopy},
		{Keys: []string{"s"}, Label: "save value to file", Category: "Export", Hint: true, Handler: d.actSaveValue},
		{Keys: []string{"S"}, Label: "save full JSON", Category: "Export", Handler: d.actSaveFull},
		{Keys: []string{"e"}, Label: "open in $EDITOR", Category: "Export", Handler: d.actEditor},
	}
	// `R` stays bound in read-only mode so resend() can warn explicitly
	// instead of being a silent no-op. Hint/Category cleared so it doesn't
	// surface in help or the hints bar.
	resendBinding := keymap.Binding{
		Keys: []string{"R"}, Label: "resend message",
		Category: "Produce", Hint: true, Handler: d.actResend,
	}
	if d.readOnly {
		resendBinding.Category = ""
		resendBinding.Hint = false
	}
	bs = append(bs, resendBinding)
	return bs
}

func (d *DetailModel) actNext() tea.Cmd       { d.move(+1); return nil }
func (d *DetailModel) actPrev() tea.Cmd       { d.move(-1); return nil }
func (d *DetailModel) actBack() tea.Cmd       { d.action.Back = true; return nil }
func (d *DetailModel) actViewJSON() tea.Cmd   { d.setView(ViewJSON); return nil }
func (d *DetailModel) actViewRaw() tea.Cmd    { d.setView(ViewRaw); return nil }
func (d *DetailModel) actViewHex() tea.Cmd    { d.setView(ViewHex); return nil }
func (d *DetailModel) actToggleWrap() tea.Cmd { d.toggleWrap(); return nil }
func (d *DetailModel) actCopy() tea.Cmd       { d.copyRecord(); return nil }
func (d *DetailModel) actSaveValue() tea.Cmd  { d.saveValue(); return nil }
func (d *DetailModel) actSaveFull() tea.Cmd   { d.saveFullJSON(); return nil }
func (d *DetailModel) actEditor() tea.Cmd     { d.openEditor(); return nil }
func (d *DetailModel) actResend() tea.Cmd     { d.resend(); return nil }

func (d *DetailModel) actScrollDown() tea.Cmd   { d.scrollBy(+1); return nil }
func (d *DetailModel) actScrollUp() tea.Cmd     { d.scrollBy(-1); return nil }
func (d *DetailModel) actPageDown() tea.Cmd     { d.scrollBy(+d.pageStep()); return nil }
func (d *DetailModel) actPageUp() tea.Cmd       { d.scrollBy(-d.pageStep()); return nil }
func (d *DetailModel) actScrollBottom() tea.Cmd { d.scrollBottom(); return nil }
func (d *DetailModel) actScrollTop() tea.Cmd    { d.scrollTop(); return nil }

func (d *DetailModel) actHScrollLeft() tea.Cmd {
	if !d.wrap {
		d.hScrollBy(-d.hStep())
	}
	return nil
}

func (d *DetailModel) actHScrollRight() tea.Cmd {
	if !d.wrap {
		d.hScrollBy(+d.hStep())
	}
	return nil
}

func (d *DetailModel) actChordG() tea.Cmd {
	if d.gPrimed {
		d.gPrimed = false
		d.scrollTop()
		return nil
	}
	d.gPrimed = true
	return nil
}

func (d *DetailModel) Update(msg tea.Msg) (*DetailModel, tea.Cmd) {
	key, ok := msg.(tea.KeyPressMsg)
	if !ok {
		return d, nil
	}
	// non-`g` disarms the gg chord.
	if d.gPrimed && key.String() != "g" {
		d.gPrimed = false
	}
	keymap.Dispatch(d.bindings(), key)
	return d, nil
}

func (d *DetailModel) move(delta int) {
	if len(d.messages) == 0 {
		return
	}
	d.index = clampInt(d.index+delta, 0, len(d.messages)-1)
	d.resetScroll()
	d.layout()
}

func (d *DetailModel) setView(v ValueView) {
	if d.view == v {
		return
	}
	d.view = v
	d.resetScroll()
	d.layout()
}

func (d *DetailModel) toggleWrap() {
	d.wrap = !d.wrap
	// hScroll is meaningless in wrap mode; vScroll is normalised by clamp.
	d.hScroll = 0
	d.layout()
}

// layout produces visible lines and refreshes cached geometry in lock-step
// so a subsequent scroll key sees the same numbers the next frame renders.
func (d *DetailModel) layout() []string {
	if len(d.messages) == 0 {
		d.totalLines, d.maxLineWidth = 0, 0
		return nil
	}
	lines := d.layoutLines(d.renderFullBody())
	d.totalLines = len(lines)
	d.maxLineWidth = 0
	for _, line := range lines {
		if w := ansi.StringWidth(line); w > d.maxLineWidth {
			d.maxLineWidth = w
		}
	}
	d.clampScroll()
	return lines
}

func (d *DetailModel) resetScroll() {
	d.vScroll = 0
	d.hScroll = 0
}

func (d *DetailModel) pageStep() int {
	if d.height <= 1 {
		return 1
	}
	return d.height - 1
}

func (d *DetailModel) hStep() int {
	if d.width <= 4 {
		return 1
	}
	return d.width / 4
}

func (d *DetailModel) scrollBy(delta int) {
	d.vScroll += delta
	d.clampScroll()
}

func (d *DetailModel) hScrollBy(delta int) {
	d.hScroll += delta
	d.clampScroll()
}

func (d *DetailModel) scrollTop() {
	d.vScroll = 0
}

func (d *DetailModel) scrollBottom() {
	if d.totalLines <= d.height || d.height <= 0 {
		d.vScroll = 0
		return
	}
	d.vScroll = d.totalLines - d.height
}

func (d *DetailModel) clampScroll() {
	d.vScroll = max(d.vScroll, 0)
	if d.totalLines > 0 && d.height > 0 && d.vScroll > d.totalLines-d.height {
		d.vScroll = max(d.totalLines-d.height, 0)
	}
	d.hScroll = max(d.hScroll, 0)
	if d.width > 0 && d.maxLineWidth > 0 && d.hScroll > d.maxLineWidth-d.width {
		d.hScroll = max(d.maxLineWidth-d.width, 0)
	}
}

func (d *DetailModel) copyRecord() {
	cur := d.Current()
	blob, err := json.MarshalIndent(toExportable(cur), "", "  ")
	if err != nil {
		d.action.Warn = "copy: " + err.Error()
		return
	}
	d.copy(string(blob), "record")
}

func (d *DetailModel) copy(payload, label string) {
	if d.clipboard == nil {
		d.action.Warn = "copy " + label + ": clipboard unavailable"
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := d.clipboard.Copy(ctx, payload); err != nil {
		d.action.Warn = "copy " + label + ": " + err.Error()
		return
	}
	d.action.Toast = "copied " + label + " (" + strconv.Itoa(len(payload)) + " bytes)"
}

func (d *DetailModel) saveValue() {
	cur := d.Current()
	name := defaultSaveName(cur, "value", "")
	path := filepath.Join(d.outputDir, name)
	if err := d.writer.Write(path, cur.Value); err != nil {
		d.action.Warn = "save: " + err.Error()
		return
	}
	d.action.Toast = "saved " + path
}

func (d *DetailModel) saveFullJSON() {
	cur := d.Current()
	blob, err := json.MarshalIndent(toExportable(cur), "", "  ")
	if err != nil {
		d.action.Warn = "save: " + err.Error()
		return
	}
	name := defaultSaveName(cur, "record", ".json")
	path := filepath.Join(d.outputDir, name)
	if err := d.writer.Write(path, blob); err != nil {
		d.action.Warn = "save: " + err.Error()
		return
	}
	d.action.Toast = "saved " + path
}

func (d *DetailModel) openEditor() {
	cur := d.Current()
	body := FormatValue(d.view, cur.Value)
	tmp, err := os.CreateTemp("", "kafka-tui-msg-*.txt")
	if err != nil {
		d.action.Warn = "editor: " + err.Error()
		return
	}
	if _, err := tmp.WriteString(body); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		d.action.Warn = "editor: " + err.Error()
		return
	}
	if err := tmp.Close(); err != nil {
		d.action.Warn = "editor: " + err.Error()
		return
	}
	_ = os.Chmod(tmp.Name(), 0o400)
	defer os.Remove(tmp.Name())
	if err := d.pager.Open(tmp.Name()); err != nil {
		d.action.Warn = "editor: " + err.Error()
		return
	}
}

func (d *DetailModel) resend() {
	if d.readOnly {
		d.action.Warn = "cluster is read-only — resend blocked"
		return
	}
	cur := d.Current()
	dup := cur
	d.action.Produce = cur.Topic
	d.action.PrefillFromMessage = &dup
}

func (d *DetailModel) View() string {
	if len(d.messages) == 0 {
		d.layout()
		return d.styles.StatusInfo.Render("(no message)")
	}
	lines := d.layout()
	start := d.vScroll
	end := len(lines)
	if d.height > 0 && start+d.height < end {
		end = start + d.height
	}

	visible := lines[start:end]
	if !d.wrap && d.width > 0 {
		out := make([]string, len(visible))
		for i, line := range visible {
			s := line
			if d.hScroll > 0 {
				s = ansi.TruncateLeft(s, d.hScroll, "")
			}
			s = ansi.Truncate(s, d.width, "")
			out[i] = s
		}
		visible = out
	}
	return strings.Join(visible, "\n")
}

// ANSI styling is baked in here so downstream wrap/truncate must be ANSI-aware.
func (d *DetailModel) renderFullBody() string {
	cur := d.Current()
	header := d.renderHeader(cur)
	keyBlock := d.renderBlock("Key ("+strconv.Itoa(len(cur.Key))+" bytes)", string(cur.Key))
	headersBlock := d.renderBlock(fmt.Sprintf("Headers (%d)", len(cur.Headers)), headersText(cur.Headers))
	view := AutoView(d.view, cur.Value)
	valueLabel := fmt.Sprintf("Value · %s · %d bytes", view, len(cur.Value))
	valueBlock := d.renderBlock(valueLabel, FormatValue(d.view, cur.Value))
	return strings.Join([]string{header, "", keyBlock, "", headersBlock, "", valueBlock}, "\n")
}

func (d *DetailModel) layoutLines(body string) []string {
	logical := strings.Split(body, "\n")
	if !d.wrap || d.width <= 0 {
		return logical
	}
	out := make([]string, 0, len(logical))
	for _, line := range logical {
		if line == "" {
			out = append(out, "")
			continue
		}
		wrapped := ansi.Hardwrap(line, d.width, false)
		out = append(out, strings.Split(wrapped, "\n")...)
	}
	return out
}

func (d *DetailModel) renderHeader(cur kafka.Message) string {
	pos := fmt.Sprintf("%d/%d", d.index+1, len(d.messages))
	body := fmt.Sprintf(
		"%s · partition %d · offset %d · %s",
		cur.Topic,
		cur.Partition,
		cur.Offset,
		cur.Timestamp.Format(time.RFC3339),
	)
	return d.styles.HelpTitle.Render(body) + "  " + d.styles.StatusInfo.Render(pos)
}

func (d *DetailModel) renderBlock(title, body string) string {
	header := d.styles.HelpTitle.Render(title)
	if body == "" {
		return header + "\n" + d.styles.StatusInfo.Render("(empty)")
	}
	return header + "\n" + d.styles.Command.Render(body)
}

func headersText(headers []kafka.Header) string {
	if len(headers) == 0 {
		return ""
	}
	lines := make([]string, 0, len(headers))
	for _, h := range headers {
		lines = append(lines, h.Key+"="+strconv.Quote(string(h.Value)))
	}
	return strings.Join(lines, "\n")
}

// exportableMessage is JSON-friendly. Binary key/value bytes are base64;
// UTF-8 text is preserved as-is.
type exportableMessage struct {
	Topic     string             `json:"topic"`
	Partition int32              `json:"partition"`
	Offset    int64              `json:"offset"`
	Timestamp time.Time          `json:"timestamp"`
	Key       *exportableBytes   `json:"key,omitempty"`
	Value     *exportableBytes   `json:"value,omitempty"`
	Headers   []exportableHeader `json:"headers,omitempty"`
}

type exportableBytes struct {
	Encoding string `json:"encoding"` // "utf8" / "json" / "base64"
	Text     string `json:"text,omitempty"`
	Base64   string `json:"base64,omitempty"`
}

type exportableHeader struct {
	Key   string           `json:"key"`
	Value *exportableBytes `json:"value,omitempty"`
}

func toExportable(msg kafka.Message) exportableMessage {
	out := exportableMessage{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Timestamp: msg.Timestamp,
	}
	if len(msg.Key) > 0 {
		out.Key = encodeBytes(msg.Key)
	}
	if len(msg.Value) > 0 {
		out.Value = encodeBytes(msg.Value)
	}
	if len(msg.Headers) > 0 {
		out.Headers = make([]exportableHeader, 0, len(msg.Headers))
		for _, h := range msg.Headers {
			eh := exportableHeader{Key: h.Key}
			if len(h.Value) > 0 {
				eh.Value = encodeBytes(h.Value)
			}
			out.Headers = append(out.Headers, eh)
		}
	}
	return out
}

func encodeBytes(b []byte) *exportableBytes {
	switch kafka.DetectValueFormat(b) {
	case kafka.ValueFormatJSON:
		return &exportableBytes{Encoding: "json", Text: string(b)}
	case kafka.ValueFormatUTF8:
		return &exportableBytes{Encoding: "utf8", Text: string(b)}
	default:
		return &exportableBytes{Encoding: "base64", Base64: base64.StdEncoding.EncodeToString(b)}
	}
}

// defaultSaveName: <topic>-p<partition>-o<offset>-<kind><ext>.
func defaultSaveName(msg kafka.Message, kind, ext string) string {
	if ext == "" {
		ext = saveExt(msg.Value, kind)
	}
	clean := strings.ReplaceAll(msg.Topic, string(filepath.Separator), "_")
	if clean == "" {
		clean = "message"
	}
	return fmt.Sprintf("%s-p%d-o%d-%s%s", clean, msg.Partition, msg.Offset, kind, ext)
}

func saveExt(payload []byte, kind string) string {
	if kind == "record" {
		return ".json"
	}
	switch kafka.DetectValueFormat(payload) {
	case kafka.ValueFormatJSON:
		return ".json"
	case kafka.ValueFormatUTF8:
		return ".txt"
	default:
		return ".bin"
	}
}

func defaultWriteFile(path string, data []byte) error {
	if path == "" {
		return errors.New("save: empty path")
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("save: write %s: %w", path, err)
	}
	return nil
}

// DefaultPagerOpener runs `$EDITOR <path>`, falling back to `vi`.
func DefaultPagerOpener() PagerOpener {
	return PagerOpenerFunc(func(path string) error {
		editor := strings.TrimSpace(os.Getenv("EDITOR"))
		if editor == "" {
			editor = "vi"
		}
		parts := strings.Fields(editor)
		args := make([]string, 0, len(parts))
		args = append(args, parts[1:]...)
		args = append(args, path)
		cmd := exec.CommandContext(context.Background(), parts[0], args...) //nolint:gosec // user-controlled $EDITOR
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd.Run()
	})
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
