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
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// Clipboard abstracts the OSC 52 / native clipboard subsystem (Task 17).
// The detail view writes through this interface so it can be wired to the
// real implementation later without modifying screen logic.
type Clipboard interface {
	Copy(ctx context.Context, payload string) error
}

// FileWriter abstracts atomic file writes used by the `s` / `S` save
// hotkeys. Defaults to writing under the current working directory.
type FileWriter interface {
	Write(path string, data []byte) error
}

// FileWriterFunc adapts a function into a [FileWriter].
type FileWriterFunc func(path string, data []byte) error

// Write calls f.
func (f FileWriterFunc) Write(path string, data []byte) error { return f(path, data) }

// PagerOpener opens a temporary read-only file in `$EDITOR` (or the user's
// preferred pager). Defaults to [DefaultPagerOpener].
type PagerOpener interface {
	Open(path string) error
}

// PagerOpenerFunc adapts a function into a [PagerOpener].
type PagerOpenerFunc func(path string) error

// Open calls f.
func (f PagerOpenerFunc) Open(path string) error { return f(path) }

// DetailAction is the host-facing intent of the detail view.
type DetailAction struct {
	// Back signals the user pressed esc/q.
	Back bool
	// Produce, when non-empty, requests the produce form prefilled with the
	// resend payload (PrefillFromMessage).
	Produce            string
	PrefillFromMessage *kafka.Message
	// Toast and Warn surface ephemeral status messages from the detail view
	// (copy / save outcomes). The host renders them using its toast queue.
	Toast string
	Warn  string
}

// DetailOptions configure a [DetailModel].
type DetailOptions struct {
	Messages   []kafka.Message
	Index      int
	ReadOnly   bool
	Clipboard  Clipboard
	FileWriter FileWriter
	Pager      PagerOpener
	OutputDir  string // where save targets are written; defaults to cwd
	// Wrap selects the initial soft-wrap mode. The parent screen passes its
	// remembered value so the user's preference survives detail re-opens.
	Wrap   bool
	Now    func() time.Time
	Styles theme.Styles
}

// DetailModel is the message-detail screen (§7.4).
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
	// totalLines and maxLineWidth are cached layout geometry. They are
	// refreshed by [layout] whenever something that affects the
	// rendered body changes (size, view mode, wrap, current message), and
	// consumed by scroll math (clampScroll, scrollBottom, ScrollSummary,
	// hScroll bounds) so jumps work without first calling View().
	totalLines   int
	maxLineWidth int
}

// NewDetailModel constructs a detail view focused on opts.Messages[opts.Index].
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

// SetSize records the body geometry. Called by the parent screen on every
// WindowSizeMsg so the viewport can re-clamp.
func (d *DetailModel) SetSize(w, h int) {
	d.width, d.height = w, h
	d.layout()
}

// Wrap reports the current soft-wrap mode (true = on).
func (d *DetailModel) Wrap() bool { return d.wrap }

// ScrollOffset returns the current vertical line offset (for tests).
func (d *DetailModel) ScrollOffset() int { return d.vScroll }

// HScrollOffset returns the current horizontal column offset (for tests).
func (d *DetailModel) HScrollOffset() int { return d.hScroll }

// ScrollSummary describes the visible window: 1-based first/last visible
// line and the total line count. Returns ok=false when geometry is unknown.
func (d *DetailModel) ScrollSummary() (first, last, total int, ok bool) {
	if d.totalLines == 0 || d.height <= 0 {
		return 0, 0, 0, false
	}
	last = min(d.vScroll+d.height, d.totalLines)
	return d.vScroll + 1, last, d.totalLines, true
}

// Action returns the pending host action.
func (d *DetailModel) Action() DetailAction { return d.action }

// ConsumeAction returns and clears the pending action.
func (d *DetailModel) ConsumeAction() DetailAction {
	a := d.action
	d.action = DetailAction{}
	return a
}

// Index returns the current message index (for tests).
func (d *DetailModel) Index() int { return d.index }

// ViewMode returns the active rendering mode (for tests).
func (d *DetailModel) ViewMode() ValueView { return d.view }

// Current returns the focused message.
func (d *DetailModel) Current() kafka.Message {
	if len(d.messages) == 0 {
		return kafka.Message{}
	}
	return d.messages[d.index]
}

// KeyHints returns the screen-specific hints.
func (d *DetailModel) KeyHints() []layout.KeyHint {
	hints := []layout.KeyHint{
		{Key: "n/p", Label: "next/prev"},
		{Key: "1/2/3", Label: "json/raw/hex"},
		{Key: "j/k", Label: "scroll"},
		{Key: "w", Label: "wrap"},
		{Key: "y", Label: "copy"},
		{Key: "s/S", Label: "save"},
		{Key: "e", Label: "$EDITOR"},
	}
	if !d.readOnly {
		hints = append(hints, layout.KeyHint{Key: "R", Label: "resend"})
	}
	hints = append(hints, layout.KeyHint{Key: "esc", Label: "back"})
	return hints
}

// Update routes a key message into the detail view.
func (d *DetailModel) Update(msg tea.Msg) (*DetailModel, tea.Cmd) {
	key, ok := msg.(tea.KeyPressMsg)
	if !ok {
		return d, nil
	}
	str := key.String()
	if d.gPrimed {
		d.gPrimed = false
		if str == "g" {
			d.scrollTop()
			return d, nil
		}
		// any other key after `g` falls through to normal handling.
	}
	if d.handleScrollKey(str) {
		return d, nil
	}
	switch str {
	case "esc", "q":
		d.action.Back = true
	case "n":
		d.move(+1)
	case "p":
		d.move(-1)
	case "1":
		d.setView(ViewJSON)
	case "2":
		d.setView(ViewRaw)
	case "3":
		d.setView(ViewHex)
	case "y":
		d.copyRecord()
	case "s":
		d.saveValue()
	case "S":
		d.saveFullJSON()
	case "e":
		d.openEditor()
	case "R":
		d.resend()
	case "w":
		d.toggleWrap()
	}
	return d, nil
}

// handleScrollKey routes the viewport-navigation keys. Returns true when the
// key was consumed; non-scroll keys fall through to the main switch.
func (d *DetailModel) handleScrollKey(str string) bool {
	switch str {
	case "j", "down":
		d.scrollBy(+1)
	case "k", "up":
		d.scrollBy(-1)
	case "ctrl+f", "pgdown":
		d.scrollBy(+d.pageStep())
	case "ctrl+b", "pgup":
		d.scrollBy(-d.pageStep())
	case "g":
		d.gPrimed = true
	case "G", "end":
		d.scrollBottom()
	case "home":
		d.scrollTop()
	case "h", "left":
		if !d.wrap {
			d.hScrollBy(-d.hStep())
		}
	case "l", "right":
		if !d.wrap {
			d.hScrollBy(+d.hStep())
		}
	default:
		return false
	}
	return true
}

func (d *DetailModel) move(delta int) {
	if len(d.messages) == 0 {
		return
	}
	d.index = clampInt(d.index+delta, 0, len(d.messages)-1)
	d.resetScroll()
	d.layout()
}

// setView switches the value rendering mode and resets the viewport so the
// new content starts from the top.
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
	// horizontal offset is meaningless in wrap mode; vertical offset is kept
	// because clamp will normalise it after the next render.
	d.hScroll = 0
	d.layout()
}

// layout produces the visible-line slice and refreshes the cached geometry
// (totalLines, maxLineWidth) in lock-step. Called both by [View] and by any
// state-changing op (resize, message switch, view-mode change, wrap toggle)
// so a subsequent scroll key sees the same numbers the next frame will
// render.
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
	// scroll roughly a quarter screen — enough to feel responsive without
	// jumping past short overhangs.
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
	// best-effort read-only mode.
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

// View renders the detail body.
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
		// horizontal slicing applied per visible line.
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

// renderFullBody assembles every block (header / key / headers / value) into
// a single string. ANSI styling is already baked in here so downstream
// wrap/truncate must be ANSI-aware.
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

// layoutLines splits the rendered body into visual lines. With wrap enabled
// each logical line longer than width is hard-wrapped; with wrap disabled
// lines are returned as-is and truncation happens at render time.
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

// headersText renders headers as "k=value\n…", with binary-safe values.
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

// exportableMessage is a JSON-friendly view of [kafka.Message] used by save
// / copy-all hotkeys. Binary key/value bytes are base64-encoded; UTF-8 text
// is preserved as-is for human readability.
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
	Encoding string `json:"encoding"` // "utf8", "json", "base64"
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

// defaultSaveName builds a deterministic filename for save targets:
//
//	<topic>-p<partition>-o<offset><ext>
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

// DefaultPagerOpener returns a [PagerOpener] that runs `$EDITOR <path>`,
// falling back to `vi` then `less`. It is unsuitable for unit tests; tests
// inject a [PagerOpenerFunc].
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
