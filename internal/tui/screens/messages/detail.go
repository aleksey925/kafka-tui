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
	// Back signals the user pressed Esc/q.
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
	Now        func() time.Time
	Styles     theme.Styles
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
	yPrimed   bool
	action    DetailAction
	styles    theme.Styles
	now       func() time.Time
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
	}
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
		{Key: "y …", Label: "copy"},
		{Key: "s/S", Label: "save"},
		{Key: "e", Label: "$EDITOR"},
	}
	if !d.readOnly {
		hints = append(hints, layout.KeyHint{Key: "r", Label: "resend"})
	}
	hints = append(hints, layout.KeyHint{Key: "Esc", Label: "back"})
	return hints
}

// Update routes a key message into the detail view.
func (d *DetailModel) Update(msg tea.Msg) (*DetailModel, tea.Cmd) {
	key, ok := msg.(tea.KeyPressMsg)
	if !ok {
		return d, nil
	}
	if d.yPrimed {
		d.handleYankFollowup(key)
		d.yPrimed = false
		return d, nil
	}
	switch key.String() {
	case "esc", "q":
		d.action.Back = true
	case "n":
		d.move(+1)
	case "p":
		d.move(-1)
	case "1":
		d.view = ViewJSON
	case "2":
		d.view = ViewRaw
	case "3":
		d.view = ViewHex
	case "y":
		d.yPrimed = true
	case "s":
		d.saveValue()
	case "S":
		d.saveFullJSON()
	case "e":
		d.openEditor()
	case "r":
		d.resend()
	}
	return d, nil
}

func (d *DetailModel) move(delta int) {
	if len(d.messages) == 0 {
		return
	}
	d.index = clampInt(d.index+delta, 0, len(d.messages)-1)
}

// handleYankFollowup interprets the second key of `y <x>`.
func (d *DetailModel) handleYankFollowup(key tea.KeyPressMsg) {
	cur := d.Current()
	switch key.String() {
	case "k":
		d.copy(string(cur.Key), "key")
	case "v":
		d.copy(string(cur.Value), "value")
	case "h":
		d.copy(headersText(cur.Headers), "headers")
	case "a":
		blob, err := json.MarshalIndent(toExportable(cur), "", "  ")
		if err != nil {
			d.action.Warn = "copy: " + err.Error()
			return
		}
		d.copy(string(blob), "all")
	}
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
	name := defaultSaveName(cur, "message", ".json")
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
func (d *DetailModel) View(width, _ int) string {
	if len(d.messages) == 0 {
		return d.styles.StatusInfo.Render("(no message)")
	}
	cur := d.Current()
	header := d.renderHeader(cur, width)
	keyBlock := d.renderBlock("Key ("+strconv.Itoa(len(cur.Key))+" bytes)", string(cur.Key))
	headersBlock := d.renderBlock(fmt.Sprintf("Headers (%d)", len(cur.Headers)), headersText(cur.Headers))
	view := AutoView(d.view, cur.Value)
	valueLabel := fmt.Sprintf("Value · %s · %d bytes", view, len(cur.Value))
	valueBlock := d.renderBlock(valueLabel, FormatValue(d.view, cur.Value))
	return strings.Join([]string{header, "", keyBlock, "", headersBlock, "", valueBlock}, "\n")
}

func (d *DetailModel) renderHeader(cur kafka.Message, _ int) string {
	pos := fmt.Sprintf("%d/%d", d.index+1, len(d.messages))
	body := fmt.Sprintf(
		"%s · P%d · offset %d · %s",
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
	return fmt.Sprintf("%s-p%d-o%d%s", clean, msg.Partition, msg.Offset, ext)
}

func saveExt(payload []byte, kind string) string {
	if kind == "message" {
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
		editor := os.Getenv("EDITOR")
		if editor == "" {
			editor = "vi"
		}
		cmd := exec.CommandContext(context.Background(), editor, path) //nolint:gosec // user-controlled $EDITOR
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
