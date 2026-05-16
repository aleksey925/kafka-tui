package messages

import (
	"context"
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
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/help"
	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/recordfmt"
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

// PagerOpener opens a temp file in $EDITOR. Open returns a [tea.Cmd] (not the
// result directly) so the real implementation can route through
// [tea.ExecProcess] — the only safe way to spawn a full-screen child process
// from inside bubbletea. A blocking exec.Cmd.Run() corrupts the terminal
// because the parent's raw mode / alt-screen / mouse tracking are not released,
// and the child fights bubbletea for stdin.
//
// The returned Cmd must eventually post an [EditorOpenedMsg].
type PagerOpener interface {
	Open(path string) tea.Cmd
}

type PagerOpenerFunc func(path string) tea.Cmd

func (f PagerOpenerFunc) Open(path string) tea.Cmd { return f(path) }

// EditorOpenedMsg is the result of an external editor invocation. Err is the
// exec failure (if any); the consumer is responsible for removing the temp
// file at Path.
type EditorOpenedMsg struct {
	Path string
	Err  error
}

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
	viewport      *components.Viewport

	// copyMenu is non-nil only while the `c` popup is open; while open,
	// it owns the entire input stream of the detail screen (see
	// handleCopyMenu).
	copyMenu *components.Menu
}

// indices of the items built in openCopyMenu — used by
// dispatchCopySelection to route the selected payload.
const (
	copyItemRecord = iota
	copyItemKey
	copyItemValue
	copyItemHeaders
)

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
	vp := components.NewViewport()
	vp.SetWrap(opts.Wrap)
	vp.ClearCursor() // detail is a pure viewer — no row cursor.
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
		viewport:  vp,
	}
}

func (d *DetailModel) SetSize(w, h int) {
	d.width, d.height = w, h
	d.viewport.SetSize(w, h)
	d.refresh()
}

func (d *DetailModel) Wrap() bool { return d.viewport.Wrap() }

func (d *DetailModel) ScrollOffset() int { return d.viewport.ScrollOffset() }

func (d *DetailModel) HScrollOffset() int { return d.viewport.HScrollOffset() }

// ScrollSummary returns 1-based first/last visible line plus total. ok=false
// when geometry is unknown.
func (d *DetailModel) ScrollSummary() (first, last, total int, ok bool) {
	total = d.viewport.TotalLines()
	if total == 0 || d.height <= 0 {
		return 0, 0, 0, false
	}
	last = min(d.viewport.ScrollOffset()+d.height, total)
	return d.viewport.ScrollOffset() + 1, last, total, true
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
	// while the copy popup owns input, advertise its bindings instead
	// of the screen's normal set — same pattern as messages.go seek
	// popup. Keeps the hints bar and help screen accurate.
	if d.copyMenu != nil {
		return d.copyMenu.Bindings("Copy")
	}
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
		{Keys: []string{"end"}, Label: "scroll to bottom", Category: "Movement", Handler: d.actScrollBottom},
		{Keys: []string{"home"}, Label: "scroll to top", Category: "Movement", Handler: d.actScrollTop},
		{Keys: []string{"h", "left"}, Label: "scroll left (no-wrap)", Category: "Movement", Handler: d.actHScrollLeft},
		{Keys: []string{"l", "right"}, Label: "scroll right (no-wrap)", Category: "Movement", Handler: d.actHScrollRight},

		{Keys: []string{"c"}, Label: "copy record", Category: "Export", Hint: true, Handler: d.actCopy},
		{Keys: []string{"s"}, Label: "save record to file", Category: "Export", Hint: true, Handler: d.actSaveRecord},
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
func (d *DetailModel) actCopy() tea.Cmd       { d.openCopyMenu(); return nil }
func (d *DetailModel) actSaveRecord() tea.Cmd { d.saveRecord(); return nil }
func (d *DetailModel) actEditor() tea.Cmd     { return d.openEditor() }
func (d *DetailModel) actResend() tea.Cmd     { d.resend(); return nil }

func (d *DetailModel) actScrollDown() tea.Cmd   { d.viewport.ScrollBy(+1); return nil }
func (d *DetailModel) actScrollUp() tea.Cmd     { d.viewport.ScrollBy(-1); return nil }
func (d *DetailModel) actPageDown() tea.Cmd     { d.viewport.PageDown(); return nil }
func (d *DetailModel) actPageUp() tea.Cmd       { d.viewport.PageUp(); return nil }
func (d *DetailModel) actScrollBottom() tea.Cmd { d.viewport.ScrollToBottom(); return nil }
func (d *DetailModel) actScrollTop() tea.Cmd    { d.viewport.ScrollToTop(); return nil }

func (d *DetailModel) actHScrollLeft() tea.Cmd {
	d.viewport.HScrollBy(-d.viewport.HStep())
	return nil
}

func (d *DetailModel) actHScrollRight() tea.Cmd {
	d.viewport.HScrollBy(+d.viewport.HStep())
	return nil
}

func (d *DetailModel) Update(msg tea.Msg) (*DetailModel, tea.Cmd) {
	switch msg := msg.(type) {
	case EditorOpenedMsg:
		d.handleEditorOpened(msg)
		return d, nil
	case tea.KeyPressMsg:
		// the copy popup owns the input stream while open; no
		// detail-screen bindings (n/p/1/2/3/scroll/etc.) are evaluated,
		// so digits route to menu items without colliding with view-mode
		// shortcuts. Globals are handled at the host level before this
		// point, so :/?/ctrl+r/ctrl+c still fire normally.
		if d.copyMenu != nil {
			d.handleCopyMenu(msg)
			return d, nil
		}
		keymap.Dispatch(d.bindings(), msg)
		return d, nil
	}
	return d, nil
}

func (d *DetailModel) move(delta int) {
	if len(d.messages) == 0 {
		return
	}
	d.index = clampInt(d.index+delta, 0, len(d.messages)-1)
	d.viewport.Reset()
	d.refresh()
}

func (d *DetailModel) setView(v ValueView) {
	if d.view == v {
		return
	}
	d.view = v
	d.viewport.Reset()
	d.refresh()
}

func (d *DetailModel) toggleWrap() {
	d.viewport.SetWrap(!d.viewport.Wrap())
	d.refresh()
}

// refresh rebuilds the visual line list and pushes it into the viewport.
// Wrap mode is consulted on the viewport so the toggle stays in one place.
// Idempotent — safe to call on every render.
func (d *DetailModel) refresh() {
	if len(d.messages) == 0 {
		d.viewport.SetLines(nil)
		return
	}
	logical := strings.Split(d.renderFullBody(), "\n")
	if d.viewport.Wrap() && d.width > 0 {
		logical = components.WrapLines(logical, d.width)
	}
	d.viewport.SetLines(logical)
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

// CopyMenuOpen reports whether the copy popup is currently displayed.
// Exposed for tests.
func (d *DetailModel) CopyMenuOpen() bool { return d.copyMenu != nil }

func (d *DetailModel) openCopyMenu() {
	if len(d.messages) == 0 {
		return
	}
	items := []components.MenuItem{
		{Label: "Record (with metadata)"},
		{Label: "Key"},
		{Label: "Value"},
		{Label: "Headers"},
	}
	d.copyMenu = components.NewMenu(items,
		components.WithMenuStyles(d.styles),
		components.WithMenuTitle("Copy"),
	)
}

func (d *DetailModel) handleCopyMenu(key tea.KeyPressMsg) {
	d.copyMenu, _ = d.copyMenu.Update(key)
	if d.copyMenu.Canceled() {
		d.copyMenu = nil
		return
	}
	if idx, _, ok := d.copyMenu.Selected(); ok {
		d.dispatchCopySelection(idx)
		d.copyMenu = nil
	}
}

func (d *DetailModel) dispatchCopySelection(idx int) {
	cur := d.Current()
	switch idx {
	case copyItemRecord:
		meta := recordfmt.Metadata{
			Topic:     cur.Topic,
			Partition: cur.Partition,
			Offset:    cur.Offset,
			Timestamp: cur.Timestamp,
		}
		blob := recordfmt.EncodeWithMetadata(string(cur.Key), cur.Headers, cur.Value, meta)
		d.copy(string(blob), "record")
	case copyItemKey:
		d.copy(string(cur.Key), "key")
	case copyItemValue:
		d.copy(string(cur.Value), "value")
	case copyItemHeaders:
		d.copy(copyHeadersPayload(cur.Headers), "headers")
	default:
		// new menu items must be added to both openCopyMenu and this
		// switch — surface the gap as a warning instead of silently
		// dropping the user's keystroke.
		d.action.Warn = fmt.Sprintf("copy: unknown menu index %d", idx)
	}
}

// copyHeadersPayload renders the headers section as `name=value\n` per
// line — same shape as the `# Headers` section in the recordfmt format,
// so a paste into the produce form / editor lands cleanly. Distinct
// from the on-screen [headersText] which quotes values for display.
func copyHeadersPayload(headers []kafka.Header) string {
	var b strings.Builder
	for _, h := range headers {
		b.WriteString(h.Key)
		b.WriteByte('=')
		b.Write(h.Value)
		b.WriteByte('\n')
	}
	return b.String()
}

func (d *DetailModel) saveRecord() {
	cur := d.Current()
	meta := recordfmt.Metadata{
		Topic:     cur.Topic,
		Partition: cur.Partition,
		Offset:    cur.Offset,
		Timestamp: cur.Timestamp,
	}
	blob := recordfmt.EncodeWithMetadata(string(cur.Key), cur.Headers, cur.Value, meta)
	// extension is always .txt: the section frame is plain text;
	// value bytes (possibly binary) flow verbatim into it.
	name := defaultSaveName(cur, "record", ".txt")
	path := filepath.Join(d.outputDir, name)
	if err := d.writer.Write(path, blob); err != nil {
		d.action.Warn = "save: " + err.Error()
		return
	}
	d.action.Toast = "saved " + path
}

// openEditor writes the current value into a tmpfile and returns a tea.Cmd
// that hands the terminal off to $EDITOR via [PagerOpener.Open]. The tmpfile
// is removed when the resulting [EditorOpenedMsg] is dispatched.
func (d *DetailModel) openEditor() tea.Cmd {
	cur := d.Current()
	body := FormatValue(d.view, cur.Value)
	tmp, err := os.CreateTemp("", "kafka-tui-msg-*.txt")
	if err != nil {
		d.action.Warn = "editor: " + err.Error()
		return nil
	}
	if _, err := tmp.WriteString(body); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		d.action.Warn = "editor: " + err.Error()
		return nil
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		d.action.Warn = "editor: " + err.Error()
		return nil
	}
	_ = os.Chmod(tmp.Name(), 0o400)
	return d.pager.Open(tmp.Name())
}

// handleEditorOpened consumes the result of an external-editor session: the
// view is read-only so we only need to remove the tmpfile and surface any
// exec failure as a warning toast.
func (d *DetailModel) handleEditorOpened(msg EditorOpenedMsg) {
	if msg.Path != "" {
		_ = os.Remove(msg.Path)
	}
	if msg.Err != nil {
		d.action.Warn = "editor: " + msg.Err.Error()
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
		return d.styles.StatusInfo.Render("(no message)")
	}
	if d.copyMenu != nil {
		// modal: show the record header for orientation, then center
		// the popup over the body. Viewport content is hidden until
		// the popup closes.
		header := d.renderHeader(d.Current())
		body := layout.PlaceCenteredTop(d.width, d.bodyHeight(), d.copyMenu.View(0))
		return header + "\n" + body
	}
	d.refresh()
	return d.viewport.View()
}

// bodyHeight returns the height available below the record-header line
// for popups / viewport content. Falls back to 0 when the screen size
// hasn't been propagated yet.
func (d *DetailModel) bodyHeight() int {
	if d.height <= 1 {
		return 0
	}
	return d.height - 1
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

// DefaultPagerOpener runs `$EDITOR <path>` (falling back to `vi`) through
// [tea.ExecProcess] so bubbletea can release the terminal cleanly while the
// editor is running and restore it afterwards.
//
// I/O wiring (stdin/stdout/stderr) is intentionally NOT set here — bubbletea
// fills in the program's own streams when they are unset.
func DefaultPagerOpener() PagerOpener {
	return PagerOpenerFunc(func(path string) tea.Cmd {
		editor := strings.TrimSpace(os.Getenv("EDITOR"))
		if editor == "" {
			editor = "vi"
		}
		parts := strings.Fields(editor)
		args := append([]string(nil), parts[1:]...)
		args = append(args, path)
		execCmd := exec.CommandContext(context.Background(), parts[0], args...) //nolint:gosec // user-controlled $EDITOR
		return tea.ExecProcess(execCmd, func(runErr error) tea.Msg {
			return EditorOpenedMsg{Path: path, Err: runErr}
		})
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
