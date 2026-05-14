package topics

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/kafka/configcatalog"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/help"
	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/theme"
)

// ConfigsAction is the host-facing intent of the configs screen.
type ConfigsAction struct {
	Back bool
	// Edit is the config key the user wants to edit; empty when no edit
	// is pending.
	Edit string
}

// ConfigsOptions configures a [ConfigsModel].
type ConfigsOptions struct {
	Service  Service
	Topic    string
	ReadOnly bool
	// FocusKey is the config key the cursor should land on after the
	// first load completes — used by the host to restore the previous
	// position after the user returns from the edit screen.
	FocusKey string
	Now      func() time.Time
	Styles   theme.Styles
}

// ConfigsModel is the topic configs viewer screen.
type ConfigsModel struct {
	svc      Service
	topic    string
	readOnly bool

	rows    []configRow
	visible []int // indexes into rows after applying search
	cursor  int   // index into visible

	scrollTop int // first visible line in the viewport

	// focusKey is consumed once on the first successful load to restore
	// the cursor position after returning from the edit screen.
	focusKey string

	search   string
	helpOpen bool

	toasts        *components.Toasts
	loading       bool
	loadErr       string
	width, height int
	manualRefresh bool

	// lifeCtx is canceled by Close(); used by [loadConfigsCmd] so the
	// in-flight DescribeAllTopicConfigs RPC is aborted and any late-arriving
	// [ConfigsLoadedMsg] is dropped instead of being delivered to whichever
	// screen happens to be active when it returns.
	lifeCtx    context.Context //nolint:containedctx // tied to screen lifecycle
	lifeCancel context.CancelFunc

	action ConfigsAction
	now    func() time.Time
	styles theme.Styles
}

// configRow pairs the broker-reported config with its bundled metadata.
// `entry` is the zero value (and `knownDoc` is false) for keys not present
// in the bundled catalog.
type configRow struct {
	cfg      kafka.TopicConfig
	entry    configcatalog.Entry
	knownDoc bool
	category string
}

func NewConfigsModel(opts ConfigsOptions) *ConfigsModel {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	styles := opts.Styles
	if styles.Palette.Foreground == nil {
		styles = theme.DefaultStyles()
	}
	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	return &ConfigsModel{
		svc:        opts.Service,
		topic:      opts.Topic,
		readOnly:   opts.ReadOnly,
		focusKey:   opts.FocusKey,
		toasts:     components.NewToasts(components.WithToastClock(now), components.WithToastStyles(styles)),
		now:        now,
		styles:     styles,
		lifeCtx:    lifeCtx,
		lifeCancel: lifeCancel,
	}
}

func (m *ConfigsModel) Init() tea.Cmd {
	m.loading = true
	return loadConfigsCmd(m.lifeCtx, m.svc, m.topic)
}

// Close cancels any in-flight load goroutine so a slow
// DescribeAllTopicConfigs RPC cannot deliver its result to a different
// screen (or a freshly constructed ConfigsModel for another topic) after
// the host has popped this instance.
func (m *ConfigsModel) Close() {
	if m.lifeCancel != nil {
		m.lifeCancel()
	}
}

func (m *ConfigsModel) Topic() string { return m.topic }

func (m *ConfigsModel) Action() ConfigsAction { return m.action }

func (m *ConfigsModel) ConsumeAction() ConfigsAction {
	a := m.action
	m.action = ConfigsAction{}
	return a
}

func (m *ConfigsModel) Toasts() *components.Toasts { return m.toasts }

func (m *ConfigsModel) LatestFlash() (components.Toast, bool) {
	if m.toasts == nil {
		return components.Toast{}, false
	}
	return m.toasts.Latest()
}

func (m *ConfigsModel) Title() string {
	return "Topic Configs · " + m.topic
}

func (m *ConfigsModel) Breadcrumb() string {
	if m.helpOpen {
		return "help"
	}
	return "configs"
}

// Configs returns a defensive copy of the loaded configs.
func (m *ConfigsModel) Configs() []kafka.TopicConfig {
	out := make([]kafka.TopicConfig, 0, len(m.rows))
	for _, r := range m.rows {
		out = append(out, r.cfg)
	}
	return out
}

// HelpOpen reports whether the documentation overlay is visible.
func (m *ConfigsModel) HelpOpen() bool { return m.helpOpen }

// HasOverlay routes esc to the overlay-close path inside the screen
// instead of the host's filter-clear / pop cascade — without it a
// pending filter would consume the first esc.
func (m *ConfigsModel) HasOverlay() bool { return m.helpOpen }

// SelectedKey returns the key under the cursor, or "" when nothing is
// selected (e.g. an empty filter result).
func (m *ConfigsModel) SelectedKey() string {
	r, ok := m.selectedRow()
	if !ok {
		return ""
	}
	return r.cfg.Key
}

func (m *ConfigsModel) selectedRow() (configRow, bool) {
	if len(m.visible) == 0 {
		return configRow{}, false
	}
	if m.cursor < 0 || m.cursor >= len(m.visible) {
		return configRow{}, false
	}
	return m.rows[m.visible[m.cursor]], true
}

// SetSearch narrows the visible rows. Empty string clears the filter.
func (m *ConfigsModel) SetSearch(query string) {
	m.search = query
	m.rebuildVisible()
}

func (m *ConfigsModel) ActiveFilter() string { return m.search }

func (m *ConfigsModel) SetSize(w, h int) {
	m.width, m.height = w, h
	m.clampViewport()
}

func (m *ConfigsModel) KeyHints() []layout.KeyHint {
	return layout.HintsFromBindings(m.bindings())
}

func (m *ConfigsModel) HelpSections() []help.Section {
	return help.SectionsFromBindings(m.bindings())
}

func (m *ConfigsModel) bindings() []keymap.Binding {
	bs := []keymap.Binding{
		{Keys: []string{"up", "k"}, Label: "previous", Category: "Topic", Handler: m.actUp},
		{Keys: []string{"down", "j"}, Label: "next", Category: "Topic", Handler: m.actDown},
		{Keys: []string{"ctrl+b", "pgup"}, Label: "page up", Category: "Topic", Handler: m.actPageUp},
		{Keys: []string{"ctrl+f", "pgdown"}, Label: "page down", Category: "Topic", Handler: m.actPageDown},
		{Keys: []string{"enter", "e"}, Label: "edit value", Category: "Topic", Hint: true, Handler: m.actEdit},
		{Keys: []string{"h"}, Label: "show docs", Category: "Topic", Hint: true, Handler: m.actToggleHelp},
		{Keys: []string{"r"}, Label: "refresh now", Category: "Topic", Hint: true, Handler: m.actRefresh},
		{Keys: []string{"esc", "q"}, Label: "back / close docs", Category: "Topic", Handler: m.actBackOrClose},
		{Keys: []string{"/"}, Label: "filter rows", Category: "Topic", Hint: true},
	}
	return bs
}

func (m *ConfigsModel) actBackOrClose() tea.Cmd {
	if m.helpOpen {
		m.helpOpen = false
		return nil
	}
	m.action.Back = true
	return nil
}

func (m *ConfigsModel) actToggleHelp() tea.Cmd {
	if _, ok := m.selectedRow(); !ok {
		return nil
	}
	m.helpOpen = !m.helpOpen
	return nil
}

func (m *ConfigsModel) actRefresh() tea.Cmd {
	m.loading = true
	m.manualRefresh = true
	return loadConfigsCmd(m.lifeCtx, m.svc, m.topic)
}

func (m *ConfigsModel) actUp() tea.Cmd {
	if m.cursor > 0 {
		m.cursor--
	}
	m.clampViewport()
	return nil
}

func (m *ConfigsModel) actDown() tea.Cmd {
	if m.cursor+1 < len(m.visible) {
		m.cursor++
	}
	m.clampViewport()
	return nil
}

func (m *ConfigsModel) actPageUp() tea.Cmd {
	m.cursor -= m.pageStep()
	if m.cursor < 0 {
		m.cursor = 0
	}
	m.clampViewport()
	return nil
}

func (m *ConfigsModel) actPageDown() tea.Cmd {
	m.cursor += m.pageStep()
	if m.cursor >= len(m.visible) {
		m.cursor = len(m.visible) - 1
	}
	if m.cursor < 0 {
		m.cursor = 0
	}
	m.clampViewport()
	return nil
}

func (m *ConfigsModel) pageStep() int {
	step := m.listHeight() - 1
	if step < 1 {
		return 1
	}
	return step
}

func (m *ConfigsModel) actEdit() tea.Cmd {
	r, ok := m.selectedRow()
	if !ok {
		return nil
	}
	if m.readOnly {
		m.toasts.Push(components.ToastWarning, "cluster is read-only — edit blocked")
		return nil
	}
	m.action.Edit = r.cfg.Key
	return nil
}

func (m *ConfigsModel) Update(msg tea.Msg) tea.Cmd {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.SetSize(msg.Width, msg.Height)
		return nil
	case ConfigsLoadedMsg:
		m.handleLoaded(msg)
		return nil
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	}
	return nil
}

func (m *ConfigsModel) handleKey(key tea.KeyPressMsg) tea.Cmd {
	if m.toasts != nil {
		_, _ = m.toasts.Update(key)
	}
	if cmd, ok := keymap.Dispatch(m.bindings(), key); ok {
		return cmd
	}
	return nil
}

func (m *ConfigsModel) handleLoaded(msg ConfigsLoadedMsg) {
	m.loading = false
	if msg.Err != nil {
		m.loadErr = msg.Err.Error()
		m.toasts.Push(components.ToastError, "load configs: "+msg.Err.Error())
		m.manualRefresh = false
		return
	}
	m.loadErr = ""
	// snapshot the focused key BEFORE swapping m.rows; otherwise
	// rebuildVisible would read m.visible[m.cursor] against the new
	// rows slice and may resolve to the wrong key when the broker
	// returned a different set / order between refreshes.
	prevKey := m.SelectedKey()
	m.rows = buildRows(msg.Configs)
	m.rebuildVisibleAt(prevKey)
	if m.focusKey != "" {
		m.jumpToKey(m.focusKey)
		m.focusKey = ""
	}
	if m.manualRefresh {
		m.toasts.Push(components.ToastSuccess, fmt.Sprintf(
			"refreshed · %d configs", len(msg.Configs),
		))
		m.manualRefresh = false
	}
}

// buildRows enriches each broker config with bundled metadata and sorts
// by (category, key) so categories render in stable order.
func buildRows(cfgs []kafka.TopicConfig) []configRow {
	out := make([]configRow, 0, len(cfgs))
	for _, c := range cfgs {
		entry, ok := configcatalog.Lookup(c.Key)
		category := configcatalog.CategoryFallback
		if ok {
			category = entry.Category
		}
		out = append(out, configRow{
			cfg:      c,
			entry:    entry,
			knownDoc: ok,
			category: category,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].category != out[j].category {
			return out[i].category < out[j].category
		}
		return out[i].cfg.Key < out[j].cfg.Key
	})
	return out
}

// rebuildVisible recomputes the filtered slice and tries to keep the
// cursor on the row that was selected before. It reads the prevKey from
// the current state — safe for in-place filter / search changes.
func (m *ConfigsModel) rebuildVisible() {
	m.rebuildVisibleAt(m.SelectedKey())
}

// rebuildVisibleAt is the post-load variant: callers pass the key they
// captured BEFORE replacing m.rows so we don't dereference stale indexes
// against the new slice.
func (m *ConfigsModel) rebuildVisibleAt(prevKey string) {
	q := strings.ToLower(strings.TrimSpace(m.search))
	m.visible = m.visible[:0]
	for i, r := range m.rows {
		if q != "" && !rowMatches(r, q) {
			continue
		}
		m.visible = append(m.visible, i)
	}
	m.cursor = 0
	if prevKey != "" {
		for i, idx := range m.visible {
			if m.rows[idx].cfg.Key == prevKey {
				m.cursor = i
				break
			}
		}
	}
	if m.cursor >= len(m.visible) {
		m.cursor = len(m.visible) - 1
	}
	if m.cursor < 0 {
		m.cursor = 0
	}
	m.clampViewport()
}

// jumpToKey moves the cursor onto the row identified by key when it is
// present in the current visible set. No-op otherwise — callers must
// tolerate the row being missing (filtered out, deleted upstream).
func (m *ConfigsModel) jumpToKey(key string) {
	for i, idx := range m.visible {
		if m.rows[idx].cfg.Key == key {
			m.cursor = i
			m.clampViewport()
			return
		}
	}
}

func rowMatches(r configRow, needle string) bool {
	return strings.Contains(strings.ToLower(r.cfg.Key), needle) ||
		strings.Contains(strings.ToLower(r.cfg.Value), needle) ||
		strings.Contains(strings.ToLower(r.cfg.Source), needle) ||
		strings.Contains(strings.ToLower(r.category), needle)
}

// listChromeRows is the static height contributed by the title and the
// optional bottom status line (refreshing / error). One title line plus
// one slot reserved for status keeps the viewport math stable across
// refresh ticks.
const listChromeRows = 2

func (m *ConfigsModel) View() string {
	parts := []string{m.styles.HelpTitle.Render(m.formatTitle("Topic configs"))}

	if m.loading && len(m.rows) == 0 {
		parts = append(parts, m.styles.StatusInfo.Render("(loading…)"))
		return strings.Join(parts, "\n")
	}

	body := m.renderList()
	if m.helpOpen {
		// help popup is anchored over the list area so it doesn't
		// disappear off-screen when the cursor is far down the list.
		body = m.placePopupOver(body, m.renderHelp())
	}
	parts = append(parts, body)

	if m.loading {
		parts = append(parts, m.styles.StatusInfo.Render("(refreshing…)"))
	}
	if m.loadErr != "" {
		parts = append(parts, m.styles.StatusErr.Render("error: "+m.loadErr))
	}
	return strings.Join(parts, "\n")
}

func (m *ConfigsModel) renderList() string {
	if len(m.visible) == 0 {
		return m.styles.StatusInfo.Render("(no rows)")
	}

	allLines, _ := m.buildListLines(m.keyColumnWidth())
	avail := m.listHeight()
	if avail <= 0 || len(allLines) <= avail {
		return strings.Join(allLines, "\n")
	}
	// scrollTop is maintained by [clampViewport]; here we only slice.
	end := min(m.scrollTop+avail, len(allLines))
	return strings.Join(allLines[m.scrollTop:end], "\n")
}

// buildListLines flattens the categorized rows into a slice of rendered
// lines (category headers + spacer lines + rows). The second return is
// the line index of the currently-selected row; -1 when nothing is
// selected.
func (m *ConfigsModel) buildListLines(keyW int) ([]string, int) {
	lines := make([]string, 0, len(m.visible)*2)
	cursorLine := -1
	var lastCat string
	for vi, idx := range m.visible {
		r := m.rows[idx]
		if r.category != lastCat {
			if lastCat != "" {
				lines = append(lines, "")
			}
			lines = append(lines, m.styles.Header.Render("─ "+r.category+" ─"))
			lastCat = r.category
		}
		if vi == m.cursor {
			cursorLine = len(lines)
		}
		lines = append(lines, m.renderRow(r, keyW, vi == m.cursor))
	}
	return lines, cursorLine
}

func (m *ConfigsModel) renderRow(r configRow, keyW int, selected bool) string {
	cursor := "  "
	if selected {
		cursor = m.styles.Cursor.Render("▸ ")
	}
	body := padRight(r.cfg.Key, keyW) + "  " + r.cfg.Value
	if selected {
		body = m.styles.CommandHL.Render(body)
	}
	return cursor + body
}

func (m *ConfigsModel) keyColumnWidth() int {
	const (
		minKey = 24
		maxKey = 44
	)
	w := minKey
	for _, idx := range m.visible {
		if l := lipgloss.Width(m.rows[idx].cfg.Key); l > w {
			w = l
		}
	}
	if w > maxKey {
		w = maxKey
	}
	return w
}

func (m *ConfigsModel) listHeight() int {
	if m.height <= listChromeRows {
		return 0
	}
	return m.height - listChromeRows
}

func (m *ConfigsModel) clampViewport() {
	avail := m.listHeight()
	if avail <= 0 {
		m.scrollTop = 0
		return
	}
	keyW := m.keyColumnWidth()
	allLines, cursorLine := m.buildListLines(keyW)
	if len(allLines) <= avail {
		m.scrollTop = 0
		return
	}
	if cursorLine >= 0 {
		if cursorLine < m.scrollTop {
			m.scrollTop = cursorLine
		} else if cursorLine >= m.scrollTop+avail {
			m.scrollTop = cursorLine - avail + 1
		}
	}
	if m.scrollTop+avail > len(allLines) {
		m.scrollTop = len(allLines) - avail
	}
	if m.scrollTop < 0 {
		m.scrollTop = 0
	}
}

// placePopupOver anchors popup at the top of body's area, centered
// horizontally, so the help box always sits in a predictable spot
// regardless of how much was scrolled. The body string is replaced —
// classic single-buffer terminals can't paint a real overlay and the
// list isn't useful while the popup is open.
func (m *ConfigsModel) placePopupOver(body, popup string) string {
	if popup == "" || m.width <= 0 {
		return body
	}
	avail := m.listHeight()
	if avail <= 0 {
		avail = lipgloss.Height(body)
	}
	centered := lipgloss.PlaceHorizontal(m.width, lipgloss.Center, popup)
	if avail <= 0 {
		return centered
	}
	return lipgloss.PlaceVertical(avail, lipgloss.Top, centered)
}

func (m *ConfigsModel) renderHelp() string {
	r, ok := m.selectedRow()
	if !ok {
		return ""
	}
	title := m.styles.HelpTitle.Render(r.cfg.Key)
	lines := []string{title}
	if r.knownDoc {
		typ := r.entry.Type.String()
		lines = append(lines, m.styles.HintLabel.Render("type: "+typ+" · category: "+r.category))
		if len(r.entry.EnumValues) > 0 {
			lines = append(lines, m.styles.HintLabel.Render("values: "+strings.Join(r.entry.EnumValues, ", ")))
		}
	} else {
		lines = append(lines, m.styles.HintLabel.Render("(no bundled documentation)"))
	}
	lines = append(lines, "", fmt.Sprintf("current: %s  source: %s", r.cfg.Value, r.cfg.Source))
	if r.knownDoc && r.entry.Doc != "" {
		lines = append(lines, "", wrap(r.entry.Doc, m.docWidth()))
	}
	box := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(m.styles.Palette.Subtle).
		Padding(0, 1)
	return box.Render(strings.Join(lines, "\n"))
}

func (m *ConfigsModel) docWidth() int {
	return max(min(m.width-4, 100), 40)
}

func padRight(s string, w int) string {
	delta := w - lipgloss.Width(s)
	if delta <= 0 {
		return s
	}
	return s + strings.Repeat(" ", delta)
}

// wrap soft-wraps s to width w on word boundaries; existing newlines are
// preserved.
func wrap(s string, w int) string {
	if w <= 0 {
		return s
	}
	var out strings.Builder
	for li, line := range strings.Split(s, "\n") {
		if li > 0 {
			out.WriteByte('\n')
		}
		col := 0
		for wi, word := range strings.Fields(line) {
			wl := lipgloss.Width(word)
			if wi == 0 {
				out.WriteString(word)
				col = wl
				continue
			}
			if col+1+wl > w {
				out.WriteByte('\n')
				out.WriteString(word)
				col = wl
			} else {
				out.WriteByte(' ')
				out.WriteString(word)
				col += 1 + wl
			}
		}
	}
	return out.String()
}

func (m *ConfigsModel) formatTitle(prefix string) string {
	if m.topic != "" {
		return prefix + " · " + m.topic
	}
	return prefix
}

type ConfigsLoadedMsg struct {
	Configs []kafka.TopicConfig
	Err     error
}

func loadConfigsCmd(lifeCtx context.Context, svc Service, topic string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(lifeCtx, 10*time.Second)
		defer cancel()
		cfgs, err := svc.DescribeAllTopicConfigs(ctx, topic)
		// the screen popped before the RPC returned; drop the result so a
		// stale ConfigsLoadedMsg can't land on a freshly opened ConfigsModel
		// for a different topic (or on a different screen type entirely).
		if lifeCtx.Err() != nil {
			return nil
		}
		if err != nil {
			return ConfigsLoadedMsg{Err: err}
		}
		return ConfigsLoadedMsg{Configs: cfgs}
	}
}
