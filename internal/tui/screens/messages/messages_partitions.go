package messages

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
	"github.com/aleksey925/kafka-tui/internal/tui/lineedit"
)

type partitionsFocus int

const (
	focusList partitionsFocus = iota
	focusInput
)

// partitionsPopup keeps checkbox list and input in sync — toggling a
// checkbox rewrites the input; valid edits re-tick checkboxes.
type partitionsPopup struct {
	loading      bool
	loadErr      string
	partitions   []int32
	selected     map[int32]bool
	listCursor   int
	listScroll   int
	focus        partitionsFocus
	input        string
	inputCursor  int
	parseErr     string
	allDiscarded bool // parsed ok but referenced unknown partitions
}

type partitionsLoadedMsg struct {
	partitions []int32
	err        error
}

// popupChromeRows must be kept in sync with renderPartitionsPopup — the
// list area on big topics depends on it for scroll bounds.
const popupChromeRows = 12

func (m *Model) partitionsBindings() []keymap.Binding {
	bs := []keymap.Binding{
		{Keys: []string{"enter"}, Label: "apply partition filter", Category: "Partition filter", Hint: true, Handler: m.actPartApply},
		{Keys: []string{"esc"}, Label: "back", Category: "Partition filter", Hint: true, Handler: m.actPartCancel},
		keymap.FocusToggle("switch focus (list ↔ input)", "Partition filter", m.actPartToggleFocus),
	}
	if m.partitionsPopup != nil && m.partitionsPopup.focus == focusList {
		bs = append(bs,
			keymap.Binding{Keys: []string{"space", " "}, DisplayKeys: []string{"space"}, Label: "toggle partition", Category: "Partition filter", Handler: m.actPartToggle},
			keymap.Binding{Keys: []string{"a"}, Label: "toggle all", Category: "Partition filter", Handler: m.actPartToggleAll},
			keymap.Binding{Keys: []string{"k", "up"}, Label: "previous partition", Category: "Partition filter", Handler: m.actPartCursor(-1)},
			keymap.Binding{Keys: []string{"j", "down"}, Label: "next partition", Category: "Partition filter", Handler: m.actPartCursor(+1)},
			keymap.Binding{Keys: []string{"home"}, Label: "first partition", Category: "Partition filter", Handler: m.actPartCursorTo(0)},
			keymap.Binding{Keys: []string{"end"}, Label: "last partition", Category: "Partition filter", Handler: m.actPartCursorTo(-1)},
		)
	}
	return bs
}

func (m *Model) actPartApply() tea.Cmd {
	pop := m.partitionsPopup
	if pop.parseErr != "" {
		m.toasts.Push(components.ToastError, pop.parseErr)
		return nil
	}
	var parts []int32
	if pop.partitions != nil {
		parts = m.selectedPartitions()
		if len(parts) == len(pop.partitions) {
			parts = nil
		}
	} else {
		p, err := kafka.ParsePartitionFilter(pop.input)
		if err != nil {
			m.toasts.Push(components.ToastError, err.Error())
			return nil
		}
		parts = p
	}
	m.filter = parts
	m.partitionsPopup = nil
	m.mode = ModeList
	return tea.Batch(m.persistView(), m.dispatchSeek())
}

func (m *Model) actPartCancel() tea.Cmd {
	m.partitionsPopup = nil
	m.mode = ModeList
	return nil
}

func (m *Model) actPartToggleFocus() tea.Cmd {
	pop := m.partitionsPopup
	if pop.focus == focusList {
		pop.focus = focusInput
	} else {
		pop.focus = focusList
	}
	return nil
}

func (m *Model) actPartToggle() tea.Cmd {
	pop := m.partitionsPopup
	if len(pop.partitions) == 0 {
		return nil
	}
	p := pop.partitions[pop.listCursor]
	if pop.selected[p] {
		delete(pop.selected, p)
	} else {
		pop.selected[p] = true
	}
	m.syncInputFromSelection()
	return nil
}

func (m *Model) actPartToggleAll() tea.Cmd {
	pop := m.partitionsPopup
	if len(pop.partitions) == 0 {
		return nil
	}
	if len(pop.selected) == len(pop.partitions) {
		pop.selected = map[int32]bool{}
	} else {
		for _, p := range pop.partitions {
			pop.selected[p] = true
		}
	}
	m.syncInputFromSelection()
	return nil
}

func (m *Model) actPartCursor(delta int) func() tea.Cmd {
	return func() tea.Cmd {
		pop := m.partitionsPopup
		if len(pop.partitions) == 0 {
			return nil
		}
		n := len(pop.partitions)
		pop.listCursor = (pop.listCursor + delta + n) % n
		return nil
	}
}

func (m *Model) actPartCursorTo(idx int) func() tea.Cmd {
	return func() tea.Cmd {
		pop := m.partitionsPopup
		if len(pop.partitions) == 0 {
			return nil
		}
		if idx < 0 {
			idx = len(pop.partitions) - 1
		}
		pop.listCursor = idx
		return nil
	}
}

func loadPartitionsCmd(svc Service, topic string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		wm, err := svc.WatermarksFor(ctx, topic, nil)
		if err != nil {
			return partitionsLoadedMsg{err: err}
		}
		out := make([]int32, 0, len(wm))
		for p := range wm {
			out = append(out, p)
		}
		slices.Sort(out)
		return partitionsLoadedMsg{partitions: out}
	}
}

func (m *Model) openPartitions() tea.Cmd {
	m.partitionsPopup = &partitionsPopup{
		loading:  true,
		selected: map[int32]bool{},
		input:    renderPartitionFilter(m.filter),
		focus:    focusList,
	}
	m.partitionsPopup.inputCursor = runeLen(m.partitionsPopup.input)
	m.mode = ModePartitions
	return loadPartitionsCmd(m.svc, m.topic)
}

func (m *Model) handlePartitionsLoaded(msg partitionsLoadedMsg) {
	if m.partitionsPopup == nil {
		return
	}
	pop := m.partitionsPopup
	pop.loading = false
	if msg.err != nil {
		pop.loadErr = msg.err.Error()
		return
	}
	pop.partitions = msg.partitions
	pop.selected = map[int32]bool{}
	if len(m.filter) == 0 {
		for _, p := range pop.partitions {
			pop.selected[p] = true
		}
	} else {
		want := map[int32]bool{}
		for _, p := range m.filter {
			want[p] = true
		}
		for _, p := range pop.partitions {
			if want[p] {
				pop.selected[p] = true
			}
		}
	}
	pop.input = m.canonicalSelection()
	pop.inputCursor = runeLen(pop.input)
}

func (m *Model) handlePartitionsKey(key tea.KeyPressMsg) tea.Cmd {
	if m.partitionsPopup == nil {
		m.mode = ModeList
		return nil
	}
	if cmd, ok := keymap.Dispatch(m.partitionsBindings(), key); ok {
		return cmd
	}
	if m.partitionsPopup.focus == focusInput {
		m.handlePartitionsInputKey(key)
	}
	return nil
}

func (m *Model) handlePartitionsInputKey(key tea.KeyPressMsg) {
	pop := m.partitionsPopup
	state, ok := lineedit.Apply(lineedit.State{
		Runes:  []rune(pop.input),
		Cursor: pop.inputCursor,
	}, key)
	if !ok {
		return
	}
	newBuf := state.String()
	changed := newBuf != pop.input
	pop.input = newBuf
	pop.inputCursor = state.Cursor
	if changed {
		m.syncSelectionFromInput()
	}
}

// handlePartitionsPaste sanitizes the pasted payload through lineedit and
// merges it into the popup input. The popup is single-line, so newlines /
// tabs are flattened to spaces per the project paste contract.
func (m *Model) handlePartitionsPaste(content string) {
	pop := m.partitionsPopup
	state := lineedit.InsertText(lineedit.State{
		Runes:  []rune(pop.input),
		Cursor: pop.inputCursor,
	}, content)
	newBuf := state.String()
	changed := newBuf != pop.input
	pop.input = newBuf
	pop.inputCursor = state.Cursor
	if changed {
		m.syncSelectionFromInput()
	}
}

func (m *Model) selectedPartitions() []int32 {
	pop := m.partitionsPopup
	out := make([]int32, 0, len(pop.selected))
	for _, p := range pop.partitions {
		if pop.selected[p] {
			out = append(out, p)
		}
	}
	return out
}

// canonicalSelection: "all ticked" / "none ticked" both emit "" to match
// the "all partitions" convention.
func (m *Model) canonicalSelection() string {
	pop := m.partitionsPopup
	if len(pop.partitions) == 0 {
		return ""
	}
	picks := m.selectedPartitions()
	if len(picks) == len(pop.partitions) {
		return ""
	}
	return renderPartitionFilter(picks)
}

func (m *Model) syncInputFromSelection() {
	pop := m.partitionsPopup
	pop.input = m.canonicalSelection()
	pop.inputCursor = runeLen(pop.input)
	pop.parseErr = ""
	pop.allDiscarded = false
}

// syncSelectionFromInput keeps checkbox state stable on invalid input.
// References to unknown partitions are a soft warning (allDiscarded), not
// a block — the kafka layer silently drops them on fetch.
func (m *Model) syncSelectionFromInput() {
	pop := m.partitionsPopup
	if pop.partitions == nil {
		// metadata not yet loaded — validate syntax only.
		_, err := kafka.ParsePartitionFilter(pop.input)
		if err != nil {
			pop.parseErr = err.Error()
		} else {
			pop.parseErr = ""
		}
		return
	}
	parts, err := kafka.ParsePartitionFilter(pop.input)
	if err != nil {
		pop.parseErr = err.Error()
		return
	}
	pop.parseErr = ""
	pop.allDiscarded = false
	known := map[int32]bool{}
	for _, p := range pop.partitions {
		known[p] = true
	}
	pop.selected = map[int32]bool{}
	if len(parts) == 0 {
		for _, p := range pop.partitions {
			pop.selected[p] = true
		}
		return
	}
	unknownCount := 0
	for _, p := range parts {
		if known[p] {
			pop.selected[p] = true
		} else {
			unknownCount++
		}
	}
	if unknownCount > 0 && len(pop.selected) == 0 {
		pop.allDiscarded = true
	}
}

func (m *Model) renderPartitionsPopup() string {
	pop := m.partitionsPopup
	title := m.styles.HelpTitle.Render("partition filter")

	var listBlock string
	switch {
	case pop.loading:
		listBlock = "    " + m.styles.HintLabel.Render("loading partitions…")
	case pop.loadErr != "":
		listBlock = "    " + m.styles.StatusErr.Render("load failed: "+pop.loadErr)
	case len(pop.partitions) == 0:
		listBlock = "    " + m.styles.HintLabel.Render("(topic has no partitions)")
	default:
		maxRows := m.partitionsListWindow()
		m.clampPartitionsScroll(maxRows)
		first := pop.listScroll
		last := min(first+maxRows, len(pop.partitions))
		rows := make([]string, 0, last-first+2)
		if first > 0 {
			rows = append(rows, "    "+m.styles.HintLabel.Render(fmt.Sprintf("↑ %d more", first)))
		}
		for i := first; i < last; i++ {
			p := pop.partitions[i]
			marker := "[ ]"
			if pop.selected[p] {
				marker = "[×]"
			}
			prefix := "  "
			rowStyle := m.styles.Command
			if pop.focus == focusList && i == pop.listCursor {
				prefix = "▸ "
				rowStyle = m.styles.CommandHL
			}
			rows = append(rows, prefix+rowStyle.Render(fmt.Sprintf("%s %d", marker, p)))
		}
		if last < len(pop.partitions) {
			rows = append(rows, "    "+m.styles.HintLabel.Render(fmt.Sprintf("↓ %d more", len(pop.partitions)-last)))
		}
		listBlock = strings.Join(rows, "\n")
	}

	var listLabel string
	if pop.focus == focusList {
		listLabel = m.styles.HintKey.Render("▸ partitions")
	} else {
		listLabel = m.styles.HintLabel.Render("  partitions")
	}

	var inputLabel string
	if pop.focus == focusInput {
		inputLabel = m.styles.HintKey.Render("▸ filter")
	} else {
		inputLabel = m.styles.HintLabel.Render("  filter")
	}
	inputBody := m.renderPartitionsInputField()
	var inputErr string
	switch {
	case pop.parseErr != "":
		inputErr = "    " + m.styles.StatusErr.Render("invalid: "+pop.parseErr)
	case pop.allDiscarded:
		inputErr = "    " + m.styles.StatusWarn.Render("none of the listed partitions exist in this topic")
	}

	hint := m.styles.HintLabel.Render("tab switch   space toggle   a all/none   enter apply   esc back")

	parts := []string{
		title,
		"",
		listLabel,
		listBlock,
		"",
		inputLabel,
		inputBody,
	}
	if inputErr != "" {
		parts = append(parts, inputErr)
	}
	parts = append(parts, "", hint)
	return lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		Padding(0, 2).
		Render(strings.Join(parts, "\n"))
}

func (m *Model) partitionsListWindow() int {
	avail := m.bodyHeight() - popupChromeRows
	if avail < 3 {
		return 3
	}
	return avail
}

func (m *Model) clampPartitionsScroll(window int) {
	pop := m.partitionsPopup
	if window <= 0 || len(pop.partitions) == 0 {
		pop.listScroll = 0
		return
	}
	if pop.listCursor < pop.listScroll {
		pop.listScroll = pop.listCursor
	}
	if pop.listCursor >= pop.listScroll+window {
		pop.listScroll = pop.listCursor - window + 1
	}
	maxScroll := max(len(pop.partitions)-window, 0)
	if pop.listScroll > maxScroll {
		pop.listScroll = maxScroll
	}
	if pop.listScroll < 0 {
		pop.listScroll = 0
	}
}

func (m *Model) renderPartitionsInputField() string {
	pop := m.partitionsPopup
	if pop.focus != focusInput {
		if pop.input == "" {
			return "    " + m.styles.HintLabel.Render("(empty = all)")
		}
		return "    " + m.styles.Command.Render(pop.input)
	}
	runes := []rune(pop.input)
	cur := min(pop.inputCursor, len(runes))
	before := string(runes[:cur])
	var underCursor, after string
	if cur >= len(runes) {
		underCursor = " "
	} else {
		underCursor = string(runes[cur])
		after = string(runes[cur+1:])
	}
	return "    " + m.styles.Command.Render(before) + m.styles.Cursor.Render(underCursor) + m.styles.Command.Render(after)
}
