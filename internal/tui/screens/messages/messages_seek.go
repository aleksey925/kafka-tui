package messages

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
)

// ----- seek popup -----

type seekStage int

const (
	stageMenu seekStage = iota
	stageInput
)

type seekPopup struct {
	stage  seekStage
	chosen SeekMode
	menu   *components.Menu
	form   *components.Form
}

// seekBindings exposes the input-stage bindings when the user is typing a
// value (offset / timestamp). The menu stage has no entry here because the
// menu component owns its own key set; the input stage dispatches
// esc / enter through this slice.
func (m *Model) seekBindings() []keymap.Binding {
	if m.seekPopup != nil && m.seekPopup.stage == stageInput {
		bs := []keymap.Binding{
			{Keys: []string{"enter"}, Label: "apply seek", Category: "Seek", Hint: true, Handler: m.actSeekApply},
			{Keys: []string{"esc"}, Label: "back to strategy menu", Category: "Seek", Hint: true, Handler: m.actSeekBackToMenu},
		}
		return append(bs, m.seekPopup.form.Bindings("Form")...)
	}
	if m.seekPopup != nil && m.seekPopup.menu != nil {
		return m.seekPopup.menu.Bindings("Seek")
	}
	return nil
}

func (m *Model) actSeekApply() tea.Cmd {
	pop := m.seekPopup
	state, err := m.parseSeekForm(pop.chosen, pop.form)
	if err != nil {
		m.toasts.Push(components.ToastError, err.Error())
		return nil
	}
	persist := m.applySeek(state)
	m.closeSeek()
	return tea.Batch(persist, m.dispatchSeek())
}

func (m *Model) actSeekBackToMenu() tea.Cmd {
	pop := m.seekPopup
	pop.stage = stageMenu
	pop.form = nil
	pop.menu.Reset()
	return nil
}

func (m *Model) openSeek() {
	cursor := int(m.seek.Mode)
	items := []components.MenuItem{
		{Label: "latest"},
		{Label: "earliest"},
		{Label: "from offset"},
		{Label: "to offset"},
		{Label: "from timestamp"},
		{Label: "to timestamp"},
		{Label: "live"},
	}
	menu := components.NewMenu(items,
		components.WithMenuStyles(m.styles),
		components.WithMenuTitle("seek"),
		components.WithMenuCursor(cursor),
	)
	m.seekPopup = &seekPopup{stage: stageMenu, menu: menu}
	m.mode = ModeSeek
}

func (m *Model) handleSeekKey(key tea.KeyPressMsg) tea.Cmd {
	if m.seekPopup == nil {
		m.mode = ModeList
		return nil
	}
	if m.seekPopup.stage == stageInput {
		return m.handleSeekInput(key)
	}
	pop := m.seekPopup
	pop.menu, _ = pop.menu.Update(key)
	if pop.menu.Canceled() {
		m.closeSeek()
		return nil
	}
	if idx, _, ok := pop.menu.Selected(); ok {
		mode := SeekMode(idx)
		pop.chosen = mode
		switch mode {
		case SeekLatest, SeekEarliest, SeekLive:
			persist := m.applySeek(SeekState{Mode: mode})
			m.closeSeek()
			return tea.Batch(persist, m.dispatchSeek())
		default:
			pop.stage = stageInput
			pop.form = m.buildSeekForm(mode)
		}
	}
	return nil
}

func (m *Model) handleSeekInput(key tea.KeyPressMsg) tea.Cmd {
	pop := m.seekPopup
	if cmd, ok := keymap.Dispatch(m.seekBindings(), key); ok {
		return cmd
	}
	pop.form, _ = pop.form.Update(key)
	return nil
}

func (m *Model) closeSeek() {
	m.seekPopup = nil
	m.mode = ModeList
}

func (m *Model) buildSeekForm(mode SeekMode) *components.Form {
	var label, prefill string
	switch mode {
	case SeekFromOffset, SeekToOffset:
		label = "offset (partition:offset or offset)"
		if msg, ok := m.selected(); ok {
			prefill = strconv.FormatInt(int64(msg.Partition), 10) + ":" + strconv.FormatInt(msg.Offset, 10)
		}
	case SeekFromTimestamp, SeekToTimestamp:
		label = "timestamp (RFC3339, '1h ago', 'today', …)"
		if msg, ok := m.selected(); ok && !msg.Timestamp.IsZero() {
			prefill = msg.Timestamp.UTC().Format(time.RFC3339)
		}
	case SeekLatest, SeekEarliest, SeekLive:
	}
	return components.NewForm(
		[]components.Field{{Key: "value", Label: label, Kind: components.FieldText, Value: prefill}},
		components.WithFormStyles(m.styles),
	)
}

func (m *Model) parseSeekForm(mode SeekMode, form *components.Form) (SeekState, error) {
	fld, _ := form.Field("value")
	raw := strings.TrimSpace(fld.Value)
	switch mode {
	case SeekFromOffset, SeekToOffset:
		p, off, hasPart, err := parseOffsetExpression(raw)
		if err != nil {
			return SeekState{}, err
		}
		return SeekState{Mode: mode, Partition: p, Offset: off, HasPart: hasPart}, nil
	case SeekFromTimestamp, SeekToTimestamp:
		ts, err := kafka.ParseTimestamp(raw, m.now())
		if err != nil {
			return SeekState{}, fmt.Errorf("invalid timestamp: %w", err)
		}
		return SeekState{Mode: mode, Timestamp: ts}, nil
	case SeekLatest, SeekEarliest, SeekLive:
	}
	return SeekState{Mode: mode}, nil
}

// parseOffsetExpression accepts `partition:offset` or `offset`.
func parseOffsetExpression(s string) (int32, int64, bool, error) {
	if s == "" {
		return 0, 0, false, errors.New("invalid offset: expected partition:offset or offset")
	}
	if strings.Contains(s, ":") {
		parts := strings.SplitN(s, ":", 2)
		p, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 32)
		if err != nil || p < 0 {
			return 0, 0, false, fmt.Errorf("invalid offset: bad partition %q", parts[0])
		}
		off, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil || off < 0 {
			return 0, 0, false, fmt.Errorf("invalid offset: bad offset %q", parts[1])
		}
		return int32(p), off, true, nil
	}
	off, err := strconv.ParseInt(s, 10, 64)
	if err != nil || off < 0 {
		return 0, 0, false, errors.New("invalid offset: expected partition:offset or offset")
	}
	return 0, off, false, nil
}

func (m *Model) renderSeekPopup() string {
	if m.seekPopup.stage == stageMenu {
		return m.seekPopup.menu.View(0)
	}
	title := m.styles.HelpTitle.Render("seek · " + m.seekPopup.chosen.String())
	hint := components.HintLine(m.styles,
		components.Hint{Key: "enter", Label: "ok"},
		components.Hint{Key: "esc", Label: "back"},
	)
	body := title + "\n\n" + m.seekPopup.form.View() + "\n\n" + hint
	return lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		Padding(0, 2).
		Render(body)
}
