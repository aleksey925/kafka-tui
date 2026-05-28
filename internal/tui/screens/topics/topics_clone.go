package topics

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/components"
	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
)

// CloneProgressMsg is dispatched on every progress chunk from the kafka
// clone worker — the screen drives a chain of clonePollCmds until it
// receives one with Done=true.
type CloneProgressMsg struct {
	Progress kafka.CloneProgress
}

// cloneStartedMsg hands the freshly-opened progress channel back to the
// model so it can drive a chain of clonePollCmds.
type cloneStartedMsg struct {
	ch     <-chan kafka.CloneProgress
	cancel context.CancelFunc
}

func (m *Model) actCloneTopic() tea.Cmd {
	if m.readOnly {
		return m.blockedReadOnly("clone")
	}
	m.openCloneForm()
	return nil
}

func (m *Model) openCloneForm() {
	row, ok := m.table.SelectedRow()
	if !ok {
		m.toasts.Push(components.ToastWarning, "no topic selected")
		return
	}
	m.clone = NewCloneForm(row.ID, m.styles)
	m.mode = ModeClone
}

func (m *Model) handleCloneKey(key tea.KeyPressMsg) tea.Cmd {
	if m.cloneConfirm != nil {
		return m.handleCloneConfirmKey(key)
	}
	inEdit := m.clone.Mode() == FormInsert || m.clone.Form().PopupActive()
	if !inEdit || key.String() == "esc" {
		if cmd, ok := keymap.Dispatch(m.cloneBindings(), key); ok {
			return cmd
		}
	}
	c, _ := m.clone.Update(key)
	m.clone = c
	return nil
}

func (m *Model) handleCloneConfirmKey(key tea.KeyPressMsg) tea.Cmd {
	c, _ := m.cloneConfirm.Update(key)
	m.cloneConfirm = c
	switch c.Result() {
	case components.ConfirmPending:
		return nil
	case components.ConfirmYes:
		m.cloneConfirm = nil
		return m.actCloneSubmit()
	case components.ConfirmNo:
		m.cloneConfirm = nil
	}
	return nil
}

func (m *Model) handleCloningKey(key tea.KeyPressMsg) tea.Cmd {
	cmd, _ := keymap.Dispatch(m.cloningBindings(), key)
	return cmd
}

func (m *Model) cloneBindings() []keymap.Binding {
	if m.cloneConfirm != nil {
		return m.cloneConfirm.Bindings("Clone topic", "clone")
	}
	return []keymap.Binding{
		{Keys: []string{"s"}, Label: "submit (clone topic)", Category: "Clone topic", Hint: true, Handler: m.actOpenCloneConfirm},
		{Keys: []string{"esc"}, Label: "cancel / leave INSERT / close popup", Category: "Clone topic", Hint: true, HandlerMsg: m.actCloneEsc},
	}
}

func (m *Model) cloningBindings() []keymap.Binding {
	return []keymap.Binding{
		{Keys: []string{"esc"}, Label: "leave (clone keeps running in background)", Category: "Cloning", Hint: true, Handler: m.actCloningLeave},
	}
}

// actOpenCloneConfirm validates the form and mounts the confirm modal so
// the user sees `src → dst` before committing — clone is reversible but
// can be expensive (it copies data), so we gate it the same way as a
// destructive action.
func (m *Model) actOpenCloneConfirm() tea.Cmd {
	src, dst, err := m.clone.Submit()
	if err != nil {
		m.clone.SetError(err.Error())
		return nil
	}
	m.clone.SetError("")
	m.cloneConfirm = components.NewConfirm(
		"Clone topic",
		fmt.Sprintf("Clone %s → %s?", src, dst),
		components.WithConfirmStyles(m.styles),
	)
	return nil
}

func (m *Model) actCloneSubmit() tea.Cmd {
	src, dst, err := m.clone.Submit()
	if err != nil {
		m.clone.SetError(err.Error())
		return nil
	}
	m.mode = ModeCloning
	m.progress = kafka.CloneProgress{}
	m.cloneSrc, m.cloneDst = src, dst
	m.toasts.Push(components.ToastInfo, "cloning "+src+" → "+dst+"…")
	return cloneStartCmd(m.svc, src, dst, m.clone.Options())
}

func (m *Model) actCloneEsc(key tea.KeyPressMsg) tea.Cmd {
	if m.clone.Mode() == FormInsert || m.clone.Form().PopupActive() {
		c, _ := m.clone.Update(key)
		m.clone = c
		return nil
	}
	m.clone = nil
	m.mode = ModeList
	return nil
}

func (m *Model) actCloningLeave() tea.Cmd {
	m.mode = ModeList
	return nil
}

func (m *Model) handleCloneProgress(msg CloneProgressMsg) tea.Cmd {
	m.progress = msg.Progress
	if msg.Progress.Done {
		m.mode = ModeList
		m.clone = nil
		ch := m.cloneCh
		m.cloneCh = nil
		if m.cloneCxl != nil {
			m.cloneCxl()
			m.cloneCxl = nil
		}
		label := fmt.Sprintf("%s → %s", m.cloneSrc, m.cloneDst)
		m.cloneSrc, m.cloneDst = "", ""
		if msg.Progress.Err != nil {
			m.toasts.Push(components.ToastError, fmt.Sprintf("clone %s: %s", label, msg.Progress.Err.Error()))
		} else {
			m.toasts.Push(components.ToastSuccess, fmt.Sprintf("clone done %s — %d records", label, msg.Progress.Copied))
		}
		// drain any remaining items so the producer goroutine isn't blocked
		// waiting on a closed-but-buffered channel.
		if ch != nil {
			go drainChannel(ch)
		}
		return nil
	}
	if m.cloneCh != nil {
		return clonePollCmd(m.cloneCh)
	}
	return nil
}

func (m *Model) renderCloningOverlay() string {
	header := m.styles.HelpTitle.Render("Cloning…")
	body := fmt.Sprintf(
		"copied %s / %s records",
		formatThousands(m.progress.Copied),
		formatThousands(m.progress.Total),
	)
	hint := components.HintLine(m.styles,
		components.Hint{Key: "esc", Label: "return to list (clone continues in background)"},
	)
	return strings.Join([]string{header, m.styles.Command.Render(body), "", hint}, "\n")
}

func cloneStartCmd(svc Service, src, dst string, opts kafka.CloneOptions) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		ch, err := svc.CloneTopic(ctx, src, dst, opts)
		if err != nil {
			cancel()
			return CloneProgressMsg{Progress: kafka.CloneProgress{Done: true, Err: err}}
		}
		return cloneStartedMsg{ch: ch, cancel: cancel}
	}
}

// clonePollCmd reads one progress message from ch. When the channel closes
// before a Done flag arrived, it synthesizes one so the screen always
// transitions back to ModeList.
func clonePollCmd(ch <-chan kafka.CloneProgress) tea.Cmd {
	return func() tea.Msg {
		p, ok := <-ch
		if !ok {
			return CloneProgressMsg{Progress: kafka.CloneProgress{Done: true}}
		}
		return CloneProgressMsg{Progress: p}
	}
}

// drainChannel releases the clone goroutine when the user transitions away
// before the channel is fully drained.
func drainChannel(ch <-chan kafka.CloneProgress) {
	for range ch {
		_ = struct{}{}
	}
}
