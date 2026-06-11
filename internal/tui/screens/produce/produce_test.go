package produce_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/produce"
)

func drive(t *testing.T, m *produce.Model, cmd tea.Cmd) {
	t.Helper()
	queue := []tea.Cmd{cmd}
	for len(queue) > 0 {
		next := queue[0]
		queue = queue[1:]
		if next == nil {
			continue
		}
		msg := next()
		if msg == nil {
			continue
		}
		if batch, ok := msg.(tea.BatchMsg); ok {
			queue = append(queue, batch...)
			continue
		}
		follow := m.Update(msg)
		queue = append(queue, follow)
	}
}

func TestNew_RendersHeaderAndFields(t *testing.T) {
	// arrange
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})

	// act
	out := m.View()

	// Topic appears in the screen title (rendered by the host frame), not
	// as an editable form field.
	assert.Equal(t, "Produce · orders", m.Title())
	for _, want := range []string{"Partition", "Compression", "Key", "Headers", "Value"} {
		assert.Contains(t, out, want)
	}
	_, ok := m.Form().Field("topic")
	assert.False(t, ok, "topic must not exist as a form field")
}

// TestWantsRawInput_TracksInsertMode pins the carve-out: raw-input
// only kicks in once the user has entered INSERT mode. NORMAL mode
// stays non-raw so global shortcuts like `?` (help) keep working.
func TestWantsRawInput_TracksInsertMode(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})

	// fresh form starts in NORMAL — `?` etc. must remain global.
	assert.False(t, m.WantsRawInput())

	// tab to the Key text field (past Partition + Compression segmented
	// fields), then enter to flip into INSERT.
	_ = m.Update(keyPress("tab"))
	_ = m.Update(keyPress("tab"))
	_ = m.Update(keyPress("enter"))
	assert.True(t, m.WantsRawInput(), "INSERT must enable raw-input")

	// esc returns to NORMAL → raw-input lifts again.
	_ = m.Update(keyPress("esc"))
	assert.False(t, m.WantsRawInput())
}

func TestEsc_RaisesBackAction(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})

	_ = m.Update(keyPress("esc"))

	assert.True(t, m.ConsumeAction().Back)
}

func TestSendConfirm_YesSendsAndClosesOnSuccess(t *testing.T) {
	// arrange
	svc := newFakeService()
	svc.result = kafka.ProduceResult{Topic: "orders", Partition: 2, Offset: 99, Duration: 12 * time.Millisecond}
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	typeText(m, "value", "hello")

	// act
	drive(t, m, sendAndConfirm(m))

	// assert
	require.Len(t, svc.Sent(), 1)
	assert.Equal(t, "orders", svc.Sent()[0].Topic)
	assert.Equal(t, []byte("hello"), svc.Sent()[0].Value)

	a := m.ConsumeAction()
	assert.True(t, a.Back, "y must close after a successful send")
	require.NotNil(t, a.Sent)
	assert.Equal(t, int64(99), a.Sent.Offset)
}

func TestSendConfirm_KeepKeepsFormOpen(t *testing.T) {
	svc := newFakeService()
	svc.result = kafka.ProduceResult{Topic: "orders", Partition: 0, Offset: 1, Duration: time.Millisecond}
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	typeText(m, "value", "x")

	drive(t, m, sendAndKeep(m))

	require.Len(t, svc.Sent(), 1)
	a := m.ConsumeAction()
	assert.False(t, a.Back, "k must NOT close the form")
	require.NotNil(t, a.Sent)
}

func TestSendConfirm_EscDismissesWithoutSending(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	typeText(m, "value", "hello")
	_ = m.Update(keyPress("esc"))

	_ = m.Update(keyPress("s"))
	require.True(t, m.SendConfirmOpen())

	_ = m.Update(keyPress("esc"))
	assert.False(t, m.SendConfirmOpen(), "esc must close the confirm modal")
	assert.Empty(t, svc.Sent(), "dismissing the modal must not send anything")
}

func TestSendConfirm_EnterIsNoOp(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	typeText(m, "value", "hello")
	_ = m.Update(keyPress("esc"))

	_ = m.Update(keyPress("s"))
	require.True(t, m.SendConfirmOpen())

	_ = m.Update(keyPress("enter"))
	assert.True(t, m.SendConfirmOpen(), "enter must NOT confirm — anti-accident contract")
	assert.Empty(t, svc.Sent())
}

func TestSendConfirm_ViewShowsClusterAndTopic(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders", Cluster: "staging"})
	m.SetSize(80, 20)

	_ = m.Update(keyPress("s"))
	require.True(t, m.SendConfirmOpen())

	out := m.View()
	assert.Contains(t, out, "staging", "modal must surface the cluster name")
	assert.Contains(t, out, "orders", "modal must surface the topic name")
}

func TestSendConfirm_PasteIsDroppedWhileOpen(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	typeText(m, "value", "hello")
	_ = m.Update(keyPress("esc"))

	_ = m.Update(keyPress("s"))
	require.True(t, m.SendConfirmOpen())

	// paste-while-modal-open used to leak the buffer into the focused
	// field and silently flip the form back into INSERT — the record
	// the user was being asked to confirm would then carry the pasted
	// chunk. The modal must drop the event entirely.
	_ = m.Update(tea.PasteMsg{Content: "leak"})

	got, _ := m.Form().Field("value")
	assert.Equal(t, "hello", got.Value, "paste must not mutate the form while the modal owns input")
	assert.Equal(t, produce.ModeNormal, m.Mode(), "paste must not flip the form to INSERT under the modal")
	assert.True(t, m.SendConfirmOpen())
}

func TestS_FromInsertIsLiteral(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	m.Form().FocusKey("value")
	_ = m.Update(keyPress("enter")) // INSERT

	_ = m.Update(keyPressRune('s'))

	got, _ := m.Form().Field("value")
	assert.Equal(t, "s", got.Value, "s in INSERT must be typed, not open the send confirm")
	assert.False(t, m.SendConfirmOpen())
}

func TestSend_ToastIncludesPartitionOffsetAndLatency(t *testing.T) {
	svc := newFakeService()
	svc.result = kafka.ProduceResult{Topic: "orders", Partition: 4, Offset: 7, Duration: 42 * time.Millisecond}
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	typeText(m, "value", "x")

	drive(t, m, sendAndConfirm(m))

	require.GreaterOrEqual(t, m.Toasts().Len(), 1)
	assert.Contains(t, m.Toasts().Items()[m.Toasts().Len()-1].Message, "Sent to orders P4:7 (42ms)")
}

func TestSend_FailureSurfacesErrorToast(t *testing.T) {
	svc := newFakeService()
	svc.err = errors.New("broker unavailable")
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	typeText(m, "value", "x")

	drive(t, m, sendAndConfirm(m))

	a := m.ConsumeAction()
	assert.False(t, a.Back, "failed send must not close the form")
	assert.Nil(t, a.Sent)
	assert.Contains(t, m.View(), "broker unavailable")
}

func TestSend_ValidationErrorOnEmptyTopic(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: ""})

	_ = m.Update(keyPress("s"))

	assert.False(t, m.SendConfirmOpen(), "validation error must abort before opening the modal")
	assert.Empty(t, svc.Sent(), "validation error must abort before calling Service")
	assert.Contains(t, m.View(), "topic is required")
}

func TestPartition_AutoEqualsKafkaAuto(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})

	drive(t, m, sendAndConfirm(m))

	require.Len(t, svc.Sent(), 1)
	assert.Equal(t, kafka.PartitionAuto, svc.Sent()[0].Partition)
}

func TestPartition_SegmentedOptionsLoadFromService(t *testing.T) {
	svc := newFakeService()
	svc.setPartitions(0, 1, 2, 3, 4)
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())

	got, ok := m.Form().Field("partition")
	require.True(t, ok)
	assert.Equal(t, []string{"auto", "0", "1", "2", "3", "4"}, got.Options)
}

func TestPartition_CycleSelectsManualNumber(t *testing.T) {
	svc := newFakeService()
	svc.setPartitions(0, 1, 2, 3, 4)
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())

	m.Form().FocusKey("partition")
	// auto → 0 → 1 → 2 → 3
	for range 4 {
		_ = m.Update(keyPress("right"))
	}

	drive(t, m, sendAndConfirm(m))

	require.Len(t, svc.Sent(), 1)
	assert.Equal(t, int32(3), svc.Sent()[0].Partition)
}

func TestPartition_TypeToJumpSelectsExactMatch(t *testing.T) {
	svc := newFakeService()
	svc.setPartitions(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())
	m.Form().FocusKey("partition")

	// "1" then "2" → "12" (multi-digit accumulation against the running buffer).
	_ = m.Update(keyPress("1"))
	got, _ := m.Form().Field("partition")
	assert.Equal(t, "1", got.Value)

	_ = m.Update(keyPress("2"))
	got, _ = m.Form().Field("partition")
	assert.Equal(t, "12", got.Value)
}

func TestPartition_TypeToJumpRestartsBufferOnPrefixMiss(t *testing.T) {
	svc := newFakeService()
	svc.setPartitions(0, 1, 2, 3, 4)
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())
	m.Form().FocusKey("partition")

	// "4" → "4". Then "9": buffer "49" matches no option, but "9" alone
	// also doesn't match (max is 4). Buffer is dropped silently and the
	// previous selection survives.
	_ = m.Update(keyPress("4"))
	_ = m.Update(keyPress("9"))
	got, _ := m.Form().Field("partition")
	assert.Equal(t, "4", got.Value)

	// "2" — fresh single-digit jump.
	_ = m.Update(keyPress("2"))
	got, _ = m.Form().Field("partition")
	assert.Equal(t, "2", got.Value)
}

func TestPartition_TypeToJumpClearedByFocusChange(t *testing.T) {
	svc := newFakeService()
	svc.setPartitions(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())
	m.Form().FocusKey("partition")

	_ = m.Update(keyPress("1"))
	_ = m.Update(keyPress("tab")) // focus → compression, buffer cleared
	_ = m.Update(keyPress("shift+tab"))
	_ = m.Update(keyPress("2")) // fresh buffer "2", not "12"

	got, _ := m.Form().Field("partition")
	assert.Equal(t, "2", got.Value)
}

func TestPartition_LoadFailureSurfacesToast(t *testing.T) {
	svc := newFakeService()
	svc.partErr = errors.New("metadata unavailable")
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())

	got, ok := m.Form().Field("partition")
	require.True(t, ok)
	assert.Equal(t, []string{"auto"}, got.Options)
	require.GreaterOrEqual(t, m.Toasts().Len(), 1)
	assert.Contains(t, m.Toasts().Items()[m.Toasts().Len()-1].Message, "metadata unavailable")
}

func TestHeaders_ParsedAsKeyEquals(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})

	m.Form().FocusKey("headers")
	_ = m.Update(keyPress("enter")) // NORMAL → INSERT, empty row added
	for _, r := range "x-trace=abc" {
		_ = m.Update(keyPressRune(r))
	}
	_ = m.Update(keyPress("enter")) // commit + new empty row, still INSERT
	for _, r := range "x-source=ui" {
		_ = m.Update(keyPressRune(r))
	}

	drive(t, m, sendAndConfirm(m))

	require.Len(t, svc.Sent(), 1)
	got := svc.Sent()[0].Headers
	assert.Equal(t, []kafka.Header{
		{Key: "x-trace", Value: []byte("abc")},
		{Key: "x-source", Value: []byte("ui")},
	}, got)
}

func TestHeaders_InvalidEntryRejected(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})

	m.Form().FocusKey("headers")
	_ = m.Update(keyPress("enter")) // NORMAL → INSERT, empty row added
	for _, r := range "no-equals-sign" {
		_ = m.Update(keyPressRune(r))
	}
	_ = m.Update(keyPress("esc")) // back to NORMAL so `s` is a binding

	_ = m.Update(keyPress("s"))

	assert.False(t, m.SendConfirmOpen(), "invalid headers must abort before the confirm modal opens")
	assert.Empty(t, svc.Sent())
	assert.Contains(t, m.View(), "no-equals-sign")
}

func TestCtrlU_NormalClearsAllFields(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})

	typeText(m, "key", "k1")
	typeText(m, "value", "v1")
	// clear-form is a NORMAL-only action: in INSERT, ctrl+u is the readline
	// kill-to-line-start handled by the field. Drop out to NORMAL first.
	_ = m.Update(keyPress("esc"))

	_ = m.Update(keyPress("ctrl+u"))

	for _, k := range []string{"key", "value"} {
		got, _ := m.Form().Field(k)
		assert.Empty(t, got.Value, "field %q should be cleared", k)
	}
	// the topic survives the clear because it lives on the model, not in
	// the form (it isn't editable from inside the produce screen).
	assert.Equal(t, "orders", m.Topic())
}

// Regression: ctrl+u in NORMAL must keep the partition options that were
// loaded asynchronously for the current topic. Previously clear() rebuilt
// the form from scratch and collapsed the picker back to {auto}, leaving
// the user no way to re-select a specific partition without switching
// topics and back.
func TestCtrlU_NormalPreservesLoadedPartitionOptions(t *testing.T) {
	svc := newFakeService()
	svc.setPartitions(0, 1, 2, 3)
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())

	typeText(m, "key", "k1")
	_ = m.Update(keyPress("esc"))
	_ = m.Update(keyPress("ctrl+u"))

	got, _ := m.Form().Field("partition")
	assert.Equal(t, []string{"auto", "0", "1", "2", "3"}, got.Options)
	assert.Equal(t, "auto", got.Value, "value falls back to the construction default")
}

// Regression: a pasted multi-line value used to render every line directly,
// blowing past the terminal height with no way to scroll. After the viewport
// integration the textarea body is bounded by the form's allotted height —
// no matter how long the value is, View() emits at most that many rows.
func TestTextarea_LongValueBoundedByFormHeight(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	m.SetSize(80, 24)
	m.Form().FocusKey("value")

	var value strings.Builder
	for range 100 {
		value.WriteString("line\n")
	}
	_ = m.Update(tea.PasteMsg{Content: value.String()})
	_ = m.Update(keyPress("esc"))

	out := m.View()
	lines := strings.Split(out, "\n")
	assert.LessOrEqual(t, len(lines), 24, "rendered output must fit the allotted terminal height")
}

// Regression: in INSERT, the cursor at the end of a long value used to be
// offscreen with no scroll. Now the viewport follows the cursor — and the
// early lines have correspondingly scrolled out of view, proving it's the
// viewport, not just an unbounded render.
func TestTextarea_CursorFollowKeepsLastLineVisible(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	m.SetSize(80, 24)
	m.Form().FocusKey("value")

	var value strings.Builder
	for i := range 80 {
		value.WriteString("FIRST_MARKER_")
		value.WriteString(strings.Repeat("x", i))
		value.WriteByte('\n')
	}
	value.WriteString("CURSOR_TARGET")
	_ = m.Update(tea.PasteMsg{Content: value.String()})
	require.Equal(t, produce.ModeNormal, m.Mode(), "paste must not cross into INSERT")
	// enter INSERT to edit: the cursor sits at the end of the pasted content,
	// so the bounded textarea viewport must follow it to the last line.
	_ = m.Update(keyPress("enter"))
	require.Equal(t, produce.ModeInsert, m.Mode())

	out := m.View()
	assert.Contains(t, out, "CURSOR_TARGET",
		"cursor's logical line must be in the visible window after entering INSERT")
	assert.NotContains(t, out, "FIRST_MARKER_xxxxx\n",
		"early lines must have scrolled out — otherwise the viewport isn't actually bounded")
}

// Regression: when focused on a long textarea in NORMAL, `j` must scroll the
// viewport down rather than be dropped. This is the read-only navigation
// affordance — the user can pan around a long value without entering INSERT.
// Regression: a Headers list with many rows used to render every row,
// crowding the textarea out of the screen. Now the list is bounded by a
// fraction of the form height; listCursor still scrolls into view.
func TestHeaders_LongListBoundedAndCursorFollows(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	m.SetSize(80, 24)
	m.Form().FocusKey("headers")

	var headers strings.Builder
	for i := range 40 {
		headers.WriteString("h")
		headers.WriteString(strings.Repeat("x", 1))
		headers.WriteByte(byte('a' + i%10))
		headers.WriteString("=v\n")
	}
	headers.WriteString("LAST_HEADER=v")
	_ = m.Update(tea.PasteMsg{Content: headers.String()})

	out := m.View()
	lines := strings.Split(out, "\n")
	assert.LessOrEqual(t, len(lines), 24, "headers list must not push the screen past terminal height")
	// paste lands listCursor on the final pasted row; cursor-follow must
	// bring it into the viewport's window.
	assert.Contains(t, out, "LAST_HEADER", "the row under listCursor must be visible after a long paste")
}

func TestTextarea_NormalModeScrollKeysPanViewport(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	m.SetSize(80, 20)
	m.Form().FocusKey("value")

	var value strings.Builder
	for range 60 {
		value.WriteString("FILLER\n")
	}
	value.WriteString("LAST_ROW")
	_ = m.Update(tea.PasteMsg{Content: value.String()})
	_ = m.Update(keyPress("esc")) // NORMAL on Value field

	// pre-condition: after esc the cursor went away (caret cleared) so the
	// viewport stays where it was last (scrolled to bottom from cursor-follow
	// during paste). Scroll up to top, then end must scroll back down.
	_ = m.Update(keyPress("home"))
	out := m.View()
	assert.NotContains(t, out, "LAST_ROW", "home in NORMAL must scroll to top first")

	_ = m.Update(keyPress("end"))
	out = m.View()
	assert.Contains(t, out, "LAST_ROW",
		"end in NORMAL on a textarea must scroll the viewport to the bottom")
}

func TestPaste_InNormalLandsValueAndStaysNormal(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	// focus the value field; mode stays NORMAL because we never pressed enter.
	m.Form().FocusKey("value")
	require.Equal(t, produce.ModeNormal, m.Mode())

	_ = m.Update(tea.PasteMsg{Content: "payload"})

	got, _ := m.Form().Field("value")
	assert.Equal(t, "payload", got.Value, "paste must land in the focused value field")
	assert.Equal(t, produce.ModeNormal, m.Mode(), "paste must not cross NORMAL into INSERT")
}

func TestPaste_OnSegmentedFieldIsIgnored(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	m.Form().FocusKey("compression")
	before, _ := m.Form().Field("compression")

	_ = m.Update(tea.PasteMsg{Content: "snappy"})

	got, _ := m.Form().Field("compression")
	assert.Equal(t, before.Value, got.Value, "paste must leave a segmented field untouched")
	assert.Equal(t, produce.ModeNormal, m.Mode(), "paste on non-text field must NOT enter INSERT")
}

func TestPaste_MultilineIntoValueTextareaKeepsNewlines(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	m.Form().FocusKey("value")

	_ = m.Update(tea.PasteMsg{Content: "line1\nline2"})

	got, _ := m.Form().Field("value")
	assert.Equal(t, "line1\nline2", got.Value)
}

func TestPaste_MultilineIntoKeyStripsNewlines(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	m.Form().FocusKey("key")

	_ = m.Update(tea.PasteMsg{Content: "a\nb"})

	got, _ := m.Form().Field("key")
	assert.Equal(t, "a b", got.Value, "single-line field must replace \\n with space")
}

func TestCtrlU_NoopWhileSegmentedPopupOpen(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	// seed a value the user would lose if the guard regressed.
	typeText(m, "key", "preserve-me")
	_ = m.Update(keyPress("esc"))
	// focus the compression segmented field and open its popup with enter.
	m.Form().FocusKey("compression")
	_ = m.Update(keyPress("enter"))
	require.True(t, m.Form().PopupActive(), "popup must be open for this assertion to be meaningful")

	_ = m.Update(keyPress("ctrl+u"))

	got, _ := m.Form().Field("key")
	assert.Equal(t, "preserve-me", got.Value, "ctrl+u must yield to an open popup, not wipe the form")
	assert.True(t, m.Form().PopupActive(), "popup must stay open")
}

func TestCtrlU_InsertKillsLineNotForm(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})

	typeText(m, "key", "abcdef")
	typeText(m, "value", "preserved")
	// move focus back to key. typeText left the form in INSERT on "value"
	// and FocusKey doesn't change the mode, so we're now editing "key".
	m.Form().FocusKey("key")
	require.Equal(t, produce.ModeInsert, m.Mode())

	// in INSERT, ctrl+u is kill-to-line-start. With the cursor at end of
	// "abcdef" the whole line dies — but the form-level "clear" must not
	// fire, so the other field's value is preserved.
	_ = m.Update(keyPress("ctrl+u"))

	gotKey, _ := m.Form().Field("key")
	gotVal, _ := m.Form().Field("value")
	assert.Empty(t, gotKey.Value)
	assert.Equal(t, "preserved", gotVal.Value, "value field must survive a kill-line on key")
}

func TestPrefillFromMessage_PopulatesFieldsAndResetsPartition(t *testing.T) {
	svc := newFakeService()
	src := &kafka.Message{
		Topic:     "orders-resend",
		Partition: 7,
		Offset:    42,
		Key:       []byte("key-1"),
		Value:     []byte(`{"id":1}`),
		Headers:   []kafka.Header{{Key: "h1", Value: []byte("v1")}},
	}
	m := produce.New(produce.Options{
		Service:            svc,
		Topic:              "orders",
		PrefillFromMessage: src,
	})

	get := func(k string) string {
		fld, _ := m.Form().Field(k)
		return fld.Value
	}
	assert.Equal(t, "orders-resend", m.Topic(), "topic switches to source on resend")
	assert.Equal(t, "auto", get("partition"), "partition resets to auto on resend")
	assert.Equal(t, "key-1", get("key"))
	assert.Equal(t, `{"id":1}`, get("value"))
	headers, _ := m.Form().Field("headers")
	assert.Equal(t, []string{"h1=v1"}, headers.List)
}

func TestE_OpensEditorAndAppliesEditedValue(t *testing.T) {
	svc := newFakeService()
	calls := 0
	pager := produce.PagerOpenerFunc(func(initial []byte) tea.Cmd {
		calls++
		// editor buffer carries the full record; here only Value is set.
		assert.Equal(t, "# Key\n\n# Headers\n\n# Value\nseed", string(initial))
		edited := "# Key\n\n# Headers\n\n# Value\nseed-edited"
		return func() tea.Msg { return produce.EditorEditedMsg{Content: []byte(edited)} }
	})
	m := produce.New(produce.Options{Service: svc, Topic: "orders", Pager: pager})
	typeText(m, "value", "seed")
	// typeText leaves the form in INSERT; back to NORMAL so `e` is a binding,
	// not a literal letter into the textarea.
	_ = m.Update(keyPress("esc"))

	// `e` returns an async Cmd that posts EditorEditedMsg — drive() runs
	// it so the result reaches handleEditorResult.
	cmd := m.Update(keyPress("e"))
	drive(t, m, cmd)

	assert.Equal(t, 1, calls)
	val, _ := m.Form().Field("value")
	assert.Equal(t, "seed-edited", val.Value)
}

func TestE_RoundtripsKeyHeadersAndValue(t *testing.T) {
	svc := newFakeService()
	pager := produce.PagerOpenerFunc(func(initial []byte) tea.Cmd {
		// buffer reflects the prefilled form state.
		want := "# Key\nk1\n\n# Headers\nh1=v1\nh2=v2\n\n# Value\nbody"
		assert.Equal(t, want, string(initial))
		edited := "# Key\nk2\n\n# Headers\nh3=v3\n\n# Value\nnew body"
		return func() tea.Msg { return produce.EditorEditedMsg{Content: []byte(edited)} }
	})
	m := produce.New(produce.Options{Service: svc, Topic: "orders", Pager: pager})
	typeText(m, "key", "k1")
	_ = m.Update(keyPress("esc"))
	typeText(m, "headers", "h1=v1")
	_ = m.Update(keyPress("enter"))
	for _, r := range "h2=v2" {
		_ = m.Update(keyPressRune(r))
	}
	_ = m.Update(keyPress("esc"))
	typeText(m, "value", "body")
	_ = m.Update(keyPress("esc"))

	cmd := m.Update(keyPress("e"))
	drive(t, m, cmd)

	key, _ := m.Form().Field("key")
	headers, _ := m.Form().Field("headers")
	val, _ := m.Form().Field("value")
	assert.Equal(t, "k2", key.Value)
	assert.Equal(t, []string{"h3=v3"}, headers.List)
	assert.Equal(t, "new body", val.Value)
}

func TestE_ParseErrorEmitsToastAndKeepsFormState(t *testing.T) {
	svc := newFakeService()
	pager := produce.PagerOpenerFunc(func(_ []byte) tea.Cmd {
		// missing '# Value' marker — parser rejects.
		bad := "# Key\nk\n\n# Headers\nh=v\n"
		return func() tea.Msg { return produce.EditorEditedMsg{Content: []byte(bad)} }
	})
	m := produce.New(produce.Options{Service: svc, Topic: "orders", Pager: pager})
	typeText(m, "key", "before")
	_ = m.Update(keyPress("esc"))
	typeText(m, "value", "before-value")
	_ = m.Update(keyPress("esc"))

	cmd := m.Update(keyPress("e"))
	drive(t, m, cmd)

	key, _ := m.Form().Field("key")
	val, _ := m.Form().Field("value")
	assert.Equal(t, "before", key.Value)
	assert.Equal(t, "before-value", val.Value)
	require.GreaterOrEqual(t, m.Toasts().Len(), 1)
	assert.Contains(t, m.Toasts().Items()[m.Toasts().Len()-1].Message, "parse failed")
}

func TestE_NoPagerEmitsWarning(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})

	cmd := m.Update(keyPress("e"))
	drive(t, m, cmd)

	require.GreaterOrEqual(t, m.Toasts().Len(), 1)
	assert.Contains(t, m.Toasts().Items()[m.Toasts().Len()-1].Message, "no $EDITOR opener configured")
}

func TestE_EditorErrorSurfacesToast(t *testing.T) {
	svc := newFakeService()
	pager := produce.PagerOpenerFunc(func(_ []byte) tea.Cmd {
		return func() tea.Msg { return produce.EditorEditedMsg{Err: errors.New("boom")} }
	})
	m := produce.New(produce.Options{Service: svc, Topic: "orders", Pager: pager})

	cmd := m.Update(keyPress("e"))
	drive(t, m, cmd)

	require.GreaterOrEqual(t, m.Toasts().Len(), 1)
	assert.Contains(t, m.Toasts().Items()[m.Toasts().Len()-1].Message, "boom")
}

func TestReadOnly_BlocksSend(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders", ReadOnly: true})

	_ = m.Update(keyPress("s"))

	assert.False(t, m.SendConfirmOpen(), "read-only must short-circuit before the confirm modal opens")
	assert.Empty(t, svc.Sent())
	require.GreaterOrEqual(t, m.Toasts().Len(), 1)
	assert.Contains(t, m.Toasts().Items()[m.Toasts().Len()-1].Message, "read-only")
}

func TestKeyHints_IncludeExpectedShortcuts(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})

	labels := []string{}
	for _, h := range m.KeyHints() {
		labels = append(labels, h.Label)
	}
	got := strings.Join(labels, ",")
	for _, want := range []string{"send", "$EDITOR", "clear", "cancel"} {
		assert.Contains(t, got, want)
	}
}

func TestCompressionDropdown_DefaultsToNone(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	cmp, _ := m.Form().Field("compression")
	assert.Equal(t, "none", cmp.Value)
}

func TestSend_PropagatesCompressionAndKey(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})

	m.Form().FocusKey("compression")
	_ = m.Update(keyPress("l"))
	typeText(m, "key", "k1")
	typeText(m, "value", "v1")

	drive(t, m, sendAndConfirm(m))

	require.Len(t, svc.Sent(), 1)
	assert.Equal(t, kafka.CompressionGzip, svc.Sent()[0].Compression)
	assert.Equal(t, []byte("k1"), svc.Sent()[0].Key)
	assert.Equal(t, []byte("v1"), svc.Sent()[0].Value)
}

func TestFullscreen_ShiftPlusToggles(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	assert.False(t, m.Fullscreen())

	_ = m.Update(keyPress("shift++"))
	assert.True(t, m.Fullscreen())

	// either key flips back (carousel)
	_ = m.Update(keyPress("shift+-"))
	assert.False(t, m.Fullscreen())

	_ = m.Update(keyPress("shift++"))
	_ = m.Update(keyPress("shift++"))
	assert.False(t, m.Fullscreen()) // two presses = back to A
}

// On terminals without the kitty keyboard protocol (Apple Terminal etc.)
// shift+plus delivers the rune '+' and shift+minus delivers '_' (because
// those are the shifted forms on US layouts). The screen must accept those
// literals as the same toggle.
func TestFullscreen_PlainPlusUnderscoreAlsoToggles(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})

	_ = m.Update(keyPress("+"))
	assert.True(t, m.Fullscreen())

	_ = m.Update(keyPress("_"))
	assert.False(t, m.Fullscreen())
}

func TestFullscreen_EscReturnsToSplitThenClosesForm(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	_ = m.Update(keyPress("shift++"))
	assert.True(t, m.Fullscreen())

	// first esc collapses fullscreen, no Back action
	_ = m.Update(keyPress("esc"))
	assert.False(t, m.Fullscreen())
	assert.False(t, m.ConsumeAction().Back)

	// second esc closes the form
	_ = m.Update(keyPress("esc"))
	assert.True(t, m.ConsumeAction().Back)
}

func TestFullscreen_TabCyclesThroughAllFields(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	_ = m.Update(keyPress("shift++"))

	// initial focus is field 0 (partition)
	assert.Equal(t, "partition", m.Form().FocusedField().Key)

	expected := []string{"compression", "key", "headers", "value", "partition"}
	for _, want := range expected {
		_ = m.Update(keyPress("tab"))
		assert.Equal(t, want, m.Form().FocusedField().Key)
	}
}

func TestFullscreen_ViewShowsTabStripWithActiveHighlighted(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	_ = m.Update(keyPress("shift++"))

	out := m.View()
	for _, label := range []string{"Partition", "Compression", "Key", "Headers", "Value"} {
		assert.Contains(t, out, label)
	}
	// active tab uses the bracketed form
	assert.Contains(t, out, "[ Partition ]")

	// after tab, compression becomes the active tab
	_ = m.Update(keyPress("tab"))
	out = m.View()
	assert.Contains(t, out, "[ Compression ]")
}

func TestFullscreen_CompressionRendersAsExpandedList(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	_ = m.Update(keyPress("shift++"))
	// move focus to compression
	for m.Form().FocusedField().Key != "compression" {
		_ = m.Update(keyPress("tab"))
	}
	out := m.View()
	// expanded list shows multiple radio markers and the compact slider chrome
	// is absent for the compression field.
	assert.Contains(t, out, "(•)")
	assert.NotContains(t, out, "◂ none ▸")
}

func TestFullscreen_LeavingModeBCollapsesCompressionPopup(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	_ = m.Update(keyPress("shift++"))
	// compression is forced into popup form; render confirms.
	for m.Form().FocusedField().Key != "compression" {
		_ = m.Update(keyPress("tab"))
	}
	assert.Contains(t, m.View(), "(•)")

	// leave fullscreen and the segmented field returns to compact slider
	_ = m.Update(keyPress("shift+-"))
	out := m.View()
	assert.Contains(t, out, "◂ none ▸")
}

// ----- mode tests -----

func TestMode_DefaultsToNormal(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	assert.Equal(t, produce.ModeNormal, m.Mode())
}

func TestMode_NormalLetterDoesNotInsert(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	m.Form().FocusKey("key")

	_ = m.Update(keyPressRune('a'))
	got, _ := m.Form().Field("key")
	assert.Empty(t, got.Value)
	assert.Equal(t, produce.ModeNormal, m.Mode())
}

func TestMode_EnterOnTextEntersInsertAndAcceptsTyping(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	m.Form().FocusKey("key")

	_ = m.Update(keyPress("enter"))
	assert.Equal(t, produce.ModeInsert, m.Mode())

	for _, r := range "abc" {
		_ = m.Update(keyPressRune(r))
	}
	got, _ := m.Form().Field("key")
	assert.Equal(t, "abc", got.Value)
}

func TestMode_EscReturnsToNormal(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	m.Form().FocusKey("key")
	_ = m.Update(keyPress("enter"))
	require.Equal(t, produce.ModeInsert, m.Mode())

	_ = m.Update(keyPress("esc"))
	assert.Equal(t, produce.ModeNormal, m.Mode())
	// esc in NORMAL with no fullscreen/popup must close the form
	assert.False(t, m.ConsumeAction().Back, "first esc out of INSERT must NOT close form")

	_ = m.Update(keyPress("esc"))
	assert.True(t, m.ConsumeAction().Back, "second esc closes the form")
}

func TestMode_TabInTextareaInsertsLiteral(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	m.Form().FocusKey("value")
	_ = m.Update(keyPress("enter")) // INSERT
	_ = m.Update(keyPress("tab"))

	got, _ := m.Form().Field("value")
	assert.Equal(t, "\t", got.Value)
	assert.Equal(t, produce.ModeInsert, m.Mode(), "tab in textarea must NOT leave INSERT")
}

func TestMode_TabInSingleLineCommitsAndNavigates(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	m.Form().FocusKey("key")
	_ = m.Update(keyPress("enter")) // INSERT
	for _, r := range "id" {
		_ = m.Update(keyPressRune(r))
	}
	_ = m.Update(keyPress("tab"))

	assert.Equal(t, produce.ModeNormal, m.Mode())
	assert.Equal(t, "headers", m.Form().FocusedField().Key)
	got, _ := m.Form().Field("key")
	assert.Equal(t, "id", got.Value)
}

func TestMode_EnterInTextareaInsertsNewline(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	m.Form().FocusKey("value")
	_ = m.Update(keyPress("enter")) // INSERT
	_ = m.Update(keyPressRune('a'))
	_ = m.Update(keyPress("enter")) // newline, NOT exit
	_ = m.Update(keyPressRune('b'))

	got, _ := m.Form().Field("value")
	assert.Equal(t, "a\nb", got.Value)
	assert.Equal(t, produce.ModeInsert, m.Mode())
}

func TestMode_EnterInSingleLineExitsToNormal(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	m.Form().FocusKey("key")
	_ = m.Update(keyPress("enter")) // INSERT
	for _, r := range "abc" {
		_ = m.Update(keyPressRune(r))
	}
	_ = m.Update(keyPress("enter"))

	assert.Equal(t, produce.ModeNormal, m.Mode())
	assert.Equal(t, "key", m.Form().FocusedField().Key, "stays on same field")
	got, _ := m.Form().Field("key")
	assert.Equal(t, "abc", got.Value)
}

func TestHeadersInsert_InvalidRowShowsInlineMarker(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	m.Form().FocusKey("headers")
	_ = m.Update(keyPress("enter")) // INSERT, empty row
	for _, r := range "no-equals" {
		_ = m.Update(keyPressRune(r))
	}
	out := m.View()
	assert.Contains(t, out, "must be key=value", "invalid row must surface its reason inline")
}

func TestHeadersInsert_EmptyKeyIsInvalid(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	m.Form().FocusKey("headers")
	_ = m.Update(keyPress("enter"))
	for _, r := range "=value-only" {
		_ = m.Update(keyPressRune(r))
	}
	assert.Contains(t, m.View(), "key is empty")
}

func TestHeadersInsert_ValidRowHasNoMarker(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	m.Form().FocusKey("headers")
	_ = m.Update(keyPress("enter"))
	for _, r := range "x-trace=abc" {
		_ = m.Update(keyPressRune(r))
	}
	out := m.View()
	assert.NotContains(t, out, "must be key=value")
	assert.NotContains(t, out, "key is empty")
}

func TestHeadersInsert_EnterOnInvalidRowIsBlocked(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	m.Form().FocusKey("headers")
	_ = m.Update(keyPress("enter")) // INSERT, empty row
	for _, r := range "no-equals" {
		_ = m.Update(keyPressRune(r))
	}
	// chain-Enter on invalid row: must NOT add a new row, must surface a toast.
	_ = m.Update(keyPress("enter"))

	got, _ := m.Form().Field("headers")
	assert.Equal(t, []string{"no-equals"}, got.List, "no new row added while current is invalid")
	assert.Equal(t, produce.ModeInsert, m.Mode(), "stay in INSERT to fix the row")
	require.GreaterOrEqual(t, m.Toasts().Len(), 1)
	assert.Contains(t, m.Toasts().Items()[m.Toasts().Len()-1].Message, "header invalid", "toast surfaces the reason")
}

func TestHeadersInsert_EnterChainsAddingRows(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	m.Form().FocusKey("headers")

	// open headers edit: NORMAL → enter → empty row added, INSERT
	_ = m.Update(keyPress("enter"))
	require.Equal(t, produce.ModeInsert, m.Mode())

	// fill first header
	for _, r := range "x-trace=abc" {
		_ = m.Update(keyPressRune(r))
	}
	// enter on a non-empty row: commit + add new empty + stay in INSERT
	_ = m.Update(keyPress("enter"))
	got, _ := m.Form().Field("headers")
	assert.Equal(t, []string{"x-trace=abc", ""}, got.List)
	assert.Equal(t, produce.ModeInsert, m.Mode())
	assert.Equal(t, 0, m.Form().ListEntryCursor("headers"))

	// fill second
	for _, r := range "x-source=ui" {
		_ = m.Update(keyPressRune(r))
	}
	_ = m.Update(keyPress("enter"))
	got, _ = m.Form().Field("headers")
	assert.Equal(t, []string{"x-trace=abc", "x-source=ui", ""}, got.List)
	assert.Equal(t, produce.ModeInsert, m.Mode())

	// enter on the now-empty trailing row finishes the add-many loop
	_ = m.Update(keyPress("enter"))
	assert.Equal(t, produce.ModeNormal, m.Mode())
}

func TestHeadersInsert_CtrlNAddsRowAtEnd(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	m.Form().SetList("headers", []string{"a=1", "b=2"})
	m.Form().FocusKey("headers")
	// open INSERT on the first row
	_ = m.Update(keyPress("enter"))
	require.Equal(t, produce.ModeInsert, m.Mode())

	_ = m.Update(keyPress("ctrl+n"))
	got, _ := m.Form().Field("headers")
	assert.Equal(t, []string{"a=1", "b=2", ""}, got.List)
	assert.Equal(t, produce.ModeInsert, m.Mode())
	assert.Equal(t, 0, m.Form().ListEntryCursor("headers"))
}

func TestHeadersInsert_CtrlXDeletesFocusedRow(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	m.Form().SetList("headers", []string{"a=1", "b=2"})
	m.Form().FocusKey("headers")
	_ = m.Update(keyPress("enter")) // INSERT on row 0 ("a=1")

	_ = m.Update(keyPress("ctrl+x"))
	got, _ := m.Form().Field("headers")
	assert.Equal(t, []string{"b=2"}, got.List)
	assert.Equal(t, produce.ModeInsert, m.Mode(), "stays in INSERT while list is non-empty")
}

func TestHeadersInsert_CtrlXOnLastRowReseedsEmptyAndStaysInsert(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	m.Form().SetList("headers", []string{"only=row"})
	m.Form().FocusKey("headers")
	_ = m.Update(keyPress("enter"))

	_ = m.Update(keyPress("ctrl+x"))
	got, _ := m.Form().Field("headers")
	// list is re-seeded with an empty row so the user can keep typing —
	// only an explicit Enter on an empty row exits INSERT.
	assert.Equal(t, []string{""}, got.List)
	assert.Equal(t, produce.ModeInsert, m.Mode())
}

func TestHeadersInsert_BackspaceEmptyingLastRowKeepsInsert(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	m.Form().SetList("headers", []string{"x=1"})
	m.Form().FocusKey("headers")
	_ = m.Update(keyPress("enter")) // INSERT on the only row

	// erase the row character by character; the last backspace removes the
	// now-empty row and would normally leave the list at zero — but the
	// invariant re-seeds an empty row so we stay in INSERT.
	for range "x=1" {
		_ = m.Update(keyPress("backspace"))
	}
	_ = m.Update(keyPress("backspace"))

	got, _ := m.Form().Field("headers")
	assert.Equal(t, []string{""}, got.List)
	assert.Equal(t, produce.ModeInsert, m.Mode())
}

func TestHeadersInsert_PlusUnderscoreAreLiterals(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	m.Form().FocusKey("headers")
	_ = m.Update(keyPress("enter")) // INSERT, empty row
	for _, r := range "x_user=a+b" {
		_ = m.Update(keyPressRune(r))
	}
	got, _ := m.Form().Field("headers")
	assert.Equal(t, []string{"x_user=a+b"}, got.List, "+ and _ must be insertable in header values")
}

func TestHeadersInsert_CtrlNAddsRowAfterTyping(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	m.Form().FocusKey("headers")
	_ = m.Update(keyPress("enter")) // INSERT, empty row

	for _, r := range "k=v" {
		_ = m.Update(keyPressRune(r))
	}
	_ = m.Update(keyPress("ctrl+n"))
	got, _ := m.Form().Field("headers")
	assert.Equal(t, []string{"k=v", ""}, got.List)
}

func TestHeadersNormal_EnterOnEmptyListAddsRowThenInsert(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	m.Form().FocusKey("headers")

	_ = m.Update(keyPress("enter"))
	got, _ := m.Form().Field("headers")
	assert.Equal(t, []string{""}, got.List)
	assert.Equal(t, produce.ModeInsert, m.Mode())
}

func TestNormal_PlusToggleStillFiresInNormal(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	_ = m.Update(keyPressRune('+'))
	assert.True(t, m.Fullscreen())
}

func TestInsert_PlusIsLiteralInTextarea(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	m.Form().FocusKey("value")
	_ = m.Update(keyPress("enter")) // INSERT
	_ = m.Update(keyPressRune('+'))

	got, _ := m.Form().Field("value")
	assert.Equal(t, "+", got.Value)
	assert.False(t, m.Fullscreen())
}

func TestEditSuffix_ShownNextToFocusedFieldInInsert(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})

	// in NORMAL there is no [EDIT] tag anywhere
	out := m.View()
	assert.NotContains(t, out, "[EDIT]")

	// entering INSERT on Key surfaces [EDIT] next to that field's label
	m.Form().FocusKey("key")
	_ = m.Update(keyPress("enter"))
	out = m.View()
	assert.Contains(t, out, "[EDIT]")
	keyLineHasTag := false
	for line := range strings.SplitSeq(out, "\n") {
		if strings.Contains(line, "Key") && strings.Contains(line, "[EDIT]") {
			keyLineHasTag = true
			break
		}
	}
	assert.True(t, keyLineHasTag, "[EDIT] tag must sit on the same line as the focused field's label")

	// leaving INSERT removes the tag
	_ = m.Update(keyPress("esc"))
	out = m.View()
	assert.NotContains(t, out, "[EDIT]")
}

// ----- helpers -----

// typeText focuses the named field, ensures the screen is in INSERT mode,
// and types each rune. Multi-line strings only make sense on a textarea.
func typeText(m *produce.Model, field, text string) {
	m.Form().FocusKey(field)
	if m.Mode() != produce.ModeInsert {
		_ = m.Update(keyPress("enter"))
	}
	for _, r := range text {
		_ = m.Update(keyPressRune(r))
	}
}

// sendAndConfirm drives a "send & close" through the confirm modal:
// drops to NORMAL if needed, opens the modal with `s`, and answers
// `y`. The Cmd from the confirm answer is returned so callers can
// drive() it. Returns nil if the modal failed to open (validation
// rejected the spec, read-only, etc).
func sendAndConfirm(m *produce.Model) tea.Cmd {
	if m.Mode() == produce.ModeInsert {
		_ = m.Update(keyPress("esc"))
	}
	_ = m.Update(keyPress("s"))
	if !m.SendConfirmOpen() {
		return nil
	}
	return m.Update(keyPress("y"))
}

// sendAndKeep mirrors [sendAndConfirm] for the "send & keep open"
// variant — answers `k` instead of `y`.
func sendAndKeep(m *produce.Model) tea.Cmd {
	if m.Mode() == produce.ModeInsert {
		_ = m.Update(keyPress("esc"))
	}
	_ = m.Update(keyPress("s"))
	if !m.SendConfirmOpen() {
		return nil
	}
	return m.Update(keyPress("k"))
}

type fakeService struct {
	mu         sync.Mutex
	sent       []kafka.ProduceSpec
	result     kafka.ProduceResult
	err        error
	partitions []int32
	partErr    error
}

func newFakeService() *fakeService {
	// keep a non-empty default so tests that drive Init() get a deterministic
	// minimal partition list ([auto, 0]); tests that want a different count
	// overwrite via setPartitions. Tests that never call Init() see only the
	// initial form options ([auto]) regardless of this default.
	return &fakeService{partitions: []int32{0}}
}

func (f *fakeService) setPartitions(ids ...int32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.partitions = append([]int32(nil), ids...)
}

func (f *fakeService) TopicPartitions(_ context.Context, _ string) ([]kafka.PartitionDetail, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.partErr != nil {
		return nil, f.partErr
	}
	out := make([]kafka.PartitionDetail, len(f.partitions))
	for i, id := range f.partitions {
		out[i] = kafka.PartitionDetail{Partition: id}
	}
	return out, nil
}

func (f *fakeService) Produce(_ context.Context, spec kafka.ProduceSpec) (kafka.ProduceResult, error) {
	f.mu.Lock()
	f.sent = append(f.sent, spec)
	res, err := f.result, f.err
	f.mu.Unlock()
	if err != nil {
		return kafka.ProduceResult{}, err
	}
	if res.Topic == "" {
		res.Topic = spec.Topic
		res.Partition = spec.Partition
	}
	return res, nil
}

func (f *fakeService) Sent() []kafka.ProduceSpec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]kafka.ProduceSpec(nil), f.sent...)
}

var keyPressTable = map[string]tea.KeyPressMsg{
	"enter":     {Code: tea.KeyEnter},
	"esc":       {Code: tea.KeyEscape},
	"backspace": {Code: tea.KeyBackspace},
	"tab":       {Code: tea.KeyTab},
	"shift+tab": {Code: tea.KeyTab, Mod: tea.ModShift},
	"down":      {Code: tea.KeyDown},
	"up":        {Code: tea.KeyUp},
	"left":      {Code: tea.KeyLeft},
	"right":     {Code: tea.KeyRight},
	"home":      {Code: tea.KeyHome},
	"end":       {Code: tea.KeyEnd},
	"ctrl+a":    {Code: 'a', Mod: tea.ModCtrl},
	"ctrl+e":    {Code: 'e', Mod: tea.ModCtrl},
	"ctrl+k":    {Code: 'k', Mod: tea.ModCtrl},
	"ctrl+n":    {Code: 'n', Mod: tea.ModCtrl},
	"ctrl+u":    {Code: 'u', Mod: tea.ModCtrl},
	"ctrl+w":    {Code: 'w', Mod: tea.ModCtrl},
	"ctrl+x":    {Code: 'x', Mod: tea.ModCtrl},
	"shift++":   {Code: '+', Mod: tea.ModShift},
	"shift+-":   {Code: '-', Mod: tea.ModShift},
}

func keyPress(name string) tea.KeyPressMsg {
	if msg, ok := keyPressTable[name]; ok {
		return msg
	}
	if len(name) == 1 {
		r := rune(name[0])
		return tea.KeyPressMsg{Code: r, Text: string(r)}
	}
	return tea.KeyPressMsg{}
}

func keyPressRune(r rune) tea.KeyPressMsg {
	return tea.KeyPressMsg{Code: r, Text: string(r)}
}
