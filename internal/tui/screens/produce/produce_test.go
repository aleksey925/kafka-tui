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
	assert.Equal(t, "Produce → orders", m.Title())
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

func TestCtrlS_SendsAndClosesOnSuccess(t *testing.T) {
	// arrange
	svc := newFakeService()
	svc.result = kafka.ProduceResult{Topic: "orders", Partition: 2, Offset: 99, Duration: 12 * time.Millisecond}
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	typeText(m, "value", "hello")

	// act
	cmd := m.Update(keyPress("ctrl+s"))
	drive(t, m, cmd)

	// assert
	require.Len(t, svc.Sent(), 1)
	assert.Equal(t, "orders", svc.Sent()[0].Topic)
	assert.Equal(t, []byte("hello"), svc.Sent()[0].Value)

	a := m.ConsumeAction()
	assert.True(t, a.Back, "ctrl+s must close after a successful send")
	require.NotNil(t, a.Sent)
	assert.Equal(t, int64(99), a.Sent.Offset)
}

func TestCtrlShiftS_SendsButKeepsForm(t *testing.T) {
	svc := newFakeService()
	svc.result = kafka.ProduceResult{Topic: "orders", Partition: 0, Offset: 1, Duration: time.Millisecond}
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	typeText(m, "value", "x")

	cmd := m.Update(keyPress("ctrl+shift+s"))
	drive(t, m, cmd)

	require.Len(t, svc.Sent(), 1)
	a := m.ConsumeAction()
	assert.False(t, a.Back, "ctrl+shift+s must NOT close")
	require.NotNil(t, a.Sent)
}

func TestSend_ToastIncludesPartitionOffsetAndLatency(t *testing.T) {
	svc := newFakeService()
	svc.result = kafka.ProduceResult{Topic: "orders", Partition: 4, Offset: 7, Duration: 42 * time.Millisecond}
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	typeText(m, "value", "x")

	cmd := m.Update(keyPress("ctrl+s"))
	drive(t, m, cmd)

	require.GreaterOrEqual(t, m.Toasts().Len(), 1)
	assert.Contains(t, m.Toasts().Items()[m.Toasts().Len()-1].Message, "Sent to orders P4:7 (42ms)")
}

func TestSend_FailureSurfacesErrorToast(t *testing.T) {
	svc := newFakeService()
	svc.err = errors.New("broker unavailable")
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	typeText(m, "value", "x")

	cmd := m.Update(keyPress("ctrl+s"))
	drive(t, m, cmd)

	a := m.ConsumeAction()
	assert.False(t, a.Back, "failed send must not close the form")
	assert.Nil(t, a.Sent)
	assert.Contains(t, m.View(), "broker unavailable")
}

func TestSend_ValidationErrorOnEmptyTopic(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: ""})

	cmd := m.Update(keyPress("ctrl+s"))
	drive(t, m, cmd)

	assert.Empty(t, svc.Sent(), "validation error must abort before calling Service")
	assert.Contains(t, m.View(), "topic is required")
}

func TestPartition_AutoEqualsKafkaAuto(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})

	cmd := m.Update(keyPress("ctrl+s"))
	drive(t, m, cmd)

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

	cmd := m.Update(keyPress("ctrl+s"))
	drive(t, m, cmd)

	require.Len(t, svc.Sent(), 1)
	assert.Equal(t, int32(3), svc.Sent()[0].Partition)
}

// Regression: when the user toggles between two topics via history faster
// than the metadata fetch can complete, the picker must not get stuck on
// `[auto]` for the original topic just because its prior load result is
// still tracked as the "current" topic on the model.
func TestPartition_RapidTopicTogglePreservesOptionsAfterReturn(t *testing.T) {
	svc := newFakeService()
	svc.setPartitions(0, 1, 2)
	hist := newFakeHistory()
	// histBuf[0] is newest; ctrl+p walks toward older entries.
	hist.entries = []produce.Entry{
		{Topic: "topic-a", Partition: 0, Compression: kafka.CompressionNone},
		{Topic: "topic-b", Partition: 0, Compression: kafka.CompressionNone},
	}
	m := produce.New(produce.Options{Service: svc, Topic: "topic-a", History: hist})
	drive(t, m, m.Init())

	// ctrl+p onto the topic-a entry — same topic, no reload expected.
	_ = m.Update(keyPress("ctrl+p"))

	// ctrl+p onto the topic-b entry — options are wiped and a load for
	// topic-b is emitted. Intentionally do NOT drive that cmd: we want to
	// model the case where the user moves away before topic-b's metadata
	// arrives.
	_ = m.Update(keyPress("ctrl+p"))

	// ctrl+n back to the topic-a entry. The picker must re-issue a fetch
	// for topic-a (the prior wipe to [auto] cleared its options).
	cmd := m.Update(keyPress("ctrl+n"))
	require.NotNil(t, cmd, "ctrl+n back to topic-a must re-fetch its partitions")
	drive(t, m, cmd)

	got, ok := m.Form().Field("partition")
	require.True(t, ok)
	assert.Equal(t, []string{"auto", "0", "1", "2"}, got.Options)
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

	cmd := m.Update(keyPress("ctrl+s"))
	drive(t, m, cmd)

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

	cmd := m.Update(keyPress("ctrl+s"))
	drive(t, m, cmd)

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

func TestHistoryPrefill_TopOpensWithLastEntry(t *testing.T) {
	svc := newFakeService()
	hist := newFakeHistory()
	hist.Add(produce.Entry{
		Topic: "orders", Key: []byte("k-old"), Value: []byte("v-old"),
		Compression: kafka.CompressionGzip, Partition: kafka.PartitionAuto,
	})
	m := produce.New(produce.Options{Service: svc, Topic: "orders", History: hist})

	val, _ := m.Form().Field("value")
	assert.Equal(t, "v-old", val.Value)
	cmp, _ := m.Form().Field("compression")
	assert.Equal(t, "gzip", cmp.Value)
}

func TestCtrlP_StepsThroughHistory(t *testing.T) {
	svc := newFakeService()
	hist := newFakeHistory()
	hist.Add(produce.Entry{Topic: "orders", Value: []byte("oldest"), Partition: kafka.PartitionAuto})
	hist.Add(produce.Entry{Topic: "orders", Value: []byte("middle"), Partition: kafka.PartitionAuto})
	hist.Add(produce.Entry{Topic: "orders", Value: []byte("newest"), Partition: kafka.PartitionAuto})

	m := produce.New(produce.Options{Service: svc, Topic: "orders", History: hist, HistorySize: 5})

	val, _ := m.Form().Field("value")
	assert.Equal(t, "newest", val.Value, "fresh open prefills with newest entry for topic")

	// ctrl+p walks back into older entries.
	_ = m.Update(keyPress("ctrl+p"))
	val, _ = m.Form().Field("value")
	assert.Equal(t, "newest", val.Value, "first ctrl+p selects pos 0 (newest)")

	_ = m.Update(keyPress("ctrl+p"))
	val, _ = m.Form().Field("value")
	assert.Equal(t, "middle", val.Value)

	_ = m.Update(keyPress("ctrl+p"))
	val, _ = m.Form().Field("value")
	assert.Equal(t, "oldest", val.Value)

	// ctrl+n steps forward.
	_ = m.Update(keyPress("ctrl+n"))
	val, _ = m.Form().Field("value")
	assert.Equal(t, "middle", val.Value)
}

func TestCtrlN_PastNewestResetsForm(t *testing.T) {
	svc := newFakeService()
	hist := newFakeHistory()
	hist.Add(produce.Entry{Topic: "orders", Value: []byte("only"), Partition: kafka.PartitionAuto})
	m := produce.New(produce.Options{Service: svc, Topic: "orders", History: hist})

	_ = m.Update(keyPress("ctrl+p")) // pos 0 (newest)
	_ = m.Update(keyPress("ctrl+n")) // pos -1: empty form

	val, _ := m.Form().Field("value")
	assert.Empty(t, val.Value)
}

func TestHistory_AddedAfterSuccessfulSend(t *testing.T) {
	svc := newFakeService()
	svc.result = kafka.ProduceResult{Topic: "orders", Partition: 0, Offset: 1}
	hist := newFakeHistory()
	m := produce.New(produce.Options{
		Service: svc, Topic: "orders", History: hist, Cluster: "stage",
	})
	typeText(m, "value", "data")

	cmd := m.Update(keyPress("ctrl+s"))
	drive(t, m, cmd)

	added := hist.All()
	require.Len(t, added, 1)
	assert.Equal(t, "stage", added[0].Cluster)
	assert.Equal(t, "orders", added[0].Topic)
	assert.Equal(t, []byte("data"), added[0].Value)
}

func TestCtrlO_OpensEditorAndAppliesEditedValue(t *testing.T) {
	svc := newFakeService()
	calls := 0
	pager := produce.PagerOpenerFunc(func(initial []byte) ([]byte, error) {
		calls++
		assert.Equal(t, []byte("seed"), initial)
		return []byte("seed-edited"), nil
	})
	m := produce.New(produce.Options{Service: svc, Topic: "orders", Pager: pager})
	typeText(m, "value", "seed")

	_ = m.Update(keyPress("ctrl+o"))

	assert.Equal(t, 1, calls)
	val, _ := m.Form().Field("value")
	assert.Equal(t, "seed-edited", val.Value)
}

func TestCtrlO_NoPagerEmitsWarning(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})

	_ = m.Update(keyPress("ctrl+o"))

	require.GreaterOrEqual(t, m.Toasts().Len(), 1)
	assert.Contains(t, m.Toasts().Items()[m.Toasts().Len()-1].Message, "no $EDITOR opener configured")
}

func TestCtrlO_EditorErrorSurfacesToast(t *testing.T) {
	svc := newFakeService()
	pager := produce.PagerOpenerFunc(func(_ []byte) ([]byte, error) { return nil, errors.New("boom") })
	m := produce.New(produce.Options{Service: svc, Topic: "orders", Pager: pager})

	_ = m.Update(keyPress("ctrl+o"))

	require.GreaterOrEqual(t, m.Toasts().Len(), 1)
	assert.Contains(t, m.Toasts().Items()[m.Toasts().Len()-1].Message, "boom")
}

func TestReadOnly_BlocksSend(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders", ReadOnly: true})

	cmd := m.Update(keyPress("ctrl+s"))
	drive(t, m, cmd)

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
	for _, want := range []string{"send", "send & keep", "$EDITOR", "history", "clear", "cancel"} {
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
	_ = m.Update(keyPress("j")) // none → gzip
	typeText(m, "key", "k1")
	typeText(m, "value", "v1")

	cmd := m.Update(keyPress("ctrl+s"))
	drive(t, m, cmd)

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

func TestHeadersInsert_CtrlNOverridesGlobalHistory(t *testing.T) {
	// On non-list fields ctrl+n is "history step (newer)". On Headers in
	// INSERT it must be intercepted as "add row" before the global handler.
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})
	m.Form().FocusKey("headers")
	_ = m.Update(keyPress("enter")) // INSERT, empty row

	// type something so the row is non-empty, then ctrl+n must add a new
	// row (not jump to history).
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

func TestEditSuffix_PreservedAcrossFormRebuilds(t *testing.T) {
	hist := newFakeHistory()
	hist.Add(produce.Entry{Topic: "orders", Value: []byte("v1")})
	m := produce.New(produce.Options{
		Service: newFakeService(), Topic: "orders", History: hist,
	})
	m.Form().FocusKey("key")
	_ = m.Update(keyPress("enter"))
	require.Equal(t, produce.ModeInsert, m.Mode())

	// ctrl+n past the newest history slot rebuilds the form; mode must
	// stay INSERT and [EDIT] must remain.
	_ = m.Update(keyPress("ctrl+n"))
	assert.Equal(t, produce.ModeInsert, m.Mode())
	assert.Contains(t, m.View(), "[EDIT]")
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

type fakeHistory struct {
	mu      sync.Mutex
	entries []produce.Entry
}

func newFakeHistory() *fakeHistory { return &fakeHistory{} }

func (h *fakeHistory) Add(e produce.Entry) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.entries = append(h.entries, e)
}

func (h *fakeHistory) All() []produce.Entry {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]produce.Entry(nil), h.entries...)
}

func (h *fakeHistory) LastForTopic(topic string) (produce.Entry, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := len(h.entries) - 1; i >= 0; i-- {
		if h.entries[i].Topic == topic {
			return h.entries[i], true
		}
	}
	return produce.Entry{}, false
}

func (h *fakeHistory) Recent(n int) []produce.Entry {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]produce.Entry, 0, n)
	for i := len(h.entries) - 1; i >= 0 && len(out) < n; i-- {
		out = append(out, h.entries[i])
	}
	return out
}

var keyPressTable = map[string]tea.KeyPressMsg{
	"enter":        {Code: tea.KeyEnter},
	"esc":          {Code: tea.KeyEscape},
	"backspace":    {Code: tea.KeyBackspace},
	"tab":          {Code: tea.KeyTab},
	"shift+tab":    {Code: tea.KeyTab, Mod: tea.ModShift},
	"down":         {Code: tea.KeyDown},
	"up":           {Code: tea.KeyUp},
	"left":         {Code: tea.KeyLeft},
	"right":        {Code: tea.KeyRight},
	"ctrl+a":       {Code: 'a', Mod: tea.ModCtrl},
	"ctrl+e":       {Code: 'e', Mod: tea.ModCtrl},
	"ctrl+k":       {Code: 'k', Mod: tea.ModCtrl},
	"ctrl+n":       {Code: 'n', Mod: tea.ModCtrl},
	"ctrl+o":       {Code: 'o', Mod: tea.ModCtrl},
	"ctrl+p":       {Code: 'p', Mod: tea.ModCtrl},
	"ctrl+s":       {Code: 's', Mod: tea.ModCtrl},
	"ctrl+shift+s": {Code: 's', Mod: tea.ModCtrl | tea.ModShift},
	"ctrl+u":       {Code: 'u', Mod: tea.ModCtrl},
	"ctrl+w":       {Code: 'w', Mod: tea.ModCtrl},
	"ctrl+x":       {Code: 'x', Mod: tea.ModCtrl},
	"shift++":      {Code: '+', Mod: tea.ModShift},
	"shift+-":      {Code: '-', Mod: tea.ModShift},
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
