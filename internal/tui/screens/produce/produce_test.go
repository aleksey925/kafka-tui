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
		_, follow := m.Update(msg)
		queue = append(queue, follow)
	}
}

func TestNew_RendersHeaderAndFields(t *testing.T) {
	// arrange
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})

	// act
	out := m.View()

	// assert
	for _, want := range []string{"Produce → orders", "Topic", "Partition", "Compression", "Key", "Headers", "Value"} {
		assert.Contains(t, out, want)
	}
}

func TestNew_TopicPrefilledFromOption(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})

	got, _ := m.Form().Field("topic")
	assert.Equal(t, "orders", got.Value)
}

func TestEsc_RaisesBackAction(t *testing.T) {
	m := produce.New(produce.Options{Service: newFakeService(), Topic: "orders"})

	_, _ = m.Update(keyPress("esc"))

	assert.True(t, m.ConsumeAction().Back)
}

func TestCtrlS_SendsAndClosesOnSuccess(t *testing.T) {
	// arrange
	svc := newFakeService()
	svc.result = kafka.ProduceResult{Topic: "orders", Partition: 2, Offset: 99, Duration: 12 * time.Millisecond}
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	typeText(m, "value", "hello")

	// act
	_, cmd := m.Update(keyPress("ctrl+s"))
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

	_, cmd := m.Update(keyPress("ctrl+shift+s"))
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

	_, cmd := m.Update(keyPress("ctrl+s"))
	drive(t, m, cmd)

	out := m.View()
	assert.Contains(t, out, "Sent to orders P4:7 (42ms)")
}

func TestSend_FailureSurfacesErrorToast(t *testing.T) {
	svc := newFakeService()
	svc.err = errors.New("broker unavailable")
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	typeText(m, "value", "x")

	_, cmd := m.Update(keyPress("ctrl+s"))
	drive(t, m, cmd)

	a := m.ConsumeAction()
	assert.False(t, a.Back, "failed send must not close the form")
	assert.Nil(t, a.Sent)
	assert.Contains(t, m.View(), "broker unavailable")
}

func TestSend_ValidationErrorOnEmptyTopic(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: ""})

	_, cmd := m.Update(keyPress("ctrl+s"))
	drive(t, m, cmd)

	assert.Empty(t, svc.Sent(), "validation error must abort before calling Service")
	assert.Contains(t, m.View(), "topic is required")
}

func TestSend_ValidationErrorOnInvalidPartition(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})
	m.Form().FocusKey("partition")
	// replace "auto" with garbage by clearing then typing
	for range "auto" {
		_, _ = m.Update(keyPress("backspace"))
	}
	typeText(m, "partition", "abc")

	_, cmd := m.Update(keyPress("ctrl+s"))
	drive(t, m, cmd)

	assert.Empty(t, svc.Sent())
	assert.Contains(t, m.View(), "partition")
}

func TestPartition_AutoEqualsKafkaAuto(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})

	_, cmd := m.Update(keyPress("ctrl+s"))
	drive(t, m, cmd)

	require.Len(t, svc.Sent(), 1)
	assert.Equal(t, kafka.PartitionAuto, svc.Sent()[0].Partition)
}

func TestPartition_ManualNumberPropagated(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})

	m.Form().FocusKey("partition")
	for range "auto" {
		_, _ = m.Update(keyPress("backspace"))
	}
	typeText(m, "partition", "3")

	_, cmd := m.Update(keyPress("ctrl+s"))
	drive(t, m, cmd)

	require.Len(t, svc.Sent(), 1)
	assert.Equal(t, int32(3), svc.Sent()[0].Partition)
}

func TestHeaders_ParsedAsKeyEquals(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})

	m.Form().FocusKey("headers")
	_, _ = m.Update(keyPress("ctrl+a"))
	typeText(m, "headers", "x-trace=abc")
	_, _ = m.Update(keyPress("ctrl+a"))
	typeText(m, "headers", "x-source=ui")

	_, cmd := m.Update(keyPress("ctrl+s"))
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
	_, _ = m.Update(keyPress("ctrl+a"))
	typeText(m, "headers", "no-equals-sign")

	_, cmd := m.Update(keyPress("ctrl+s"))
	drive(t, m, cmd)

	assert.Empty(t, svc.Sent())
	assert.Contains(t, m.View(), "no-equals-sign")
}

func TestCtrlR_ClearsAllFields(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})

	typeText(m, "key", "k1")
	typeText(m, "value", "v1")

	_, _ = m.Update(keyPress("ctrl+r"))

	for _, k := range []string{"key", "value"} {
		got, _ := m.Form().Field(k)
		assert.Empty(t, got.Value, "field %q should be cleared", k)
	}
	// the topic survives the clear because it's part of the canonical default.
	got, _ := m.Form().Field("topic")
	assert.Equal(t, "orders", got.Value)
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
	assert.Equal(t, "orders-resend", get("topic"), "topic switches to source on resend")
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
	_, _ = m.Update(keyPress("ctrl+p"))
	val, _ = m.Form().Field("value")
	assert.Equal(t, "newest", val.Value, "first ctrl+p selects pos 0 (newest)")

	_, _ = m.Update(keyPress("ctrl+p"))
	val, _ = m.Form().Field("value")
	assert.Equal(t, "middle", val.Value)

	_, _ = m.Update(keyPress("ctrl+p"))
	val, _ = m.Form().Field("value")
	assert.Equal(t, "oldest", val.Value)

	// ctrl+n steps forward.
	_, _ = m.Update(keyPress("ctrl+n"))
	val, _ = m.Form().Field("value")
	assert.Equal(t, "middle", val.Value)
}

func TestCtrlN_PastNewestResetsForm(t *testing.T) {
	svc := newFakeService()
	hist := newFakeHistory()
	hist.Add(produce.Entry{Topic: "orders", Value: []byte("only"), Partition: kafka.PartitionAuto})
	m := produce.New(produce.Options{Service: svc, Topic: "orders", History: hist})

	_, _ = m.Update(keyPress("ctrl+p")) // pos 0 (newest)
	_, _ = m.Update(keyPress("ctrl+n")) // pos -1: empty form

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

	_, cmd := m.Update(keyPress("ctrl+s"))
	drive(t, m, cmd)

	added := hist.All()
	require.Len(t, added, 1)
	assert.Equal(t, "stage", added[0].Cluster)
	assert.Equal(t, "orders", added[0].Topic)
	assert.Equal(t, []byte("data"), added[0].Value)
}

func TestCtrlE_OpensEditorAndAppliesEditedValue(t *testing.T) {
	svc := newFakeService()
	calls := 0
	pager := produce.PagerOpenerFunc(func(initial []byte) ([]byte, error) {
		calls++
		assert.Equal(t, []byte("seed"), initial)
		return []byte("seed-edited"), nil
	})
	m := produce.New(produce.Options{Service: svc, Topic: "orders", Pager: pager})
	typeText(m, "value", "seed")

	_, _ = m.Update(keyPress("ctrl+e"))

	assert.Equal(t, 1, calls)
	val, _ := m.Form().Field("value")
	assert.Equal(t, "seed-edited", val.Value)
}

func TestCtrlE_NoPagerEmitsWarning(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders"})

	_, _ = m.Update(keyPress("ctrl+e"))

	assert.Contains(t, m.View(), "no $EDITOR opener configured")
}

func TestCtrlE_EditorErrorSurfacesToast(t *testing.T) {
	svc := newFakeService()
	pager := produce.PagerOpenerFunc(func(_ []byte) ([]byte, error) { return nil, errors.New("boom") })
	m := produce.New(produce.Options{Service: svc, Topic: "orders", Pager: pager})

	_, _ = m.Update(keyPress("ctrl+e"))

	assert.Contains(t, m.View(), "boom")
}

func TestReadOnly_BlocksSend(t *testing.T) {
	svc := newFakeService()
	m := produce.New(produce.Options{Service: svc, Topic: "orders", ReadOnly: true})

	_, cmd := m.Update(keyPress("ctrl+s"))
	drive(t, m, cmd)

	assert.Empty(t, svc.Sent())
	assert.Contains(t, m.View(), "read-only")
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
	_, _ = m.Update(keyPress("j")) // none → gzip
	typeText(m, "key", "k1")
	typeText(m, "value", "v1")

	_, cmd := m.Update(keyPress("ctrl+s"))
	drive(t, m, cmd)

	require.Len(t, svc.Sent(), 1)
	assert.Equal(t, kafka.CompressionGzip, svc.Sent()[0].Compression)
	assert.Equal(t, []byte("k1"), svc.Sent()[0].Key)
	assert.Equal(t, []byte("v1"), svc.Sent()[0].Value)
}

// ----- helpers -----

// typeText focuses the named field and types each rune. Multi-line strings
// supported only when the focused field is a textarea.
func typeText(m *produce.Model, field, text string) {
	m.Form().FocusKey(field)
	for _, r := range text {
		_, _ = m.Update(keyPressRune(r))
	}
}

type fakeService struct {
	mu     sync.Mutex
	sent   []kafka.ProduceSpec
	result kafka.ProduceResult
	err    error
}

func newFakeService() *fakeService { return &fakeService{} }

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

func keyPress(name string) tea.KeyPressMsg {
	switch name {
	case "enter":
		return tea.KeyPressMsg{Code: tea.KeyEnter}
	case "esc":
		return tea.KeyPressMsg{Code: tea.KeyEscape}
	case "backspace":
		return tea.KeyPressMsg{Code: tea.KeyBackspace}
	case "tab":
		return tea.KeyPressMsg{Code: tea.KeyTab}
	case "down":
		return tea.KeyPressMsg{Code: tea.KeyDown}
	case "up":
		return tea.KeyPressMsg{Code: tea.KeyUp}
	case "ctrl+a":
		return tea.KeyPressMsg{Code: 'a', Mod: tea.ModCtrl}
	case "ctrl+d":
		return tea.KeyPressMsg{Code: 'd', Mod: tea.ModCtrl}
	case "ctrl+e":
		return tea.KeyPressMsg{Code: 'e', Mod: tea.ModCtrl}
	case "ctrl+n":
		return tea.KeyPressMsg{Code: 'n', Mod: tea.ModCtrl}
	case "ctrl+p":
		return tea.KeyPressMsg{Code: 'p', Mod: tea.ModCtrl}
	case "ctrl+r":
		return tea.KeyPressMsg{Code: 'r', Mod: tea.ModCtrl}
	case "ctrl+s":
		return tea.KeyPressMsg{Code: 's', Mod: tea.ModCtrl}
	case "ctrl+shift+s":
		return tea.KeyPressMsg{Code: 's', Mod: tea.ModCtrl | tea.ModShift}
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
