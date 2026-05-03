package messages_test

import (
	"context"
	"errors"
	"maps"
	"strings"
	"sync"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/layout"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/messages"
)

// drive runs cmd to completion synchronously, mirroring how Bubble Tea
// dispatches commands in production.
func drive(t *testing.T, m *messages.Model, cmd tea.Cmd) {
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

func TestNew_DefaultColumnsRendersHeader(t *testing.T) {
	// arrange
	svc := newFakeService()
	// act
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	// assert
	out := m.View()
	assert.Contains(t, out, "Timestamp")
	assert.Contains(t, out, "Offset")
	assert.Contains(t, out, "Value")
}

func TestInit_LoadsMessages(t *testing.T) {
	// arrange
	now := time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC)
	svc := newFakeService()
	svc.lastN = []kafka.Message{
		{Topic: "orders", Partition: 0, Offset: 10, Timestamp: now, Value: []byte(`{"id":1}`)},
		{Topic: "orders", Partition: 1, Offset: 20, Timestamp: now, Value: []byte("plain")},
	}
	m := messages.New(messages.Options{Service: svc, Topic: "orders", Now: func() time.Time { return now }})

	// act
	drive(t, m, m.Init())

	// assert
	require.Len(t, m.Messages(), 2)
	out := m.View()
	assert.Contains(t, out, "2 messages on orders")
	assert.Contains(t, out, `{"id":1}`)
	assert.Contains(t, out, "plain")
}

func TestInit_ErrorRaisesToast(t *testing.T) {
	svc := newFakeService()
	svc.err = errors.New("connection refused")
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})

	drive(t, m, m.Init())

	out := m.View()
	assert.Contains(t, out, "connection refused")
	require.GreaterOrEqual(t, m.Toasts().Len(), 1)
}

func TestEsc_RaisesBackAction(t *testing.T) {
	m := buildModelWith(t)
	_, _ = m.Update(keyPress("esc"))
	assert.True(t, m.ConsumeAction().Back)
}

func TestEnter_OpensDetailView(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{
		{Topic: "orders", Partition: 0, Offset: 1, Value: []byte(`{"k":"v"}`)},
	})
	_, _ = m.Update(keyPress("enter"))
	assert.Equal(t, messages.ModeDetail, m.CurrentMode())
	require.NotNil(t, m.Detail())
}

func TestDetailEsc_ReturnsToList(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{
		{Topic: "orders", Value: []byte("hello")},
	})
	_, _ = m.Update(keyPress("enter"))
	require.Equal(t, messages.ModeDetail, m.CurrentMode())
	_, _ = m.Update(keyPress("esc"))
	assert.Equal(t, messages.ModeList, m.CurrentMode())
	assert.Nil(t, m.Detail())
}

func TestDetail_NextPrevNavigatesMessages(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{
		{Topic: "orders", Offset: 1, Value: []byte("a")},
		{Topic: "orders", Offset: 2, Value: []byte("b")},
		{Topic: "orders", Offset: 3, Value: []byte("c")},
	})
	_, _ = m.Update(keyPress("enter"))
	require.Equal(t, 0, m.Detail().Index())

	_, _ = m.Update(keyPress("n"))
	assert.Equal(t, 1, m.Detail().Index())
	_, _ = m.Update(keyPress("n"))
	assert.Equal(t, 2, m.Detail().Index())
	// clamps at end
	_, _ = m.Update(keyPress("n"))
	assert.Equal(t, 2, m.Detail().Index())
	_, _ = m.Update(keyPress("p"))
	assert.Equal(t, 1, m.Detail().Index())
}

func TestDetail_FormatHotkeysSwitchView(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{
		{Topic: "orders", Value: []byte(`{"a":1}`)},
	})
	_, _ = m.Update(keyPress("enter"))

	_, _ = m.Update(keyPressRune('1'))
	assert.Equal(t, messages.ViewJSON, m.Detail().ViewMode())
	_, _ = m.Update(keyPressRune('2'))
	assert.Equal(t, messages.ViewRaw, m.Detail().ViewMode())
	_, _ = m.Update(keyPressRune('3'))
	assert.Equal(t, messages.ViewHex, m.Detail().ViewMode())
}

func TestDetail_CopyRecordUsesClipboard(t *testing.T) {
	cb := &fakeClipboard{}
	model := newDetail(t, cb, []kafka.Message{
		{Topic: "orders", Partition: 1, Offset: 7, Key: []byte("kk"), Value: []byte("hello")},
	}, 0)

	_, _ = model.Update(keyPress("y"))

	require.Len(t, cb.payloads, 1)
	assert.Contains(t, cb.payloads[0], `"topic": "orders"`)
	assert.Contains(t, cb.payloads[0], `"partition": 1`)
	assert.Contains(t, cb.payloads[0], `"offset": 7`)
	assert.Contains(t, cb.payloads[0], `"text": "kk"`)
	assert.Contains(t, model.ConsumeAction().Toast, "copied record")
}

func TestDetail_CopyRecord_NoClipboard_WarnsUnavailable(t *testing.T) {
	// arrange
	model := messages.NewDetailModel(messages.DetailOptions{
		Messages: []kafka.Message{{Topic: "t", Value: []byte("v")}},
		Index:    0,
	})

	// act
	_, _ = model.Update(keyPress("y"))

	// assert
	assert.Contains(t, model.ConsumeAction().Warn, "clipboard unavailable")
}

func TestDetail_CopyRecord_ClipboardError_WarnsWithError(t *testing.T) {
	// arrange
	cb := &fakeClipboard{err: errors.New("no display")}
	model := newDetail(t, cb, []kafka.Message{
		{Topic: "t", Value: []byte("v")},
	}, 0)

	// act
	_, _ = model.Update(keyPress("y"))

	// assert
	assert.Contains(t, model.ConsumeAction().Warn, "no display")
}

func TestDetail_SaveValue_JSONDetectedAsJSON(t *testing.T) {
	// arrange
	fw := &fakeWriter{}
	model := messages.NewDetailModel(messages.DetailOptions{
		Messages:   []kafka.Message{{Topic: "t", Partition: 0, Offset: 1, Value: []byte(`{"a":1}`)}},
		Index:      0,
		FileWriter: fw,
		OutputDir:  "/tmp",
	})

	// act
	_, _ = model.Update(keyPress("s"))

	// assert
	require.Len(t, fw.writes, 1)
	assert.Equal(t, "/tmp/t-p0-o1-value.json", fw.writes[0].path)
}

func TestDetail_SaveValue_BinaryGetsBinExt(t *testing.T) {
	// arrange
	fw := &fakeWriter{}
	model := messages.NewDetailModel(messages.DetailOptions{
		Messages:   []kafka.Message{{Topic: "t", Partition: 0, Offset: 1, Value: []byte{0x00, 0x01, 0x02}}},
		Index:      0,
		FileWriter: fw,
		OutputDir:  "/tmp",
	})

	// act
	_, _ = model.Update(keyPress("s"))

	// assert
	require.Len(t, fw.writes, 1)
	assert.Equal(t, "/tmp/t-p0-o1-value.bin", fw.writes[0].path)
}

func TestDetail_SaveValueWritesFile(t *testing.T) {
	fw := &fakeWriter{}
	model := messages.NewDetailModel(messages.DetailOptions{
		Messages:   []kafka.Message{{Topic: "orders", Partition: 1, Offset: 7, Value: []byte("hello")}},
		Index:      0,
		FileWriter: fw,
		OutputDir:  "/tmp",
	})

	_, _ = model.Update(keyPress("s"))

	require.Len(t, fw.writes, 1)
	assert.Equal(t, "/tmp/orders-p1-o7-value.txt", fw.writes[0].path)
	assert.Equal(t, []byte("hello"), fw.writes[0].data)
	assert.Contains(t, model.ConsumeAction().Toast, "saved")
}

func TestDetail_SaveFullJSON(t *testing.T) {
	fw := &fakeWriter{}
	model := messages.NewDetailModel(messages.DetailOptions{
		Messages:   []kafka.Message{{Topic: "orders", Partition: 0, Offset: 3, Value: []byte(`{"k":1}`)}},
		Index:      0,
		FileWriter: fw,
		OutputDir:  "/tmp",
	})

	_, _ = model.Update(keyPress("S"))

	require.Len(t, fw.writes, 1)
	assert.Equal(t, "/tmp/orders-p0-o3-record.json", fw.writes[0].path)
	assert.Contains(t, string(fw.writes[0].data), `"topic": "orders"`)
}

func TestDetail_OpenEditorInvokesPager(t *testing.T) {
	pager := &fakePager{}
	model := messages.NewDetailModel(messages.DetailOptions{
		Messages: []kafka.Message{{Topic: "orders", Value: []byte("hello")}},
		Index:    0,
		Pager:    pager,
	})

	_, _ = model.Update(keyPress("e"))

	assert.Equal(t, 1, pager.calls)
}

func TestDetail_ResendRaisesProduceWithMessage(t *testing.T) {
	model := messages.NewDetailModel(messages.DetailOptions{
		Messages: []kafka.Message{{Topic: "orders", Value: []byte("hello")}},
		Index:    0,
	})

	_, _ = model.Update(keyPress("r"))

	a := model.ConsumeAction()
	assert.Equal(t, "orders", a.Produce)
	require.NotNil(t, a.PrefillFromMessage)
	assert.Equal(t, []byte("hello"), a.PrefillFromMessage.Value)
}

func TestDetail_ResendBlockedInReadOnly(t *testing.T) {
	model := messages.NewDetailModel(messages.DetailOptions{
		Messages: []kafka.Message{{Topic: "orders", Value: []byte("hello")}},
		Index:    0,
		ReadOnly: true,
	})

	_, _ = model.Update(keyPress("r"))

	a := model.ConsumeAction()
	assert.Empty(t, a.Produce)
	assert.Contains(t, a.Warn, "read-only")
}

func TestProduce_RaisesAction(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{
		{Topic: "orders", Value: []byte("v")},
	})
	_, _ = m.Update(keyPress("p"))
	a := m.ConsumeAction()
	assert.Equal(t, "orders", a.Produce)
	assert.Nil(t, a.PrefillFromMessage)
}

func TestResend_FromListRaisesActionWithMessage(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{
		{Topic: "orders", Offset: 5, Value: []byte("payload")},
	})
	_, _ = m.Update(keyPress("r"))
	a := m.ConsumeAction()
	assert.Equal(t, "orders", a.Produce)
	require.NotNil(t, a.PrefillFromMessage)
	assert.Equal(t, []byte("payload"), a.PrefillFromMessage.Value)
}

func TestReadOnly_BlocksProduceAndResend(t *testing.T) {
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Value: []byte("v")}}
	m := messages.New(messages.Options{Service: svc, Topic: "orders", ReadOnly: true})
	drive(t, m, m.Init())

	_, _ = m.Update(keyPress("p"))
	assert.Empty(t, m.ConsumeAction().Produce)
	_, _ = m.Update(keyPress("r"))
	assert.Empty(t, m.ConsumeAction().Produce)
	assert.GreaterOrEqual(t, m.Toasts().Len(), 1)
}

func TestEarlierLater_FetchPagesUsingBaseline(t *testing.T) {
	svc := newFakeService()
	svc.lastN = []kafka.Message{
		{Topic: "orders", Partition: 0, Offset: 100, Value: []byte("a")},
		{Topic: "orders", Partition: 0, Offset: 101, Value: []byte("b")},
	}
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())

	svc.earlier = []kafka.Message{
		{Topic: "orders", Partition: 0, Offset: 99, Value: []byte("z")},
	}
	_, cmd := m.Update(keyPress("["))
	drive(t, m, cmd)
	require.Equal(t, map[int32]int64{0: 100}, svc.lastEarlierBaseline)
	assert.Equal(t, []byte("z"), m.Messages()[0].Value, "earlier batch is prepended")

	svc.later = []kafka.Message{
		{Topic: "orders", Partition: 0, Offset: 102, Value: []byte("c")},
	}
	_, cmd = m.Update(keyPress("]"))
	drive(t, m, cmd)
	require.Equal(t, map[int32]int64{0: 101}, svc.lastLaterBaseline)
	assert.Equal(t, []byte("c"), m.Messages()[len(m.Messages())-1].Value, "later batch is appended")
}

func TestJumpToOffset_FetchesAtOffset(t *testing.T) {
	svc := newFakeService()
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())

	svc.atOffset = []kafka.Message{
		{Topic: "orders", Partition: 2, Offset: 500, Value: []byte("at")},
	}
	cmd := m.JumpToOffset(2, 500)
	drive(t, m, cmd)

	require.Equal(t, int32(2), svc.lastOffsetPartition)
	require.Equal(t, int64(500), svc.lastOffsetValue)
	require.Len(t, m.Messages(), 1)
	assert.Equal(t, []byte("at"), m.Messages()[0].Value)
}

func TestJumpToTimestamp_FetchesAtTimestamp(t *testing.T) {
	svc := newFakeService()
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())

	target := time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC)
	svc.atTimestamp = []kafka.Message{
		{Topic: "orders", Partition: 0, Offset: 11, Timestamp: target},
	}
	cmd := m.JumpToTimestamp(target)
	drive(t, m, cmd)

	assert.Equal(t, target, svc.lastTimestamp)
	require.Len(t, m.Messages(), 1)
	assert.Equal(t, target, m.Messages()[0].Timestamp)
}

func TestJumpToPartition_NarrowsFilterAndReloads(t *testing.T) {
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Partition: 3, Offset: 1}}
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())

	cmd := m.JumpToPartition([]int32{3, 4})
	drive(t, m, cmd)

	assert.Equal(t, []int32{3, 4}, svc.lastPartitions)
}

func TestGPrefix_OpenJumpFormShowsToast(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{{Topic: "orders", Value: []byte("v")}})
	_, _ = m.Update(keyPress("g"))
	_, _ = m.Update(keyPress("o"))
	require.GreaterOrEqual(t, m.Toasts().Len(), 1)
}

func TestFollow_StartReceivesChunkAndPrependsToList(t *testing.T) {
	now := time.Now()
	svc := newFakeService()
	svc.lastN = []kafka.Message{
		{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("old")},
	}
	msgCh := make(chan kafka.Message, 4)
	errCh := make(chan error, 1)
	svc.followSession = &kafka.FollowSession{Messages: msgCh, Errors: errCh}

	m := messages.New(messages.Options{
		Service: svc,
		Topic:   "orders",
		Now:     func() time.Time { return now },
	})
	drive(t, m, m.Init())

	// queue an incoming follow chunk before the model issues the poll. The
	// errors channel is intentionally left open so the select inside
	// followPollCmd cannot race with a closed errCh.
	msgCh <- kafka.Message{Topic: "orders", Partition: 0, Offset: 2, Value: []byte("new"), Timestamp: now}
	close(msgCh)
	_ = errCh // referenced to keep the channel alive for the test duration

	_, cmd := m.Update(keyPress("f"))
	require.NotNil(t, cmd)
	assert.True(t, m.Following())
	drive(t, m, cmd)

	// after the chunk arrived the new message should be at the top.
	require.GreaterOrEqual(t, len(m.Messages()), 2)
	assert.Equal(t, []byte("new"), m.Messages()[0].Value)
}

func TestFormatTimestamp_AlwaysIncludesFullDate(t *testing.T) {
	now := time.Date(2026, 4, 28, 14, 0, 0, 0, time.UTC)
	ts := time.Date(2026, 4, 28, 9, 30, 15, 250_000_000, time.UTC)
	// timestamps come from the broker in UTC and are rendered in the
	// local timezone — compute the expected string the same way so the
	// test is portable across hosts.
	wantSame := ts.Local().Format("2006-01-02 15:04:05.000")
	assert.Equal(t, wantSame, messages.FormatTimestamp(ts, now))

	older := time.Date(2026, 3, 15, 14, 5, 0, 0, time.UTC)
	wantOlder := older.Local().Format("2006-01-02 15:04:05.000")
	assert.Equal(t, wantOlder, messages.FormatTimestamp(older, now))
}

func TestFormatTimestamp_ZeroReturnsDash(t *testing.T) {
	assert.Equal(t, "—", messages.FormatTimestamp(time.Time{}, time.Now()))
}

func TestKeyHints_ContainExpectedLabels(t *testing.T) {
	svc := newFakeService()
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	labels := keyHintLabels(m.KeyHints())
	got := strings.Join(labels, ",")
	assert.Contains(t, got, "detail")
	assert.Contains(t, got, "follow")
	assert.Contains(t, got, "earlier/later")
	assert.Contains(t, got, "search")
	assert.Contains(t, got, "produce")
}

func TestKeyHints_ReadOnlyOmitsProduce(t *testing.T) {
	svc := newFakeService()
	m := messages.New(messages.Options{Service: svc, Topic: "orders", ReadOnly: true})
	labels := keyHintLabels(m.KeyHints())
	assert.NotContains(t, strings.Join(labels, ","), "produce")
}

// ----- helpers -----

func buildModelWith(t *testing.T) *messages.Model {
	t.Helper()
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Value: []byte("hello")}}
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())
	return m
}

func buildModelWithMessages(t *testing.T, msgs []kafka.Message) *messages.Model {
	t.Helper()
	svc := newFakeService()
	svc.lastN = msgs
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())
	return m
}

func newDetail(t *testing.T, cb messages.Clipboard, msgs []kafka.Message, idx int) *messages.DetailModel {
	t.Helper()
	return messages.NewDetailModel(messages.DetailOptions{
		Messages:  msgs,
		Index:     idx,
		Clipboard: cb,
	})
}

func keyHintLabels(hints []layout.KeyHint) []string {
	out := make([]string, 0, len(hints))
	for _, h := range hints {
		out = append(out, h.Label)
	}
	return out
}

type fakeService struct {
	mu          sync.Mutex
	lastN       []kafka.Message
	earlier     []kafka.Message
	later       []kafka.Message
	atOffset    []kafka.Message
	atTimestamp []kafka.Message
	err         error

	lastPartitions      []int32
	lastEarlierBaseline map[int32]int64
	lastLaterBaseline   map[int32]int64
	lastOffsetPartition int32
	lastOffsetValue     int64
	lastTimestamp       time.Time
	followSession       *kafka.FollowSession
}

func newFakeService() *fakeService { return &fakeService{} }

func (f *fakeService) FetchLastN(_ context.Context, _ string, _ int, parts []int32) ([]kafka.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastPartitions = append([]int32(nil), parts...)
	return append([]kafka.Message(nil), f.lastN...), f.err
}

func (f *fakeService) FetchAtOffset(_ context.Context, _ string, p int32, off int64, _ int) ([]kafka.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastOffsetPartition = p
	f.lastOffsetValue = off
	return append([]kafka.Message(nil), f.atOffset...), f.err
}

func (f *fakeService) FetchAtTimestamp(_ context.Context, _ string, ts time.Time, parts []int32, _ int) ([]kafka.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastTimestamp = ts
	f.lastPartitions = append([]int32(nil), parts...)
	return append([]kafka.Message(nil), f.atTimestamp...), f.err
}

func (f *fakeService) FetchEarlier(_ context.Context, _ string, baseline map[int32]int64, _ int, parts []int32) ([]kafka.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastEarlierBaseline = copyMap(baseline)
	f.lastPartitions = append([]int32(nil), parts...)
	return append([]kafka.Message(nil), f.earlier...), f.err
}

func (f *fakeService) FetchLater(_ context.Context, _ string, baseline map[int32]int64, _ int, parts []int32) ([]kafka.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastLaterBaseline = copyMap(baseline)
	f.lastPartitions = append([]int32(nil), parts...)
	return append([]kafka.Message(nil), f.later...), f.err
}

func (f *fakeService) Follow(_ context.Context, _ string, parts []int32) (*kafka.FollowSession, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastPartitions = append([]int32(nil), parts...)
	return f.followSession, nil
}

func copyMap(m map[int32]int64) map[int32]int64 {
	out := make(map[int32]int64, len(m))
	maps.Copy(out, m)
	return out
}

type fakeClipboard struct {
	payloads []string
	err      error
}

func (f *fakeClipboard) Copy(_ context.Context, payload string) error {
	if f.err != nil {
		return f.err
	}
	f.payloads = append(f.payloads, payload)
	return nil
}

type writeRecord struct {
	path string
	data []byte
}

type fakeWriter struct {
	writes []writeRecord
	err    error
}

func (f *fakeWriter) Write(path string, data []byte) error {
	if f.err != nil {
		return f.err
	}
	f.writes = append(f.writes, writeRecord{path: path, data: append([]byte(nil), data...)})
	return nil
}

type fakePager struct {
	calls int
	err   error
}

func (f *fakePager) Open(_ string) error {
	f.calls++
	return f.err
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
