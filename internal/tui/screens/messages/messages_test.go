package messages_test

import (
	"context"
	"errors"
	"maps"
	"strconv"
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
		follow := m.Update(msg)
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
	assert.Contains(t, m.Title(), "Messages · orders [2]")
	out := m.View()
	assert.Contains(t, out, `{"id":1}`)
	assert.Contains(t, out, "plain")
}

func TestInit_ErrorRaisesToast(t *testing.T) {
	svc := newFakeService()
	svc.err = errors.New("connection refused")
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})

	drive(t, m, m.Init())

	require.GreaterOrEqual(t, m.Toasts().Len(), 1)
	assert.Contains(t, m.Toasts().Items()[m.Toasts().Len()-1].Message, "connection refused")
}

func TestEsc_RaisesBackAction(t *testing.T) {
	m := buildModelWith(t)
	_ = m.Update(keyPress("esc"))
	assert.True(t, m.ConsumeAction().Back)
}

func TestEnter_OpensDetailView(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{
		{Topic: "orders", Partition: 0, Offset: 1, Value: []byte(`{"k":"v"}`)},
	})
	_ = m.Update(keyPress("enter"))
	assert.Equal(t, messages.ModeDetail, m.CurrentMode())
	require.NotNil(t, m.Detail())
}

func TestDetailEsc_ReturnsToList(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{
		{Topic: "orders", Value: []byte("hello")},
	})
	_ = m.Update(keyPress("enter"))
	require.Equal(t, messages.ModeDetail, m.CurrentMode())
	_ = m.Update(keyPress("esc"))
	assert.Equal(t, messages.ModeList, m.CurrentMode())
	assert.Nil(t, m.Detail())
}

// TestHasOverlay_DetailIsOverlay pins the host contract: while in
// ModeDetail the screen reports HasOverlay=true so the host's q/esc
// fallback yields esc to the screen (which closes detail) instead of
// also popping the messages screen — without this the user would skip
// the list view entirely with a single esc.
func TestHasOverlay_DetailIsOverlay(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{
		{Topic: "orders", Value: []byte("hello")},
	})
	require.False(t, m.HasOverlay(), "list mode is not an overlay")

	_ = m.Update(keyPress("enter"))
	require.Equal(t, messages.ModeDetail, m.CurrentMode())
	assert.True(t, m.HasOverlay(), "detail mode must report as overlay so esc stays inside the screen")

	_ = m.Update(keyPress("esc"))
	assert.False(t, m.HasOverlay(), "after esc closes detail, overlay must clear")
}

// TestSupportsSearch_DisabledInDetail pins the host contract: while in
// ModeDetail the screen reports SupportsSearch=false so the host skips
// opening the `/` prompt — there's no rows to filter, an inert prompt
// would just swallow keystrokes.
func TestSupportsSearch_DisabledInDetail(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{
		{Topic: "orders", Value: []byte("hello")},
	})
	require.True(t, m.SearchAvailable(), "list mode must support search")

	_ = m.Update(keyPress("enter"))
	require.Equal(t, messages.ModeDetail, m.CurrentMode())
	assert.False(t, m.SearchAvailable(), "detail mode has no rows to filter")
}

// TestClose_StopsFollowSession pins the lifecycle contract: when the
// host swaps the active screen the messages model's Close() must
// terminate any open FollowSession so its kgo consumer / goroutine
// (started against context.Background) is released instead of leaking
// indefinitely.
func TestClose_StopsFollowSession(t *testing.T) {
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Partition: 0, Offset: 1}}
	msgCh := make(chan kafka.Message)
	errCh := make(chan error)
	svc.followSession = &kafka.FollowSession{Messages: msgCh, Errors: errCh}

	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())
	_ = m.Update(keyPress("f"))
	require.True(t, m.Following())

	m.Close()
	assert.False(t, m.Following(), "Close must terminate the follow session")

	// idempotent — second call must not panic on the already-closed session.
	m.Close()
}

func TestDetail_NextPrevNavigatesMessages(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{
		{Topic: "orders", Offset: 1, Value: []byte("a")},
		{Topic: "orders", Offset: 2, Value: []byte("b")},
		{Topic: "orders", Offset: 3, Value: []byte("c")},
	})
	_ = m.Update(keyPress("enter"))
	require.Equal(t, 0, m.Detail().Index())

	_ = m.Update(keyPress("n"))
	assert.Equal(t, 1, m.Detail().Index())
	_ = m.Update(keyPress("n"))
	assert.Equal(t, 2, m.Detail().Index())
	// clamps at end
	_ = m.Update(keyPress("n"))
	assert.Equal(t, 2, m.Detail().Index())
	_ = m.Update(keyPress("p"))
	assert.Equal(t, 1, m.Detail().Index())
}

func TestDetail_FormatHotkeysSwitchView(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{
		{Topic: "orders", Value: []byte(`{"a":1}`)},
	})
	_ = m.Update(keyPress("enter"))

	_ = m.Update(keyPressRune('1'))
	assert.Equal(t, messages.ViewJSON, m.Detail().ViewMode())
	_ = m.Update(keyPressRune('2'))
	assert.Equal(t, messages.ViewRaw, m.Detail().ViewMode())
	_ = m.Update(keyPressRune('3'))
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
	_ = m.Update(keyPress("p"))
	a := m.ConsumeAction()
	assert.Equal(t, "orders", a.Produce)
	assert.Nil(t, a.PrefillFromMessage)
}

func TestResend_FromListRaisesActionWithMessage(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{
		{Topic: "orders", Offset: 5, Value: []byte("payload")},
	})
	_ = m.Update(keyPress("r"))
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

	_ = m.Update(keyPress("p"))
	assert.Empty(t, m.ConsumeAction().Produce)
	_ = m.Update(keyPress("r"))
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
	cmd := m.Update(keyPress("["))
	drive(t, m, cmd)
	require.Equal(t, map[int32]int64{0: 100}, svc.lastEarlierBaseline)
	assert.Equal(t, []byte("z"), m.Messages()[0].Value, "earlier batch is prepended")

	svc.later = []kafka.Message{
		{Topic: "orders", Partition: 0, Offset: 102, Value: []byte("c")},
	}
	cmd = m.Update(keyPress("]"))
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
	_ = m.Update(keyPress("g"))
	_ = m.Update(keyPress("o"))
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

	cmd := m.Update(keyPress("f"))
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

func TestDetailView_RendersHeaderAndBlocks(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{{
		Topic: "orders", Partition: 3, Offset: 42,
		Timestamp: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Key:       []byte("k1"),
		Value:     []byte(`{"hello":"world"}`),
		Headers: []kafka.Header{
			{Key: "trace-id", Value: []byte("abc")},
		},
	}})
	m.SetSize(120, 30)
	_ = m.Update(keyPress("enter"))

	out := m.View()

	// header line — topic, partition, offset, timestamp, position counter.
	assert.Contains(t, out, "orders")
	assert.Contains(t, out, "partition 3")
	assert.Contains(t, out, "offset 42")
	assert.Contains(t, out, "2026-05-01")
	assert.Contains(t, out, "1/1")

	// block titles include byte counts and counts.
	assert.Contains(t, out, "Key (2 bytes)")
	assert.Contains(t, out, "Headers (1)")
	// header value rendered with trace-id key.
	assert.Contains(t, out, "trace-id")
	// value block carries the formatted JSON content.
	assert.Contains(t, out, "hello")
	assert.Contains(t, out, "world")
}

func TestDetailView_EmptyKeyAndHeadersRenderEmptyMarker(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{{
		Topic: "orders", Value: []byte("value-only"),
	}})
	m.SetSize(80, 20)
	_ = m.Update(keyPress("enter"))

	out := m.View()
	assert.Contains(t, out, "(empty)", "empty key + headers must show the empty marker")
	assert.Contains(t, out, "value-only")
}

// largeValue produces a deterministic multi-line payload long enough to
// overflow a small detail viewport.
func largeValue(lines int) []byte {
	parts := make([]string, lines)
	for i := range lines {
		parts[i] = "line-" + strconv.Itoa(i)
	}
	return []byte(strings.Join(parts, "\n"))
}

func openDetailWithLarge(t *testing.T, h, lines int) *messages.Model {
	t.Helper()
	m := buildModelWithMessages(t, []kafka.Message{{
		Topic: "orders", Partition: 0, Offset: 1, Value: largeValue(lines),
	}})
	m.SetSize(80, h)
	_ = m.Update(keyPress("enter"))
	require.NotNil(t, m.Detail())
	return m
}

func TestDetail_VerticalScroll_JumpsAndClamps(t *testing.T) {
	// arrange
	m := openDetailWithLarge(t, 12, 200)
	d := m.Detail()
	require.Equal(t, 0, d.ScrollOffset())

	// act + assert: j moves down by one
	_ = m.Update(keyPress("j"))
	assert.Equal(t, 1, d.ScrollOffset())

	// k moves back up
	_ = m.Update(keyPress("k"))
	assert.Equal(t, 0, d.ScrollOffset())

	// G goes to bottom — visible window should end at totalLines.
	_ = m.Update(keyPress("G"))
	_, last, total, ok := d.ScrollSummary()
	require.True(t, ok)
	assert.Equal(t, total, last, "G must align bottom of window with last line")

	// gg returns to top.
	_ = m.Update(keyPress("g"))
	_ = m.Update(keyPress("g"))
	assert.Equal(t, 0, d.ScrollOffset())
}

func TestDetail_PageScrollUsesViewportHeight(t *testing.T) {
	m := openDetailWithLarge(t, 10, 200)
	d := m.Detail()

	_ = m.Update(ctrlKey('f'))
	first := d.ScrollOffset()
	assert.Greater(t, first, 1, "ctrl+f must move by ~one page")

	_ = m.Update(ctrlKey('b'))
	assert.Equal(t, 0, d.ScrollOffset())
}

func TestDetail_HorizontalScroll_OnlyWhenNoWrap(t *testing.T) {
	// arrange: short payload but a single very wide line so wrap matters.
	wide := strings.Repeat("X", 400)
	m := buildModelWithMessages(t, []kafka.Message{{
		Topic: "orders", Value: []byte(wide),
	}})
	m.SetSize(40, 20)
	_ = m.Update(keyPress("enter"))
	d := m.Detail()
	require.True(t, d.Wrap(), "wrap must be on by default")

	// act: l in wrap mode is a no-op for hScroll
	_ = m.Update(keyPress("l"))
	assert.Equal(t, 0, d.HScrollOffset(), "horizontal scroll must be ignored while wrap is on")

	// switch wrap off, then l moves the window right
	_ = m.Update(keyPress("w"))
	require.False(t, d.Wrap())
	_ = m.Update(keyPress("l"))
	assert.Positive(t, d.HScrollOffset(), "l must advance horizontal offset when wrap is off")

	// h moves it back
	prev := d.HScrollOffset()
	_ = m.Update(keyPress("h"))
	assert.Less(t, d.HScrollOffset(), prev)
}

func TestDetail_HorizontalScroll_ClampsAtMaxLineWidth(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{{
		Topic: "orders", Value: []byte(strings.Repeat("X", 120)),
	}})
	m.SetSize(40, 20)
	_ = m.Update(keyPress("enter"))
	_ = m.Update(keyPress("w"))
	d := m.Detail()
	require.False(t, d.Wrap())

	for range 1000 {
		_ = m.Update(keyPress("l"))
	}
	// pinned at maxLineWidth - width, never grows past content.
	first := d.HScrollOffset()
	_ = m.Update(keyPress("l"))
	assert.Equal(t, first, d.HScrollOffset(), "horizontal scroll must clamp at content width")
}

func TestDetail_WrapTogglePreservedAcrossNextPrev(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{
		{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("v1")},
		{Topic: "orders", Partition: 0, Offset: 2, Value: []byte("v2")},
	})
	m.SetSize(80, 20)
	_ = m.Update(keyPress("enter"))
	d := m.Detail()

	_ = m.Update(keyPress("w"))
	require.False(t, d.Wrap())

	_ = m.Update(keyPress("n"))
	assert.False(t, d.Wrap(), "n must not reset wrap mode")
}

func TestDetail_NextResetsScroll(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{
		{Topic: "orders", Partition: 0, Offset: 1, Value: largeValue(200)},
		{Topic: "orders", Partition: 0, Offset: 2, Value: []byte("short")},
	})
	m.SetSize(80, 12)
	_ = m.Update(keyPress("enter"))
	d := m.Detail()

	_ = m.Update(keyPress("G"))
	require.Positive(t, d.ScrollOffset())

	_ = m.Update(keyPress("n"))
	assert.Equal(t, 0, d.ScrollOffset(), "switching message must rewind scroll")
}

func TestDetail_ViewModeSwitchResetsScroll(t *testing.T) {
	m := openDetailWithLarge(t, 12, 200)
	d := m.Detail()

	_ = m.Update(keyPress("G"))
	require.Positive(t, d.ScrollOffset())

	_ = m.Update(keyPress("2"))
	assert.Equal(t, 0, d.ScrollOffset(), "view mode switch must rewind scroll")
}

func TestDetail_SetSize_ReclampsScroll(t *testing.T) {
	m := openDetailWithLarge(t, 12, 60)
	d := m.Detail()

	_ = m.Update(keyPress("G"))
	scrolled := d.ScrollOffset()
	require.Positive(t, scrolled)

	// growing the body so everything fits should pin scroll back to 0.
	m.SetSize(80, 200)
	assert.Equal(t, 0, d.ScrollOffset(), "growing the viewport must drop the now-invalid offset")
}

func TestMessages_WrapPersistsAcrossDetailReopen(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{{
		Topic: "orders", Value: []byte("v"),
	}})
	m.SetSize(80, 20)
	_ = m.Update(keyPress("enter"))
	require.True(t, m.Detail().Wrap())

	_ = m.Update(keyPress("w"))
	require.False(t, m.Detail().Wrap())
	_ = m.Update(keyPress("esc"))
	require.Nil(t, m.Detail())

	_ = m.Update(keyPress("enter"))
	require.NotNil(t, m.Detail())
	assert.False(t, m.Detail().Wrap(), "wrap preference must survive close/reopen")
}

func TestMessages_BreadcrumbTracksDetailNavigation(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{
		{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("a")},
		{Topic: "orders", Partition: 2, Offset: 42, Value: []byte("b")},
	})
	m.SetSize(80, 20)
	_ = m.Update(keyPress("enter"))
	require.Equal(t, "msg-0-1", m.Breadcrumb())

	_ = m.Update(keyPress("n"))
	assert.Equal(t, "msg-2-42", m.Breadcrumb(), "breadcrumb must follow detail's focused message")

	_ = m.Update(keyPress("p"))
	assert.Equal(t, "msg-0-1", m.Breadcrumb())
}

func TestMessages_TitleEmbedsScrollIndicator(t *testing.T) {
	m := openDetailWithLarge(t, 12, 200)

	title := m.Title()
	assert.Contains(t, title, "wrap")
	assert.Regexp(t, `L\d+-\d+/\d+`, title, "title must include line range while in detail")

	_ = m.Update(keyPress("w"))
	assert.Contains(t, m.Title(), "nowrap")
}

// stripANSI removes terminal escape sequences so tests can compare against
// plain expected substrings without depending on the active theme palette.
func stripANSI(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == 0x1b && i+1 < len(s) && s[i+1] == '[' {
			j := i + 2
			for j < len(s) && (s[j] < 0x40 || s[j] > 0x7e) {
				j++
			}
			if j < len(s) {
				j++
			}
			i = j - 1
			continue
		}
		out = append(out, s[i])
	}
	return string(out)
}

// findValueLine returns the first body line that contains needle within the
// rendered view, or "" when not found. Helps tests reason about what the
// user sees regardless of header / block-title chrome.
func findValueLine(view, needle string) string {
	for line := range strings.SplitSeq(stripANSI(view), "\n") {
		if strings.Contains(line, needle) {
			return line
		}
	}
	return ""
}

func TestDetail_WrapOn_BreaksLongLineAcrossVisualLines(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{{
		Topic: "orders", Value: []byte(strings.Repeat("X", 200)),
	}})
	m.SetSize(40, 30)
	_ = m.Update(keyPress("enter"))
	require.True(t, m.Detail().Wrap())

	// the long X line should appear as several visual lines in the rendered body.
	xLines := 0
	for line := range strings.SplitSeq(stripANSI(m.View()), "\n") {
		if strings.Contains(line, "XXXX") {
			xLines++
		}
	}
	assert.Greater(t, xLines, 1, "wrap=on must split a 200-char line into multiple visual lines at width=40")
}

func TestDetail_WrapOff_KeepsLongLineSingleAndTruncates(t *testing.T) {
	payload := strings.Repeat("X", 200)
	m := buildModelWithMessages(t, []kafka.Message{{
		Topic: "orders", Value: []byte(payload),
	}})
	m.SetSize(40, 30)
	_ = m.Update(keyPress("enter"))
	_ = m.Update(keyPress("w"))
	require.False(t, m.Detail().Wrap())

	xLines := 0
	for line := range strings.SplitSeq(stripANSI(m.View()), "\n") {
		if strings.Contains(line, "XXXX") {
			xLines++
			// each rendered line must fit the viewport width (40).
			assert.LessOrEqual(t, len(line), 40, "nowrap mode must truncate, not overflow the viewport")
		}
	}
	assert.Equal(t, 1, xLines, "wrap=off must keep the long line as a single visual line")
}

func TestDetail_HScroll_SlidesVisibleContent(t *testing.T) {
	// arrange: payload begins with a recognizable prefix so we can tell when
	// the horizontal window has actually moved past it.
	payload := "AAAA" + strings.Repeat("B", 200)
	m := buildModelWithMessages(t, []kafka.Message{{
		Topic: "orders", Value: []byte(payload),
	}})
	m.SetSize(40, 30)
	_ = m.Update(keyPress("enter"))
	_ = m.Update(keyPress("w"))
	require.False(t, m.Detail().Wrap())

	before := findValueLine(m.View(), "AAAA")
	require.NotEmpty(t, before, "prefix must be visible before scrolling")

	// scroll right until prefix scrolls off-screen.
	for range 20 {
		_ = m.Update(keyPress("l"))
	}
	require.Positive(t, m.Detail().HScrollOffset())
	after := findValueLine(m.View(), "AAAA")
	assert.Empty(t, after, "AAAA prefix must scroll out of view once hScroll exceeds its position")
}

func TestDetail_GThenNonG_DoesNotJumpToTop(t *testing.T) {
	m := openDetailWithLarge(t, 12, 200)
	d := m.Detail()
	_ = m.Update(keyPress("G"))
	require.Positive(t, d.ScrollOffset())
	bottom := d.ScrollOffset()

	// `g` primes the gg sequence; the next non-`g` key must NOT scroll to top —
	// it should be processed as a normal scroll command (here, j moves down,
	// but we are already pinned at the bottom, so offset stays).
	_ = m.Update(keyPress("g"))
	_ = m.Update(keyPress("j"))
	assert.Equal(t, bottom, d.ScrollOffset(), "g followed by a non-g key must not jump to top")
}

func TestDetail_KeyHints_IncludeScrollAndWrap(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{{Topic: "orders", Value: []byte("v")}})
	m.SetSize(80, 20)
	_ = m.Update(keyPress("enter"))

	labels := keyHintKeys(m.KeyHints())
	assert.Contains(t, labels, "j/k", "detail hints must advertise scroll keys")
	assert.Contains(t, labels, "w", "detail hints must advertise wrap toggle")
}

func keyHintKeys(hints []layout.KeyHint) []string {
	out := make([]string, 0, len(hints))
	for _, h := range hints {
		out = append(out, h.Key)
	}
	return out
}

func ctrlKey(r rune) tea.KeyPressMsg {
	return tea.KeyPressMsg{Code: r, Mod: tea.ModCtrl}
}
