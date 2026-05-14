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
	svc := newFakeService()
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	out := m.View()
	assert.Contains(t, out, "Timestamp")
	assert.Contains(t, out, "Offset")
	assert.Contains(t, out, "Value")
}

func TestInit_LoadsMessages(t *testing.T) {
	now := time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC)
	svc := newFakeService()
	svc.lastN = []kafka.Message{
		{Topic: "orders", Partition: 0, Offset: 10, Timestamp: now, Value: []byte(`{"id":1}`)},
		{Topic: "orders", Partition: 1, Offset: 20, Timestamp: now, Value: []byte("plain")},
	}
	m := messages.New(messages.Options{Service: svc, Topic: "orders", Now: func() time.Time { return now }})

	drive(t, m, m.Init())

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
}

func TestHasOverlay_DetailIsOverlay(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{
		{Topic: "orders", Value: []byte("hello")},
	})
	require.False(t, m.HasOverlay())

	_ = m.Update(keyPress("enter"))
	require.Equal(t, messages.ModeDetail, m.CurrentMode())
	assert.True(t, m.HasOverlay())

	_ = m.Update(keyPress("esc"))
	assert.False(t, m.HasOverlay())
}

func TestSearch_DisabledOutsideListMode(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{
		{Topic: "orders", Value: []byte("hello")},
	})
	require.True(t, m.SearchAvailable())

	_ = m.Update(keyPress("enter"))
	assert.False(t, m.SearchAvailable())
}

// ----- seek popup -----

func TestSeek_OpensPopupOnS(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{{Topic: "orders", Value: []byte("v")}})
	_ = m.Update(keyPressRune('s'))
	assert.Equal(t, messages.ModeSeek, m.CurrentMode())
	assert.True(t, m.HasOverlay())
}

func TestSeek_DigitPicksLatestImmediately(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{{Topic: "orders", Value: []byte("v")}})
	_ = m.Update(keyPressRune('s'))
	cmd := m.Update(keyPressRune('1'))
	drive(t, m, cmd)
	assert.Equal(t, messages.ModeList, m.CurrentMode())
	assert.Equal(t, messages.SeekLatest, m.SeekState().Mode)
}

func TestSeek_FromTimestamp_OpensInputStage(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	svc := newFakeService()
	// no Timestamp on the row → form prefill stays empty.
	svc.lastN = []kafka.Message{{Topic: "orders", Value: []byte("v")}}
	m := messages.New(messages.Options{Service: svc, Topic: "orders", Now: func() time.Time { return now }})
	drive(t, m, m.Init())

	_ = m.Update(keyPressRune('s'))
	_ = m.Update(keyPressRune('5'))
	require.Equal(t, messages.ModeSeek, m.CurrentMode())

	for _, r := range "2026-04-27" {
		_ = m.Update(keyPressRune(r))
	}
	cmd := m.Update(keyPress("enter"))
	drive(t, m, cmd)

	assert.Equal(t, messages.ModeList, m.CurrentMode())
	assert.Equal(t, messages.SeekFromTimestamp, m.SeekState().Mode)
	assert.Equal(t, time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC), m.SeekState().Timestamp)
}

func TestSeek_FromOffset_ExplicitPartition(t *testing.T) {
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("v")}}
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())

	_ = m.Update(keyPressRune('s'))
	_ = m.Update(keyPressRune('3')) // from offset
	require.Equal(t, messages.ModeSeek, m.CurrentMode())

	// the form pre-fills from selected row (0:1). Wipe and type 3:500.
	for range len("0:1") {
		_ = m.Update(keyPress("backspace"))
	}
	for _, r := range "3:500" {
		_ = m.Update(keyPressRune(r))
	}
	svc.atOffset = []kafka.Message{{Topic: "orders", Partition: 3, Offset: 500, Value: []byte("at")}}
	cmd := m.Update(keyPress("enter"))
	drive(t, m, cmd)

	assert.Equal(t, int32(3), svc.lastOffsetPartition)
	assert.Equal(t, int64(500), svc.lastOffsetValue)
	assert.True(t, m.SeekState().HasPart)
}

func TestSeek_FromOffset_ImplicitPartitionClampsAgainstWatermarks(t *testing.T) {
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("v")}}
	svc.watermarks = map[int32]kafka.PartitionWatermarks{
		0: {Low: 0, High: 100},
		1: {Low: 50, High: 150},
	}
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())

	_ = m.Update(keyPressRune('s'))
	_ = m.Update(keyPressRune('3')) // from offset
	for range len("0:1") {
		_ = m.Update(keyPress("backspace"))
	}
	for _, r := range "200" {
		_ = m.Update(keyPressRune(r))
	}
	svc.atOffset = []kafka.Message{{Topic: "orders", Partition: 0, Offset: 99, Value: []byte("clamp")}}
	cmd := m.Update(keyPress("enter"))
	drive(t, m, cmd)

	require.False(t, m.SeekState().HasPart)
	// each partition's offset got clamped against its watermarks.
	require.Len(t, svc.atOffsetCalls, 2)
	clamped := map[int32]int64{}
	for _, c := range svc.atOffsetCalls {
		clamped[c.partition] = c.offset
	}
	assert.Equal(t, int64(99), clamped[0])
	assert.Equal(t, int64(149), clamped[1])
}

func TestSeek_InvalidTimestamp_ToastAndStaysOpen(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{{Topic: "orders", Value: []byte("v")}})
	_ = m.Update(keyPressRune('s'))
	_ = m.Update(keyPressRune('5')) // from timestamp
	require.Equal(t, messages.ModeSeek, m.CurrentMode())

	// clear the prefill (empty if no timestamp on selected) — type garbage.
	for range 50 {
		_ = m.Update(keyPress("backspace"))
	}
	for _, r := range "garbage" {
		_ = m.Update(keyPressRune(r))
	}
	_ = m.Update(keyPress("enter"))

	assert.Equal(t, messages.ModeSeek, m.CurrentMode())
	require.GreaterOrEqual(t, m.Toasts().Len(), 1)
	assert.Contains(t, m.Toasts().Items()[m.Toasts().Len()-1].Message, "invalid timestamp")
}

func TestSeek_EscFromInputReturnsToMenu(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{{Topic: "orders", Value: []byte("v")}})
	_ = m.Update(keyPressRune('s'))
	_ = m.Update(keyPressRune('5')) // from timestamp → input
	require.Equal(t, messages.ModeSeek, m.CurrentMode())

	_ = m.Update(keyPress("esc"))
	assert.Equal(t, messages.ModeSeek, m.CurrentMode(), "esc from input must return to menu, not close")

	_ = m.Update(keyPress("esc"))
	assert.Equal(t, messages.ModeList, m.CurrentMode(), "esc from menu must close")
}

// ----- partitions popup -----

func TestPartitions_OpensOnP(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{{Topic: "orders", Value: []byte("v")}})
	_ = m.Update(keyPressRune('P'))
	assert.Equal(t, messages.ModePartitions, m.CurrentMode())
}

func TestPartitions_SubmitFiltersAndReloads(t *testing.T) {
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Partition: 0, Value: []byte("v")}}
	svc.watermarks = map[int32]kafka.PartitionWatermarks{
		0: {Low: 0, High: 10},
		1: {Low: 0, High: 10},
		2: {Low: 0, High: 10},
		3: {Low: 0, High: 10},
	}
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())

	cmd := m.Update(keyPressRune('P'))
	drive(t, m, cmd) // resolve loadPartitionsCmd → partitionsLoadedMsg

	// switch to text input and type "0,2-3".
	_ = m.Update(keyPress("tab"))
	// initial input is "" (all selected). Type the new filter.
	for _, r := range "0,2-3" {
		_ = m.Update(keyPressRune(r))
	}
	cmd = m.Update(keyPress("enter"))
	drive(t, m, cmd)

	assert.Equal(t, messages.ModeList, m.CurrentMode())
	assert.Equal(t, []int32{0, 2, 3}, m.PartitionFilter())
	assert.Equal(t, []int32{0, 2, 3}, svc.lastPartitions)
}

func TestPartitions_ListToggleAppliesSubset(t *testing.T) {
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Value: []byte("v")}}
	svc.watermarks = map[int32]kafka.PartitionWatermarks{
		0: {Low: 0, High: 5},
		1: {Low: 0, High: 5},
		2: {Low: 0, High: 5},
	}
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())

	cmd := m.Update(keyPressRune('P'))
	drive(t, m, cmd)

	// initially all selected; press `a` to clear (toggle), then space on partition 1.
	_ = m.Update(keyPressRune('a'))
	_ = m.Update(keyPress("down"))
	_ = m.Update(keyPressRune(' '))
	cmd = m.Update(keyPress("enter"))
	drive(t, m, cmd)

	assert.Equal(t, []int32{1}, m.PartitionFilter())
}

func TestPartitions_AllToggleCyclesAllNone(t *testing.T) {
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Value: []byte("v")}}
	svc.watermarks = map[int32]kafka.PartitionWatermarks{
		0: {Low: 0, High: 5},
		1: {Low: 0, High: 5},
		2: {Low: 0, High: 5},
	}
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())

	cmd := m.Update(keyPressRune('P'))
	drive(t, m, cmd)

	// initial: all selected → first `a` clears.
	_ = m.Update(keyPressRune('a'))
	// pick one partition manually so we are now in a partial state.
	_ = m.Update(keyPressRune(' ')) // toggle partition 0 on
	// partial state → next `a` should pull to "all".
	_ = m.Update(keyPressRune('a'))
	cmd = m.Update(keyPress("enter"))
	drive(t, m, cmd)

	assert.Empty(t, m.PartitionFilter(), "all selected serializes to empty filter")
}

func TestPartitions_InvalidInputBlocksEnter(t *testing.T) {
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Value: []byte("v")}}
	svc.watermarks = map[int32]kafka.PartitionWatermarks{0: {Low: 0, High: 5}}
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())

	cmd := m.Update(keyPressRune('P'))
	drive(t, m, cmd)
	_ = m.Update(keyPress("tab"))
	for _, r := range "abc" {
		_ = m.Update(keyPressRune(r))
	}
	_ = m.Update(keyPress("enter"))

	assert.Equal(t, messages.ModePartitions, m.CurrentMode(), "invalid input must keep popup open")
	require.GreaterOrEqual(t, m.Toasts().Len(), 1)
}

func TestPartitions_RenderShowsScrollHintsForLargeTopic(t *testing.T) {
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Value: []byte("v")}}
	svc.watermarks = map[int32]kafka.PartitionWatermarks{}
	for i := range int32(50) {
		svc.watermarks[i] = kafka.PartitionWatermarks{Low: 0, High: 1}
	}
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	m.SetSize(80, 24)
	drive(t, m, m.Init())

	cmd := m.Update(keyPressRune('P'))
	drive(t, m, cmd)

	view := stripANSI(m.View())
	// at top: cursor at 0, only down-indicator should appear.
	assert.Contains(t, view, "more")
}

func TestRefresh_RReDispatchesCurrentSeek(t *testing.T) {
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("v1")}}
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())
	require.Len(t, m.Messages(), 1)

	// the broker now has fresh data; `r` must refetch with the current
	// seek state instead of touching filters or seek mode.
	svc.lastN = []kafka.Message{
		{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("v1")},
		{Topic: "orders", Partition: 0, Offset: 2, Value: []byte("v2")},
	}
	cmd := m.Update(keyPressRune('r'))
	drive(t, m, cmd)

	require.Len(t, m.Messages(), 2)
	assert.Equal(t, messages.SeekLatest, m.SeekState().Mode)
}

// ----- smart filter stub -----

func TestSmartFilter_FOpensStubModal(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{{Topic: "orders", Value: []byte("v")}})
	_ = m.Update(keyPressRune('f'))
	assert.Equal(t, messages.ModeSmartFilter, m.CurrentMode())

	out := m.View()
	assert.Contains(t, out, "Smart filter")

	_ = m.Update(keyPress("esc"))
	assert.Equal(t, messages.ModeList, m.CurrentMode())
}

// ----- header line -----

func TestHeader_DescribesActiveSeekAndPartitions(t *testing.T) {
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Value: []byte("v")}}
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	m.SetSize(120, 30)
	drive(t, m, m.Init())

	out := stripANSI(m.View())
	assert.Contains(t, out, "seek: latest")
	assert.Contains(t, out, "partitions: all")
	assert.Contains(t, out, "smart filter: —")
}

// ----- live tail via seek -----

func TestSeek_LiveClearsHistoricalMessages(t *testing.T) {
	// switching to live must drop any previously-loaded historical
	// messages — live tail is a stream of only new records.
	now := time.Now()
	svc := newFakeService()
	svc.lastN = []kafka.Message{
		{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("old-1"), Timestamp: now},
		{Topic: "orders", Partition: 0, Offset: 2, Value: []byte("old-2"), Timestamp: now},
	}
	msgCh := make(chan kafka.Message)
	errCh := make(chan error)
	svc.followSession = &kafka.FollowSession{Messages: msgCh, Errors: errCh}

	m := messages.New(messages.Options{Service: svc, Topic: "orders", Now: func() time.Time { return now }})
	drive(t, m, m.Init())
	require.Len(t, m.Messages(), 2, "lastN must populate the screen first")

	_ = m.Update(keyPressRune('s'))
	_ = m.Update(keyPressRune('7'))

	assert.Empty(t, m.Messages(), "switching to live must clear old messages")
	assert.True(t, m.Following())
}

func TestStaleMessagesAppendedDroppedAfterRefresh(t *testing.T) {
	// `[` issues a paging cmd; before its result lands the user refreshes the
	// view. The stale MessagesAppendedMsg must be dropped, otherwise the
	// pre-refresh payload would prepend onto the freshly loaded screen.
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Partition: 0, Offset: 5, Value: []byte("a")}}
	svc.earlier = []kafka.Message{{Topic: "orders", Partition: 0, Offset: 4, Value: []byte("stale")}}
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())

	pagingCmd := m.Update(keyPress("["))
	require.NotNil(t, pagingCmd)

	// refresh bumps fetchGen via dispatchSeek, making the in-flight paging
	// gen stale.
	refreshCmd := m.Update(keyPressRune('r'))
	drive(t, m, refreshCmd)

	// now drive the stale paging cmd.
	drive(t, m, pagingCmd)

	for _, msg := range m.Messages() {
		assert.NotEqual(t, []byte("stale"), msg.Value, "stale paging payload must not appear post-refresh")
	}
}

func TestStaleFollowChunkDroppedAfterFlipOutOfLive(t *testing.T) {
	// while in live, a chunk could already be sitting in the channel buffer
	// when the user steps via `[` and the screen flips to latest. The
	// chunk's stale Gen must cause it to be discarded by handleFollowChunk.
	now := time.Now()
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("a"), Timestamp: now}}
	msgCh := make(chan kafka.Message)
	errCh := make(chan error)
	svc.followSession = &kafka.FollowSession{Messages: msgCh, Errors: errCh}

	m := messages.New(messages.Options{Service: svc, Topic: "orders", Now: func() time.Time { return now }})
	drive(t, m, m.Init())
	_ = m.Update(keyPressRune('s'))
	_ = m.Update(keyPressRune('7'))
	require.True(t, m.Following())

	// flip out of live: bumps fetchGen twice (stopFollow + dispatchSeek).
	flipCmd := m.Update(keyPress("["))
	drive(t, m, flipCmd)
	require.False(t, m.Following())
	preFlip := append([]kafka.Message(nil), m.Messages()...)

	// stale chunk arrives carrying an outdated Gen — Gen=0 is guaranteed
	// stale because every dispatchSeek/stopFollow only ever increments.
	_ = m.Update(messages.FollowChunkMsg{
		Gen:      0,
		Messages: []kafka.Message{{Topic: "orders", Value: []byte("stale-live"), Timestamp: now}},
	})

	for _, msg := range m.Messages() {
		assert.NotEqual(t, []byte("stale-live"), msg.Value, "stale follow chunk must not bleed into latest")
	}
	assert.Len(t, m.Messages(), len(preFlip), "stale chunk must not change message count")
}

func TestPartitions_InputUpdatesCheckboxes(t *testing.T) {
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Value: []byte("v")}}
	svc.watermarks = map[int32]kafka.PartitionWatermarks{
		0: {Low: 0, High: 5},
		1: {Low: 0, High: 5},
		2: {Low: 0, High: 5},
	}
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	m.SetSize(80, 24)
	drive(t, m, m.Init())
	cmd := m.Update(keyPressRune('P'))
	drive(t, m, cmd)

	// switch to input and type "0,2" — checkboxes must follow.
	_ = m.Update(keyPress("tab"))
	for _, r := range "0,2" {
		_ = m.Update(keyPressRune(r))
	}

	// rendering uses `[×]` for ticked, `[ ]` for unticked.
	view := stripANSI(m.View())
	assert.Contains(t, view, "[×] 0", "input edit must tick partition 0 in the checkbox list")
	assert.Contains(t, view, "[×] 2", "input edit must tick partition 2 in the checkbox list")
	assert.Contains(t, view, "[ ] 1", "partition 1 must be unticked")

	// applying preserves the input-driven selection end-to-end.
	cmd = m.Update(keyPress("enter"))
	drive(t, m, cmd)
	assert.Equal(t, []int32{0, 2}, m.PartitionFilter())
}

func TestPartitions_LoadErrorShowsFallback(t *testing.T) {
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Value: []byte("v")}}
	svc.watermarksErr = errors.New("metadata down")
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	m.SetSize(80, 24)
	drive(t, m, m.Init())

	cmd := m.Update(keyPressRune('P'))
	drive(t, m, cmd)

	view := stripANSI(m.View())
	assert.Contains(t, view, "load failed", "load error must be surfaced in the popup")
	assert.Contains(t, view, "metadata down")

	// fallback path: text input still works for valid syntax even without
	// the partition list.
	_ = m.Update(keyPress("tab"))
	for _, r := range "0,3" {
		_ = m.Update(keyPressRune(r))
	}
	cmd = m.Update(keyPress("enter"))
	drive(t, m, cmd)
	assert.Equal(t, []int32{0, 3}, m.PartitionFilter())
}

func TestBracket_FromTimestampStartOfSeekWindowToast(t *testing.T) {
	// from-timestamp resolves to per-partition offsets via OffsetsForTimestamp.
	// the first record loaded sits at the captured left edge → `[` must
	// surface the boundary toast.
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	svc := newFakeService()
	svc.offsetsForTs = map[int32]int64{0: 50}
	svc.atOffset = []kafka.Message{{Topic: "orders", Partition: 0, Offset: 50, Timestamp: now}}
	m := messages.New(messages.Options{Service: svc, Topic: "orders", Now: func() time.Time { return now }})
	drive(t, m, m.Init())

	_ = m.Update(keyPressRune('s'))
	_ = m.Update(keyPressRune('5')) // from timestamp
	for range 50 {
		_ = m.Update(keyPress("backspace"))
	}
	for _, r := range "2026-04-27" {
		_ = m.Update(keyPressRune(r))
	}
	cmd := m.Update(keyPress("enter"))
	drive(t, m, cmd)
	require.Equal(t, messages.SeekFromTimestamp, m.SeekState().Mode)

	before := m.Toasts().Len()
	_ = m.Update(keyPress("["))
	require.Greater(t, m.Toasts().Len(), before)
	last := m.Toasts().Items()[m.Toasts().Len()-1].Message
	assert.Contains(t, last, "start of seek window")
}

func TestBracket_FromOffsetStartOfSeekWindowToast(t *testing.T) {
	svc := newFakeService()
	svc.atOffset = []kafka.Message{{Topic: "orders", Partition: 0, Offset: 100, Value: []byte("v")}}
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())

	// open seek, pick "from offset" (3), wipe prefill, type 0:100.
	_ = m.Update(keyPressRune('s'))
	_ = m.Update(keyPressRune('3'))
	for range 50 {
		_ = m.Update(keyPress("backspace"))
	}
	for _, r := range "0:100" {
		_ = m.Update(keyPressRune(r))
	}
	cmd := m.Update(keyPress("enter"))
	drive(t, m, cmd)

	// the loaded record sits exactly at the captured left edge — `[` must
	// refuse to step past it and surface the dedicated toast.
	before := m.Toasts().Len()
	_ = m.Update(keyPress("["))
	require.Greater(t, m.Toasts().Len(), before)
	last := m.Toasts().Items()[m.Toasts().Len()-1].Message
	assert.Contains(t, last, "start of seek window")
}

func TestBracket_ToOffsetEndOfSeekWindowToast(t *testing.T) {
	svc := newFakeService()
	svc.earlier = []kafka.Message{{Topic: "orders", Partition: 0, Offset: 100, Value: []byte("v")}}
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())

	// open seek, pick "to offset" (4), prefill empty, type 0:100.
	_ = m.Update(keyPressRune('s'))
	_ = m.Update(keyPressRune('4'))
	for range 50 {
		_ = m.Update(keyPress("backspace"))
	}
	for _, r := range "0:100" {
		_ = m.Update(keyPressRune(r))
	}
	cmd := m.Update(keyPress("enter"))
	drive(t, m, cmd)

	// now the boundary {0:101} is captured and m.messages = [{0,100}].
	// `]` must hit the boundary and surface the dedicated toast.
	before := m.Toasts().Len()
	_ = m.Update(keyPress("]"))
	require.Greater(t, m.Toasts().Len(), before)
	last := m.Toasts().Items()[m.Toasts().Len()-1].Message
	assert.Contains(t, last, "end of seek window")
}

func TestStaleMessagesLoadedDroppedAfterSeekChange(t *testing.T) {
	// Race: a MessagesLoadedMsg from a prior dispatch arrives after the
	// user has switched to live. Without generation tagging the stale
	// payload would replace the cleared screen with historical messages.
	now := time.Now()
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Offset: 1, Value: []byte("old"), Timestamp: now}}
	msgCh := make(chan kafka.Message)
	errCh := make(chan error)
	svc.followSession = &kafka.FollowSession{Messages: msgCh, Errors: errCh}

	m := messages.New(messages.Options{Service: svc, Topic: "orders", Now: func() time.Time { return now }})
	initCmd := m.Init() // produces MessagesLoadedMsg eventually

	// switch to live before the initial load is delivered.
	_ = m.Update(keyPressRune('s'))
	_ = m.Update(keyPressRune('7'))
	require.True(t, m.Following())

	// now drive the deferred init cmd: it returns a stale MessagesLoadedMsg
	// which the handler must drop.
	drive(t, m, initCmd)

	assert.Empty(t, m.Messages(), "stale historical fetch must not bleed into live mode")
	assert.True(t, m.Following())
}

func TestLive_TitleSpinnerAdvancesOnTick(t *testing.T) {
	now := time.Now()
	svc := newFakeService()
	svc.lastN = []kafka.Message{}
	msgCh := make(chan kafka.Message)
	errCh := make(chan error)
	svc.followSession = &kafka.FollowSession{Messages: msgCh, Errors: errCh}

	m := messages.New(messages.Options{Service: svc, Topic: "orders", Now: func() time.Time { return now }})
	drive(t, m, m.Init())
	_ = m.Update(keyPressRune('s'))
	_ = m.Update(keyPressRune('7'))
	require.True(t, m.Following())

	first := stripANSI(m.Title())
	// drive a few ticks manually with the current dispatch gen so the
	// race-protected handler accepts them.
	gen := m.FetchGen()
	for range 3 {
		_ = m.Update(messages.LiveTickMsg{Gen: gen})
	}
	second := stripANSI(m.Title())

	// the spinner glyph next to LIVE must have advanced.
	assert.NotEqual(t, first, second, "title spinner should change after ticks")
	assert.Contains(t, second, "LIVE")
}

func TestLive_StaleTickFromPreviousLiveDropped(t *testing.T) {
	// race: a LiveTickMsg from a prior live session arrives after the user
	// has already started a new one. Without Gen-tagging the stale tick
	// would re-arm itself and double the spinner rate.
	now := time.Now()
	svc := newFakeService()
	svc.followSession = &kafka.FollowSession{Messages: make(chan kafka.Message), Errors: make(chan error)}
	m := messages.New(messages.Options{Service: svc, Topic: "orders", Now: func() time.Time { return now }})
	drive(t, m, m.Init())

	_ = m.Update(keyPressRune('s'))
	_ = m.Update(keyPressRune('7'))
	staleGen := m.FetchGen()

	// step out of live and back in — fetchGen advances past staleGen.
	cmd := m.Update(keyPress("["))
	drive(t, m, cmd)
	require.False(t, m.Following())
	_ = m.Update(keyPressRune('s'))
	_ = m.Update(keyPressRune('7'))
	require.True(t, m.Following())
	require.NotEqual(t, staleGen, m.FetchGen())

	// stale tick must be dropped — handler returns no follow-up cmd.
	follow := m.Update(messages.LiveTickMsg{Gen: staleGen})
	assert.Nil(t, follow, "stale LiveTickMsg must not re-arm the spinner")
}

func TestLive_PlaceholderSpinnerAdvancesOnTick(t *testing.T) {
	// placeholder shows the same frame as the title indicator. on a tick
	// the rendered glyph in the placeholder must advance, otherwise the
	// "broker is silent" surface goes static and looks frozen.
	now := time.Now()
	svc := newFakeService()
	msgCh := make(chan kafka.Message)
	errCh := make(chan error)
	svc.followSession = &kafka.FollowSession{Messages: msgCh, Errors: errCh}

	m := messages.New(messages.Options{Service: svc, Topic: "orders", Now: func() time.Time { return now }})
	m.SetSize(80, 24)
	drive(t, m, m.Init())
	_ = m.Update(keyPressRune('s'))
	_ = m.Update(keyPressRune('7'))

	first := stripANSI(m.View())
	require.Contains(t, first, "waiting for new records")

	gen := m.FetchGen()
	for range 3 {
		_ = m.Update(messages.LiveTickMsg{Gen: gen})
	}
	second := stripANSI(m.View())

	// the placeholder text is unchanged but the leading spinner frame must differ.
	require.Contains(t, second, "waiting for new records")
	assert.NotEqual(t, first, second, "placeholder spinner must animate alongside the title")
}

func TestLive_PlaceholderShownWhenNoMessages(t *testing.T) {
	now := time.Now()
	svc := newFakeService()
	msgCh := make(chan kafka.Message)
	errCh := make(chan error)
	svc.followSession = &kafka.FollowSession{Messages: msgCh, Errors: errCh}

	m := messages.New(messages.Options{Service: svc, Topic: "orders", Now: func() time.Time { return now }})
	m.SetSize(80, 24)
	drive(t, m, m.Init())
	_ = m.Update(keyPressRune('s'))
	_ = m.Update(keyPressRune('7'))

	view := stripANSI(m.View())
	assert.Contains(t, view, "waiting for new records")
}

func TestSeek_LiveStartsFollow(t *testing.T) {
	now := time.Now()
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("old")}}
	msgCh := make(chan kafka.Message)
	errCh := make(chan error)
	svc.followSession = &kafka.FollowSession{Messages: msgCh, Errors: errCh}

	m := messages.New(messages.Options{Service: svc, Topic: "orders", Now: func() time.Time { return now }})
	drive(t, m, m.Init())

	_ = m.Update(keyPressRune('s'))
	cmd := m.Update(keyPressRune('7')) // live
	require.NotNil(t, cmd)

	// don't drive the follow-poll cmd — it would block on msgCh. Just check
	// that the seek state and follow session are wired up.
	assert.Equal(t, messages.SeekLive, m.SeekState().Mode)
	assert.True(t, m.Following())
}

// ----- bracket boundaries -----

func TestBracket_LiveFlipsToLatest(t *testing.T) {
	now := time.Now()
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("a")}}
	msgCh := make(chan kafka.Message)
	errCh := make(chan error)
	svc.followSession = &kafka.FollowSession{Messages: msgCh, Errors: errCh}

	m := messages.New(messages.Options{Service: svc, Topic: "orders", Now: func() time.Time { return now }})
	drive(t, m, m.Init())
	_ = m.Update(keyPressRune('s'))
	_ = m.Update(keyPressRune('7'))
	require.True(t, m.Following())

	// [ flips to latest synchronously and dispatches a fresh fetch.
	cmd := m.Update(keyPress("["))
	drive(t, m, cmd)

	assert.False(t, m.Following())
	assert.Equal(t, messages.SeekLatest, m.SeekState().Mode)
}

// ----- earlier/later still works -----

func TestEarlier_PagesOnlyAcrossSeenPartitions(t *testing.T) {
	// Regression: pressing `[`/`]` after an explicit-partition seek used
	// to pull in tails of unrelated partitions, because the screen passed
	// the global m.filter (often empty == "all partitions") into
	// FetchEarlier. The kafka layer would then load from watermark for
	// partitions with no baseline, blowing the user out of the focused
	// seek window into a global view.
	svc := newFakeService()
	svc.lastN = []kafka.Message{
		{Topic: "orders", Partition: 3, Offset: 500, Value: []byte("p3-500")},
		{Topic: "orders", Partition: 3, Offset: 501, Value: []byte("p3-501")},
	}
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())

	cmd := m.Update(keyPress("["))
	drive(t, m, cmd)

	// Only the partition the user has actually seen (3) must be requested.
	assert.Equal(t, []int32{3}, svc.lastPartitions, "paging must be scoped to seen partitions")
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
	assert.Equal(t, []byte("z"), m.Messages()[0].Value)

	svc.later = []kafka.Message{
		{Topic: "orders", Partition: 0, Offset: 102, Value: []byte("c")},
	}
	cmd = m.Update(keyPress("]"))
	drive(t, m, cmd)
	require.Equal(t, map[int32]int64{0: 101}, svc.lastLaterBaseline)
	assert.Equal(t, []byte("c"), m.Messages()[len(m.Messages())-1].Value)
}

// ----- produce/resend -----

func TestProduce_RaisesAction(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{{Topic: "orders", Value: []byte("v")}})
	_ = m.Update(keyPressRune('p'))
	a := m.ConsumeAction()
	assert.Equal(t, "orders", a.Produce)
	assert.Nil(t, a.PrefillFromMessage)
}

func TestResend_FromListRaisesActionWithMessage(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{
		{Topic: "orders", Offset: 5, Value: []byte("payload")},
	})
	_ = m.Update(keyPressRune('R'))
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

	_ = m.Update(keyPressRune('p'))
	assert.Empty(t, m.ConsumeAction().Produce)
	_ = m.Update(keyPressRune('R'))
	assert.Empty(t, m.ConsumeAction().Produce)
}

// ----- close / lifecycle -----

func TestClose_StopsFollowSession(t *testing.T) {
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Partition: 0, Offset: 1}}
	msgCh := make(chan kafka.Message)
	errCh := make(chan error)
	svc.followSession = &kafka.FollowSession{Messages: msgCh, Errors: errCh}

	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	drive(t, m, m.Init())
	_ = m.Update(keyPressRune('s'))
	_ = m.Update(keyPressRune('7'))
	require.True(t, m.Following())

	m.Close()
	assert.False(t, m.Following())
	m.Close()
}

// ----- persistence -----

func TestPersistence_WritesViewOnSeekChange(t *testing.T) {
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Value: []byte("v")}}
	repo := newFakeRepo()
	m := messages.New(messages.Options{Service: svc, Topic: "orders", Cluster: "c1", ViewState: repo})
	drive(t, m, m.Init())

	_ = m.Update(keyPressRune('s'))
	cmd := m.Update(keyPressRune('2')) // earliest
	drive(t, m, cmd)

	require.NotNil(t, repo.saved["c1\x00orders"])
	assert.Equal(t, messages.SeekEarliest, repo.saved["c1\x00orders"].SeekMode)
}

func TestPersistence_LiveSkipsWrite(t *testing.T) {
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Value: []byte("v")}}
	msgCh := make(chan kafka.Message)
	errCh := make(chan error)
	svc.followSession = &kafka.FollowSession{Messages: msgCh, Errors: errCh}

	repo := newFakeRepo()
	m := messages.New(messages.Options{Service: svc, Topic: "orders", Cluster: "c1", ViewState: repo})
	drive(t, m, m.Init())

	_ = m.Update(keyPressRune('s'))
	cmd := m.Update(keyPressRune('7')) // live
	require.NotNil(t, cmd)
	// live mode doesn't write; previous (initial latest) was never saved either.
	if v, ok := repo.saved["c1\x00orders"]; ok {
		assert.NotEqual(t, messages.SeekLive, v.SeekMode)
	}
}

func TestPersistence_RestoresOnInit(t *testing.T) {
	svc := newFakeService()
	svc.atTimestamp = []kafka.Message{{Topic: "orders", Value: []byte("from-restore")}}
	svc.watermarks = map[int32]kafka.PartitionWatermarks{
		0: {Low: 0, High: 100},
		2: {Low: 0, High: 50},
	}
	target := time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC)
	repo := newFakeRepo()
	repo.saved["c1\x00orders"] = messages.ViewState{
		SeekMode:   messages.SeekFromTimestamp,
		Timestamp:  target,
		Partitions: "0,2",
	}
	m := messages.New(messages.Options{Service: svc, Topic: "orders", Cluster: "c1", ViewState: repo})

	drive(t, m, m.Init())

	assert.Equal(t, messages.SeekFromTimestamp, m.SeekState().Mode)
	assert.Equal(t, target, m.SeekState().Timestamp)
	assert.Equal(t, []int32{0, 2}, m.PartitionFilter())
}

func TestPersistence_RestoreDropsDeadPartitions(t *testing.T) {
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Value: []byte("v")}}
	svc.watermarks = map[int32]kafka.PartitionWatermarks{
		0: {Low: 0, High: 10},
		// partition 5 used to exist but is gone now.
	}
	repo := newFakeRepo()
	repo.saved["c1\x00orders"] = messages.ViewState{
		SeekMode:   messages.SeekLatest,
		Partitions: "0,5",
	}
	m := messages.New(messages.Options{Service: svc, Topic: "orders", Cluster: "c1", ViewState: repo})

	drive(t, m, m.Init())

	assert.Equal(t, []int32{0}, m.PartitionFilter())
}

func TestPersistence_RestoreCancelsRacingLive(t *testing.T) {
	// race: between Init and the async viewRestoredMsg arriving, the user
	// triggers `s 7` (live). The restore must cancel that live before
	// applying the persisted state, otherwise a late FollowStartedMsg
	// attaches a session inconsistent with the restored seek mode.
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Value: []byte("v")}}
	svc.watermarks = map[int32]kafka.PartitionWatermarks{0: {Low: 0, High: 10}}
	msgCh := make(chan kafka.Message)
	errCh := make(chan error)
	svc.followSession = &kafka.FollowSession{Messages: msgCh, Errors: errCh}

	repo := newFakeRepo()
	repo.saved["c1\x00orders"] = messages.ViewState{SeekMode: messages.SeekLatest}

	m := messages.New(messages.Options{Service: svc, Topic: "orders", Cluster: "c1", ViewState: repo})

	// initiate restore but do not drive it yet — model is in initial state.
	initCmd := m.Init()

	// user races a live request in.
	_ = m.Update(keyPressRune('s'))
	_ = m.Update(keyPressRune('7'))
	require.True(t, m.Following(), "live must be active before restore arrives")

	// now deliver the restore — it should cancel the racing live.
	drive(t, m, initCmd)

	assert.False(t, m.Following(), "restore must cancel racing live")
	assert.Equal(t, messages.SeekLatest, m.SeekState().Mode)
}

func TestPersistence_RestoreClampsOffsetAboveLatest(t *testing.T) {
	svc := newFakeService()
	svc.atOffset = []kafka.Message{{Topic: "orders", Partition: 0, Offset: 9, Value: []byte("clamped")}}
	svc.watermarks = map[int32]kafka.PartitionWatermarks{
		0: {Low: 0, High: 10},
	}
	repo := newFakeRepo()
	repo.saved["c1\x00orders"] = messages.ViewState{
		SeekMode:  messages.SeekFromOffset,
		Partition: 0,
		Offset:    9_999_999,
		HasPart:   true,
	}
	m := messages.New(messages.Options{Service: svc, Topic: "orders", Cluster: "c1", ViewState: repo})

	drive(t, m, m.Init())

	// offset clamped against High-1 == 9.
	assert.Equal(t, int64(9), m.SeekState().Offset)
}

func TestPersistence_PartitionsSubmitWritesView(t *testing.T) {
	svc := newFakeService()
	svc.lastN = []kafka.Message{{Topic: "orders", Value: []byte("v")}}
	svc.watermarks = map[int32]kafka.PartitionWatermarks{
		0: {Low: 0, High: 5},
		1: {Low: 0, High: 5},
		2: {Low: 0, High: 5},
		3: {Low: 0, High: 5},
	}
	repo := newFakeRepo()
	m := messages.New(messages.Options{Service: svc, Topic: "orders", Cluster: "c1", ViewState: repo})
	drive(t, m, m.Init())

	cmd := m.Update(keyPressRune('P'))
	drive(t, m, cmd)
	_ = m.Update(keyPress("tab"))
	for _, r := range "0,3" {
		_ = m.Update(keyPressRune(r))
	}
	cmd = m.Update(keyPress("enter"))
	drive(t, m, cmd)

	require.NotNil(t, repo.saved["c1\x00orders"])
	assert.Equal(t, "0,3", repo.saved["c1\x00orders"].Partitions)
}

// ----- helpers -----

func TestFormatTimestamp_AlwaysIncludesFullDate(t *testing.T) {
	ts := time.Date(2026, 4, 28, 9, 30, 15, 250_000_000, time.UTC)
	wantSame := ts.Local().Format("2006-01-02 15:04:05.000")
	assert.Equal(t, wantSame, messages.FormatTimestamp(ts))
}

func TestFormatTimestamp_ZeroReturnsDash(t *testing.T) {
	assert.Equal(t, "—", messages.FormatTimestamp(time.Time{}))
}

func TestKeyHints_ContainsExpectedLabels(t *testing.T) {
	svc := newFakeService()
	m := messages.New(messages.Options{Service: svc, Topic: "orders"})
	got := strings.Join(keyHintLabels(m.KeyHints()), ",")
	assert.Contains(t, got, "detail")
	assert.Contains(t, got, "seek")
	assert.Contains(t, got, "partition")
	assert.Contains(t, got, "smart filter")
	assert.Contains(t, got, "previous page")
	assert.Contains(t, got, "next page")
	assert.Contains(t, got, "filter")
	assert.Contains(t, got, "produce")
	assert.Contains(t, got, "resend")
	assert.Contains(t, got, "refresh")
}

func TestKeyHints_ReadOnlyOmitsProduce(t *testing.T) {
	svc := newFakeService()
	m := messages.New(messages.Options{Service: svc, Topic: "orders", ReadOnly: true})
	joined := strings.Join(keyHintLabels(m.KeyHints()), ",")
	assert.NotContains(t, joined, "produce")
	assert.NotContains(t, joined, "resend")
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

func keyHintLabels(hints []layout.KeyHint) []string {
	out := make([]string, 0, len(hints))
	for _, h := range hints {
		out = append(out, h.Label)
	}
	return out
}

type offsetCall struct {
	partition int32
	offset    int64
}

type fakeService struct {
	mu            sync.Mutex
	lastN         []kafka.Message
	earliest      []kafka.Message
	earlier       []kafka.Message
	later         []kafka.Message
	atOffset      []kafka.Message
	atTimestamp   []kafka.Message
	watermarks    map[int32]kafka.PartitionWatermarks
	watermarksErr error
	offsetsForTs  map[int32]int64
	err           error

	lastPartitions      []int32
	lastEarlierBaseline map[int32]int64
	lastLaterBaseline   map[int32]int64
	lastOffsetPartition int32
	lastOffsetValue     int64
	atOffsetCalls       []offsetCall
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

func (f *fakeService) FetchEarliest(_ context.Context, _ string, _ int, parts []int32) ([]kafka.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastPartitions = append([]int32(nil), parts...)
	return append([]kafka.Message(nil), f.earliest...), f.err
}

func (f *fakeService) FetchAtOffset(_ context.Context, _ string, p int32, off int64, _ int) ([]kafka.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastOffsetPartition = p
	f.lastOffsetValue = off
	f.atOffsetCalls = append(f.atOffsetCalls, offsetCall{partition: p, offset: off})
	return append([]kafka.Message(nil), f.atOffset...), f.err
}

func (f *fakeService) FetchAtOffsets(_ context.Context, _ string, offsets map[int32]int64, _ int) ([]kafka.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// mirror the real client: clamp each requested offset against the
	// partition's watermarks before recording the call. This lets screen
	// tests assert that the right post-clamp offsets reach the kafka layer.
	for p, o := range offsets {
		clamped := o
		if w, ok := f.watermarks[p]; ok {
			if clamped < w.Low {
				clamped = w.Low
			}
			if clamped >= w.High {
				clamped = w.High - 1
			}
		}
		f.atOffsetCalls = append(f.atOffsetCalls, offsetCall{partition: p, offset: clamped})
	}
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

func (f *fakeService) WatermarksFor(_ context.Context, _ string, parts []int32) (map[int32]kafka.PartitionWatermarks, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.watermarksErr != nil {
		return nil, f.watermarksErr
	}
	if f.watermarks == nil {
		return map[int32]kafka.PartitionWatermarks{}, nil
	}
	if len(parts) == 0 {
		out := map[int32]kafka.PartitionWatermarks{}
		maps.Copy(out, f.watermarks)
		return out, nil
	}
	out := map[int32]kafka.PartitionWatermarks{}
	for _, p := range parts {
		if w, ok := f.watermarks[p]; ok {
			out[p] = w
		}
	}
	return out, nil
}

func (f *fakeService) OffsetsForTimestamp(_ context.Context, _ string, ts time.Time, parts []int32) (map[int32]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastTimestamp = ts
	f.lastPartitions = append([]int32(nil), parts...)
	if f.offsetsForTs == nil {
		return map[int32]int64{}, nil
	}
	out := map[int32]int64{}
	maps.Copy(out, f.offsetsForTs)
	return out, nil
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

type fakeRepo struct {
	mu    sync.Mutex
	saved map[string]messages.ViewState
}

func newFakeRepo() *fakeRepo { return &fakeRepo{saved: map[string]messages.ViewState{}} }

func (f *fakeRepo) LoadMessagesView(_ context.Context, cluster, topic string) (messages.ViewState, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.saved[cluster+"\x00"+topic]
	return v, ok, nil
}

func (f *fakeRepo) SaveMessagesView(_ context.Context, cluster, topic string, view messages.ViewState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saved[cluster+"\x00"+topic] = view
	return nil
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

func (f *fakePager) Open(path string) tea.Cmd {
	f.calls++
	return func() tea.Msg { return messages.EditorOpenedMsg{Path: path, Err: f.err} }
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

// ----- detail tests (preserved from previous suite) -----

func newDetail(t *testing.T, cb messages.Clipboard, msgs []kafka.Message, idx int) *messages.DetailModel {
	t.Helper()
	return messages.NewDetailModel(messages.DetailOptions{
		Messages:  msgs,
		Index:     idx,
		Clipboard: cb,
	})
}

func TestDetail_NextPrevNavigatesMessages(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{
		{Topic: "orders", Offset: 1, Value: []byte("a")},
		{Topic: "orders", Offset: 2, Value: []byte("b")},
		{Topic: "orders", Offset: 3, Value: []byte("c")},
	})
	_ = m.Update(keyPress("enter"))
	require.Equal(t, 0, m.Detail().Index())

	_ = m.Update(keyPressRune('n'))
	assert.Equal(t, 1, m.Detail().Index())
	_ = m.Update(keyPressRune('n'))
	assert.Equal(t, 2, m.Detail().Index())
	_ = m.Update(keyPressRune('n'))
	assert.Equal(t, 2, m.Detail().Index())
	_ = m.Update(keyPressRune('p'))
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

	_, _ = model.Update(keyPressRune('y'))

	require.Len(t, cb.payloads, 1)
	assert.Contains(t, cb.payloads[0], `"topic": "orders"`)
}

func TestDetail_SaveValueWritesFile(t *testing.T) {
	fw := &fakeWriter{}
	model := messages.NewDetailModel(messages.DetailOptions{
		Messages:   []kafka.Message{{Topic: "orders", Partition: 1, Offset: 7, Value: []byte("hello")}},
		Index:      0,
		FileWriter: fw,
		OutputDir:  "/tmp",
	})

	_, _ = model.Update(keyPressRune('s'))

	require.Len(t, fw.writes, 1)
	assert.Equal(t, "/tmp/orders-p1-o7-value.txt", fw.writes[0].path)
}

func TestDetail_OpenEditorInvokesPager(t *testing.T) {
	pager := &fakePager{}
	model := messages.NewDetailModel(messages.DetailOptions{
		Messages: []kafka.Message{{Topic: "orders", Value: []byte("hello")}},
		Index:    0,
		Pager:    pager,
	})
	_, _ = model.Update(keyPressRune('e'))
	assert.Equal(t, 1, pager.calls)
}

func TestDetail_ResendRaisesProduceWithMessage(t *testing.T) {
	model := messages.NewDetailModel(messages.DetailOptions{
		Messages: []kafka.Message{{Topic: "orders", Value: []byte("hello")}},
		Index:    0,
	})
	_, _ = model.Update(keyPressRune('R'))
	a := model.ConsumeAction()
	assert.Equal(t, "orders", a.Produce)
	require.NotNil(t, a.PrefillFromMessage)
}

func TestDetailEsc_ReturnsToList(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{
		{Topic: "orders", Value: []byte("hello")},
	})
	_ = m.Update(keyPress("enter"))
	_ = m.Update(keyPress("esc"))
	assert.Equal(t, messages.ModeList, m.CurrentMode())
}

func TestMessages_BreadcrumbTracksDetailNavigation(t *testing.T) {
	m := buildModelWithMessages(t, []kafka.Message{
		{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("a")},
		{Topic: "orders", Partition: 2, Offset: 42, Value: []byte("b")},
	})
	m.SetSize(80, 20)
	_ = m.Update(keyPress("enter"))
	require.Equal(t, "msg-0-1", m.Breadcrumb())
	_ = m.Update(keyPressRune('n'))
	assert.Equal(t, "msg-2-42", m.Breadcrumb())
}

// stripANSI removes terminal escape sequences so tests can compare plain text.
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
	m := openDetailWithLarge(t, 12, 200)
	d := m.Detail()
	require.Equal(t, 0, d.ScrollOffset())

	_ = m.Update(keyPressRune('j'))
	assert.Equal(t, 1, d.ScrollOffset())
	_ = m.Update(keyPressRune('k'))
	assert.Equal(t, 0, d.ScrollOffset())
	_ = m.Update(keyPressRune('G'))
	_, last, total, ok := d.ScrollSummary()
	require.True(t, ok)
	assert.Equal(t, total, last)
	_ = m.Update(keyPressRune('g'))
	_ = m.Update(keyPressRune('g'))
	assert.Equal(t, 0, d.ScrollOffset())
}
