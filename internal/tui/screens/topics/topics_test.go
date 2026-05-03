package topics_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/topics"
)

// drive runs cmd to completion synchronously and routes any resulting
// messages back through the Model, mirroring how the Bubble Tea program
// dispatches cmds in production.
func drive(t *testing.T, m *topics.Model, cmd tea.Cmd) {
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

func TestNew_DefaultColumns(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.New(topics.Options{Service: svc})
	out := m.View()
	assert.Contains(t, out, "Name")
	assert.Contains(t, out, "Partitions")
	assert.Contains(t, out, "Replicas")
	assert.Contains(t, out, "Cleanup")
	assert.Contains(t, out, "Messages")
	assert.Contains(t, out, "Size")
}

func TestInit_LoadsTopicsAndShowsCounter(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{
		{Name: "alpha", Partitions: 1, Replicas: 1},
		{Name: "beta", Partitions: 3, Replicas: 1},
	}, nil)
	m := topics.New(topics.Options{Service: svc})

	drive(t, m, m.Init())

	assert.Len(t, m.AllTopics(), 2)
	out := m.View()
	// the count moved to the frame title, the body only renders the table.
	assert.Contains(t, m.Title(), "Topics[2]")
	assert.Contains(t, out, "alpha")
	assert.Contains(t, out, "beta")
}

func TestInit_ErrorRaisesToast(t *testing.T) {
	svc := newFakeService(nil, errors.New("connection refused"))
	m := topics.New(topics.Options{Service: svc})

	drive(t, m, m.Init())

	require.GreaterOrEqual(t, m.Toasts().Len(), 1)
	assert.Contains(t, m.Toasts().Items()[m.Toasts().Len()-1].Message, "connection refused")
}

func TestInternalToggle_HidesAndShowsInternalTopics(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{
		{Name: "user_events"},
		{Name: "__consumer_offsets", IsInternal: true},
		{Name: "__transaction_state", IsInternal: true},
	}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	// internal hidden by default
	assert.False(t, m.ShowInternal())
	visible := m.Topics()
	require.Len(t, visible, 1)
	assert.Equal(t, "user_events", visible[0].Name)
	assert.Equal(t, 2, m.HiddenInternalCount())

	// toggle: now visible
	_, _ = m.Update(keyPress("i"))
	assert.True(t, m.ShowInternal())
	visible = m.Topics()
	assert.Len(t, visible, 3)
	out := m.View()
	assert.Contains(t, out, "__consumer_offsets")
}

func TestTitle_ShowsHiddenCount(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{
		{Name: "topic-1"},
		{Name: "__consumer_offsets", IsInternal: true},
		{Name: "__transaction_state", IsInternal: true},
		{Name: "__schemas", IsInternal: true},
	}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())
	// hidden-internal counter lives in the frame title now.
	assert.Contains(t, m.Title(), "Topics[1, +3 internal hidden]")
}

func TestEnter_RaisesMessagesAction(t *testing.T) {
	m := buildModelWith(t, []kafka.TopicSummary{
		{Name: "alpha"}, {Name: "beta"},
	})
	_, _ = m.Update(keyPress("enter"))
	assert.Equal(t, "alpha", m.ConsumeAction().Messages)
}

func TestM_AlsoOpensMessages(t *testing.T) {
	m := buildModelWith(t, []kafka.TopicSummary{{Name: "alpha"}})
	_, _ = m.Update(keyPress("m"))
	assert.Equal(t, "alpha", m.ConsumeAction().Messages)
}

func TestC_RaisesConfigsAction(t *testing.T) {
	m := buildModelWith(t, []kafka.TopicSummary{{Name: "alpha"}})
	_, _ = m.Update(keyPress("c"))
	assert.Equal(t, "alpha", m.ConsumeAction().Configs)
}

func TestG_RaisesGroupsAction(t *testing.T) {
	m := buildModelWith(t, []kafka.TopicSummary{{Name: "alpha"}})
	_, _ = m.Update(keyPress("g"))
	assert.Equal(t, "alpha", m.ConsumeAction().Groups)
}

func TestP_RaisesProduceAction(t *testing.T) {
	m := buildModelWith(t, []kafka.TopicSummary{{Name: "alpha"}})
	_, _ = m.Update(keyPress("p"))
	assert.Equal(t, "alpha", m.ConsumeAction().Produce)
}

func TestEsc_RaisesQuit(t *testing.T) {
	m := buildModelWith(t, []kafka.TopicSummary{{Name: "alpha"}})
	_, _ = m.Update(keyPress("esc"))
	assert.True(t, m.ConsumeAction().Quit)
}

func TestReadOnly_BlocksMutatingHotkeys(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{{Name: "alpha"}}, nil)
	m := topics.New(topics.Options{Service: svc, ReadOnly: true})
	drive(t, m, m.Init())

	for _, k := range []string{"n", "D", "y", "p"} {
		_, _ = m.Update(keyPress(k))
		assert.Empty(t, m.ConsumeAction().Produce, "p must not raise produce in RO")
		assert.False(t, m.ConfirmOpen(), "D must not open confirm in RO")
		assert.Equal(t, topics.ModeList, m.CurrentMode(), "n/y must not enter overlay in RO")
		// each blocked key produces a warning toast
	}
	// at least one warning toast surfaced
	assert.GreaterOrEqual(t, m.Toasts().Len(), 1)
}

func TestDelete_ConfirmYesTriggersDeleteCmd(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{{Name: "alpha"}, {Name: "beta"}}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	_, _ = m.Update(keyPress("D"))
	assert.True(t, m.ConfirmOpen())
	assert.Equal(t, "alpha", m.PendingTopic())

	_, cmd := m.Update(keyPress("y"))
	drive(t, m, cmd)
	assert.False(t, m.ConfirmOpen())
	assert.Equal(t, []string{"alpha"}, svc.Deleted())
}

func TestDelete_ConfirmNoCancels(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{{Name: "alpha"}}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	_, _ = m.Update(keyPress("D"))
	require.True(t, m.ConfirmOpen())

	_, _ = m.Update(keyPress("n"))
	assert.False(t, m.ConfirmOpen())
	assert.Empty(t, svc.Deleted())
}

func TestN_OpensCreateForm(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{{Name: "alpha"}}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	_, _ = m.Update(keyPress("n"))
	assert.Equal(t, topics.ModeCreate, m.CurrentMode())
	out := m.View()
	assert.Contains(t, out, "New topic")
	assert.Contains(t, out, "ctrl+s create")
}

func TestCreateForm_EscReturnsToList(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{{Name: "alpha"}}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	_, _ = m.Update(keyPress("n"))
	require.Equal(t, topics.ModeCreate, m.CurrentMode())
	_, _ = m.Update(keyPress("esc"))
	assert.Equal(t, topics.ModeList, m.CurrentMode())
}

func TestWantsRawInput_TracksFormModes(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{{Name: "alpha"}}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	assert.False(t, m.WantsRawInput(), "list mode does not edit text")

	_, _ = m.Update(keyPress("n"))
	require.Equal(t, topics.ModeCreate, m.CurrentMode())
	assert.True(t, m.WantsRawInput(), "create form edits text")

	_, _ = m.Update(keyPress("esc"))
	assert.False(t, m.WantsRawInput())

	_, _ = m.Update(keyPress("y"))
	require.Equal(t, topics.ModeClone, m.CurrentMode())
	assert.True(t, m.WantsRawInput(), "clone form edits text")
}

func TestCreateForm_CtrlSValidatesAndDispatches(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	_, _ = m.Update(keyPress("n"))

	// form opens in NORMAL — press enter to start typing into the focused
	// `name` field, then fill it in.
	_, _ = m.Update(keyPress("enter"))
	for _, r := range "orders" {
		_, _ = m.Update(keyPressRune(r))
	}
	_, cmd := m.Update(keyPress("ctrl+s"))
	drive(t, m, cmd)

	created := svc.Created()
	require.Len(t, created, 1)
	assert.Equal(t, "orders", created[0].Name)
	assert.Equal(t, int32(1), created[0].Partitions)
	assert.Equal(t, int16(1), created[0].ReplicationFactor)
}

func TestCreateForm_CtrlSWithEmptyNameShowsInlineError(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	_, _ = m.Update(keyPress("n"))
	// no name typed
	_, cmd := m.Update(keyPress("ctrl+s"))
	drive(t, m, cmd)

	require.Equal(t, topics.ModeCreate, m.CurrentMode(), "stay on form when invalid")
	assert.Contains(t, m.CreateForm().Err(), "name")
	assert.Empty(t, svc.Created())
}

func TestY_OpensCloneForm(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{{Name: "alpha"}, {Name: "beta"}}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	_, _ = m.Update(keyPress("y"))
	assert.Equal(t, topics.ModeClone, m.CurrentMode())
	out := m.View()
	assert.Contains(t, out, "Clone topic")
	assert.Contains(t, out, "alpha")
}

func TestCloneForm_CtrlSStartsCloneAndShowsProgress(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{{Name: "alpha"}, {Name: "beta"}}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	_, _ = m.Update(keyPress("y"))
	require.Equal(t, topics.ModeClone, m.CurrentMode())

	_, cmd := m.Update(keyPress("ctrl+s"))
	drive(t, m, cmd)

	cloned := svc.Cloned()
	require.Len(t, cloned, 1)
	assert.Equal(t, "alpha", cloned[0].src)
	assert.Equal(t, "alpha-clone", cloned[0].dst)
	// after the clone progress channel closes, mode returns to list
	assert.Equal(t, topics.ModeList, m.CurrentMode())
}

func TestRefresh_RKeyReloadsTopics(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{{Name: "alpha"}}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())
	require.Equal(t, 1, svc.ListCalls())

	_, cmd := m.Update(keyPress("r"))
	drive(t, m, cmd)
	assert.Equal(t, 2, svc.ListCalls())
}

func TestSearch_FiltersTable(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{
		{Name: "orders"},
		{Name: "events"},
		{Name: "order-history"},
	}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	_, _ = m.Update(keyPress("/"))
	for _, r := range "order" {
		_, _ = m.Update(keyPressRune(r))
	}
	_, _ = m.Update(keyPress("enter"))
	out := m.View()
	assert.Contains(t, out, "orders")
	assert.Contains(t, out, "order-history")
	assert.NotContains(t, out, "events")
}

func TestKeyHints_ContainExpectedLabels(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.New(topics.Options{Service: svc})
	hints := m.KeyHints()
	labels := make([]string, 0, len(hints))
	for _, h := range hints {
		labels = append(labels, h.Label)
	}
	got := strings.Join(labels, ",")
	assert.Contains(t, got, "messages")
	assert.Contains(t, got, "configs")
	assert.Contains(t, got, "groups")
	assert.Contains(t, got, "new")
	assert.Contains(t, got, "delete")
	assert.Contains(t, got, "clone")
	assert.Contains(t, got, "produce")
}

func TestKeyHints_OmitMutatingLabelsInReadOnly(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.New(topics.Options{Service: svc, ReadOnly: true})
	labels := []string{}
	for _, h := range m.KeyHints() {
		labels = append(labels, h.Label)
	}
	got := strings.Join(labels, ",")
	assert.NotContains(t, got, "new")
	assert.NotContains(t, got, "delete")
	assert.NotContains(t, got, "clone")
	assert.NotContains(t, got, "produce")
}

func TestColumnConfiguration_RespectsConfig(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{
		{Name: "alpha", Partitions: 5, Replicas: 1},
	}, nil)
	m := topics.New(topics.Options{
		Service: svc,
		Columns: []string{"name", "partitions"},
	})
	drive(t, m, m.Init())
	out := m.View()
	assert.Contains(t, out, "Name")
	assert.Contains(t, out, "Partitions")
	assert.NotContains(t, out, "Replicas")
}

func TestSort_SCyclesSortOnCurrentColumn(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{
		{Name: "zeta"},
		{Name: "alpha"},
		{Name: "mu"},
	}, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())

	_, _ = m.Update(keyPress("s")) // first s engages asc on first sortable col (name)
	out := m.View()
	idxAlpha := strings.Index(out, "alpha")
	idxZeta := strings.Index(out, "zeta")
	require.GreaterOrEqual(t, idxAlpha, 0)
	require.GreaterOrEqual(t, idxZeta, 0)
	assert.Less(t, idxAlpha, idxZeta, "ascending sort puts alpha first")
}

func TestRefreshInterval_AutoRefreshTickIssuesCommand(t *testing.T) {
	svc := newFakeService([]kafka.TopicSummary{{Name: "alpha"}}, nil)
	m := topics.New(topics.Options{Service: svc, RefreshInterval: 10})
	cmd := m.AutoRefreshTick()
	require.NotNil(t, cmd, "refresh interval > 0 must yield a tick cmd")
}

func TestRefreshInterval_OffYieldsNilCmd(t *testing.T) {
	svc := newFakeService(nil, nil)
	m := topics.New(topics.Options{Service: svc})
	assert.Nil(t, m.AutoRefreshTick())
}

func TestFocusTopic_CursorRestoredAfterLoad(t *testing.T) {
	// arrange
	svc := newFakeService([]kafka.TopicSummary{
		{Name: "alpha"},
		{Name: "beta"},
		{Name: "gamma"},
	}, nil)
	m := topics.New(topics.Options{
		Service:    svc,
		FocusTopic: "gamma",
	})

	// act
	drive(t, m, m.Init())

	// assert
	visible := m.Topics()
	require.Len(t, visible, 3)
	assert.Equal(t, 2, m.Cursor())
}

func TestFocusTopic_UnknownTopic_CursorStaysAtZero(t *testing.T) {
	// arrange
	svc := newFakeService([]kafka.TopicSummary{
		{Name: "alpha"},
		{Name: "beta"},
	}, nil)
	m := topics.New(topics.Options{
		Service:    svc,
		FocusTopic: "nonexistent",
	})

	// act
	drive(t, m, m.Init())

	// assert
	assert.Equal(t, 0, m.Cursor())
}

func TestFilterTopics_ShowsOnlyMatchingTopics(t *testing.T) {
	// arrange
	svc := newFakeService([]kafka.TopicSummary{
		{Name: "orders"},
		{Name: "payments"},
		{Name: "users"},
		{Name: "logs"},
	}, nil)
	m := topics.New(topics.Options{
		Service:      svc,
		FilterTopics: []string{"orders", "payments"},
	})

	// act
	drive(t, m, m.Init())

	// assert
	visible := m.Topics()
	assert.Equal(t, []kafka.TopicSummary{
		{Name: "orders"},
		{Name: "payments"},
	}, visible)
	assert.Len(t, m.AllTopics(), 4)
}

func TestFilterTopics_EmptyFilter_ShowsAll(t *testing.T) {
	// arrange
	svc := newFakeService([]kafka.TopicSummary{
		{Name: "alpha"},
		{Name: "beta"},
	}, nil)
	m := topics.New(topics.Options{Service: svc})

	// act
	drive(t, m, m.Init())

	// assert
	visible := m.Topics()
	assert.Equal(t, []kafka.TopicSummary{
		{Name: "alpha"},
		{Name: "beta"},
	}, visible)
}

func TestFilterTopics_CombinesWithInternalToggle(t *testing.T) {
	// arrange
	svc := newFakeService([]kafka.TopicSummary{
		{Name: "orders"},
		{Name: "__consumer_offsets", IsInternal: true},
		{Name: "payments"},
		{Name: "users"},
	}, nil)
	m := topics.New(topics.Options{
		Service:      svc,
		FilterTopics: []string{"orders", "__consumer_offsets", "payments"},
	})

	// act
	drive(t, m, m.Init())

	// assert — internal hidden by default even if in filter
	visible := m.Topics()
	assert.Equal(t, []kafka.TopicSummary{
		{Name: "orders"},
		{Name: "payments"},
	}, visible)

	// toggle internal on
	_, _ = m.Update(keyPress("i"))
	visible = m.Topics()
	assert.Equal(t, []kafka.TopicSummary{
		{Name: "orders"},
		{Name: "__consumer_offsets", IsInternal: true},
		{Name: "payments"},
	}, visible)
}

// ----- helpers -----

func buildModelWith(t *testing.T, ts []kafka.TopicSummary) *topics.Model {
	t.Helper()
	svc := newFakeService(ts, nil)
	m := topics.New(topics.Options{Service: svc})
	drive(t, m, m.Init())
	return m
}

type clonedPair struct {
	src, dst string
}

type fakeService struct {
	mu       sync.Mutex
	topics   []kafka.TopicSummary
	listErr  error
	listN    int
	deleted  []string
	created  []kafka.CreateTopicSpec
	cloned   []clonedPair
	configs  map[string][]kafka.TopicConfig
	parts    map[string][]kafka.PartitionDetail
	cloneErr error
}

func newFakeService(topicsList []kafka.TopicSummary, listErr error) *fakeService {
	return &fakeService{
		topics:  append([]kafka.TopicSummary(nil), topicsList...),
		listErr: listErr,
		configs: map[string][]kafka.TopicConfig{},
		parts:   map[string][]kafka.PartitionDetail{},
	}
}

func (f *fakeService) ListCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listN
}

func (f *fakeService) Deleted() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deleted...)
}

func (f *fakeService) Created() []kafka.CreateTopicSpec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]kafka.CreateTopicSpec(nil), f.created...)
}

func (f *fakeService) Cloned() []clonedPair {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]clonedPair(nil), f.cloned...)
}

func (f *fakeService) ListTopics(_ context.Context) ([]kafka.TopicSummary, error) {
	f.mu.Lock()
	f.listN++
	out := append([]kafka.TopicSummary(nil), f.topics...)
	err := f.listErr
	f.mu.Unlock()
	return out, err
}

func (f *fakeService) TopicWatermarks(_ context.Context, _ string) (kafka.TopicWatermarks, error) {
	return kafka.TopicWatermarks{}, nil
}

func (f *fakeService) TopicSize(_ context.Context, _ string) (int64, error) { return 0, nil }

func (f *fakeService) DescribeTopicConfigs(_ context.Context, topic string) ([]kafka.TopicConfig, error) {
	return f.configs[topic], nil
}

func (f *fakeService) DescribeAllTopicConfigs(_ context.Context, topic string) ([]kafka.TopicConfig, error) {
	return f.configs[topic], nil
}

func (f *fakeService) TopicWatermarksBatch(_ context.Context, names ...string) (map[string]kafka.TopicWatermarks, error) {
	out := make(map[string]kafka.TopicWatermarks, len(names))
	for _, t := range names {
		out[t] = kafka.TopicWatermarks{}
	}
	return out, nil
}

func (f *fakeService) TopicSizesBatch(_ context.Context, names ...string) (map[string]int64, error) {
	out := make(map[string]int64, len(names))
	for _, t := range names {
		out[t] = 0
	}
	return out, nil
}

func (f *fakeService) DescribeTopicConfigsBatch(_ context.Context, names ...string) (map[string][]kafka.TopicConfig, error) {
	out := make(map[string][]kafka.TopicConfig, len(names))
	for _, t := range names {
		out[t] = f.configs[t]
	}
	return out, nil
}

func (f *fakeService) TopicPartitions(_ context.Context, topic string) ([]kafka.PartitionDetail, error) {
	return f.parts[topic], nil
}

func (f *fakeService) CreateTopic(_ context.Context, spec kafka.CreateTopicSpec) error {
	f.mu.Lock()
	f.created = append(f.created, spec)
	f.topics = append(f.topics, kafka.TopicSummary{
		Name:       spec.Name,
		Partitions: int(spec.Partitions),
		Replicas:   int(spec.ReplicationFactor),
	})
	f.mu.Unlock()
	return nil
}

func (f *fakeService) DeleteTopic(_ context.Context, topic string) error {
	f.mu.Lock()
	f.deleted = append(f.deleted, topic)
	out := f.topics[:0]
	for _, t := range f.topics {
		if t.Name != topic {
			out = append(out, t)
		}
	}
	f.topics = out
	f.mu.Unlock()
	return nil
}

func (f *fakeService) CloneTopic(_ context.Context, src, dst string, _ kafka.CloneOptions) (<-chan kafka.CloneProgress, error) {
	f.mu.Lock()
	f.cloned = append(f.cloned, clonedPair{src: src, dst: dst})
	err := f.cloneErr
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	ch := make(chan kafka.CloneProgress, 1)
	ch <- kafka.CloneProgress{Total: 0, Copied: 0, Done: true}
	close(ch)
	return ch, nil
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
	case "ctrl+s":
		return tea.KeyPressMsg{Code: 's', Mod: tea.ModCtrl}
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
