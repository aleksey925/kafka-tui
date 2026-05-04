package groups_test

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
	"github.com/aleksey925/kafka-tui/internal/tui/screens/groups"
)

// drive runs cmd to completion synchronously and routes any resulting
// messages back through the Model, mirroring how the Bubble Tea program
// dispatches cmds in production.
func drive(t *testing.T, m *groups.Model, cmd tea.Cmd) {
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
	svc := newFakeService()
	m := groups.New(groups.Options{Service: svc})
	out := m.View()
	assert.Contains(t, out, "Group")
	assert.Contains(t, out, "State")
	assert.Contains(t, out, "Members")
	assert.Contains(t, out, "Total Lag")
	assert.Contains(t, out, "Coordinator")
}

func TestInit_LoadsGroupsAndShowsCounter(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{
		{Group: "g1", State: "Stable", Coordinator: 1},
		{Group: "g2", State: "Empty", Coordinator: 2},
	}
	m := groups.New(groups.Options{Service: svc})

	drive(t, m, m.Init())

	assert.Len(t, m.Groups(), 2)
	assert.Contains(t, m.Title(), "Consumer Groups [2]")
	out := m.View()
	assert.Contains(t, out, "g1")
	assert.Contains(t, out, "g2")
}

func TestInit_FilterTopicShowsHeader(t *testing.T) {
	svc := newFakeService()
	svc.filteredGroups = map[string][]kafka.GroupListInfo{
		"orders": {{Group: "g-orders", State: "Stable"}},
	}
	m := groups.New(groups.Options{Service: svc, FilterTopic: "orders"})

	drive(t, m, m.Init())

	assert.Contains(t, m.Title(), "Consumer Groups · orders")
	assert.Contains(t, m.View(), "g-orders")
	assert.Equal(t, "orders", m.FilterTopic())
}

func TestInit_ErrorRaisesToast(t *testing.T) {
	svc := newFakeService()
	svc.listErr = errors.New("connection refused")
	m := groups.New(groups.Options{Service: svc})

	drive(t, m, m.Init())

	require.GreaterOrEqual(t, m.Toasts().Len(), 1)
	assert.Contains(t, m.Toasts().Items()[m.Toasts().Len()-1].Message, "connection refused")
}

func TestEnter_OpensDetail(t *testing.T) {
	m := buildModelWith(t, []kafka.GroupListInfo{{Group: "g1", State: "Empty"}})
	_, cmd := m.Update(keyPress("enter"))
	assert.Equal(t, groups.ModeDetail, m.CurrentMode())
	assert.NotNil(t, m.Detail())
	drive(t, m, cmd)
	d := m.Detail()
	require.NotNil(t, d)
	assert.Equal(t, "g1", d.Group())
}

func TestEsc_RaisesBackOnList(t *testing.T) {
	m := buildModelWith(t, []kafka.GroupListInfo{{Group: "g1"}})
	_, _ = m.Update(keyPress("esc"))
	assert.True(t, m.ConsumeAction().Back)
}

func TestRefresh_RKeyReloadsGroups(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g1"}}
	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())
	require.Equal(t, 1, svc.ListCalls())

	_, cmd := m.Update(keyPress("r"))
	drive(t, m, cmd)
	assert.Equal(t, 2, svc.ListCalls())
}

func TestReadOnly_BlocksDestructiveHotkeys(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g1", State: "Empty"}}
	m := groups.New(groups.Options{Service: svc, ReadOnly: true})
	drive(t, m, m.Init())

	for _, k := range []string{"R", "shift+r", "D"} {
		_, _ = m.Update(keyPress(k))
		assert.False(t, m.ConfirmOpen(), "%s must not open confirm in RO", k)
		assert.NotEqual(t, groups.ModeReset, m.CurrentMode(), "%s must not enter reset in RO", k)
	}
	assert.GreaterOrEqual(t, m.Toasts().Len(), 1)
}

func TestDelete_ConfirmYesTriggersDelete(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g1", State: "Empty"}}
	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())

	_, _ = m.Update(keyPress("D"))
	require.True(t, m.ConfirmOpen())
	assert.Equal(t, "g1", m.PendingGroup())

	_, cmd := m.Update(keyPress("y"))
	drive(t, m, cmd)
	assert.False(t, m.ConfirmOpen())
	assert.Equal(t, []string{"g1"}, svc.Deleted())
}

func TestDelete_NonEmptyShowsError(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g1", State: "Stable"}}
	svc.deleteErr = kafka.ErrNonEmptyGroup
	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())

	_, _ = m.Update(keyPress("D"))
	_, cmd := m.Update(keyPress("y"))
	drive(t, m, cmd)

	// the delete-group command should have been invoked
	assert.Equal(t, []string{"g1"}, svc.Deleted())
	// and a non-empty error toast should be present
	assert.True(t, hasToast(m, "non-empty"))
}

func TestDelete_ConfirmNoCancels(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g1"}}
	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())

	_, _ = m.Update(keyPress("D"))
	require.True(t, m.ConfirmOpen())
	_, _ = m.Update(keyPress("n"))
	assert.False(t, m.ConfirmOpen())
	assert.Empty(t, svc.Deleted())
}

func TestKeyHints_ContainExpectedLabels(t *testing.T) {
	svc := newFakeService()
	m := groups.New(groups.Options{Service: svc})
	hints := m.KeyHints()
	labels := make([]string, 0, len(hints))
	for _, h := range hints {
		labels = append(labels, h.Label)
	}
	got := strings.Join(labels, ",")
	assert.Contains(t, got, "detail")
	assert.Contains(t, got, "reset")
	assert.Contains(t, got, "express")
	assert.Contains(t, got, "delete")
	assert.Contains(t, got, "search")
}

func TestKeyHints_OmitDestructiveInReadOnly(t *testing.T) {
	svc := newFakeService()
	m := groups.New(groups.Options{Service: svc, ReadOnly: true})
	labels := []string{}
	for _, h := range m.KeyHints() {
		labels = append(labels, h.Label)
	}
	got := strings.Join(labels, ",")
	assert.NotContains(t, got, "reset")
	assert.NotContains(t, got, "express")
	assert.NotContains(t, got, "delete")
}

func TestRefreshInterval_AutoRefreshTickIssuesCommand(t *testing.T) {
	svc := newFakeService()
	m := groups.New(groups.Options{Service: svc, ListRefreshInterval: 10 * time.Second})
	cmd := m.AutoRefreshTick()
	require.NotNil(t, cmd)
}

func TestRefreshInterval_OffYieldsNilCmd(t *testing.T) {
	svc := newFakeService()
	m := groups.New(groups.Options{Service: svc})
	assert.Nil(t, m.AutoRefreshTick())
}

func TestFetchLagsForVisible_PopulatesLagAndMemberCount(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g1"}}
	svc.offsets = map[string][]kafka.PartitionLag{
		"g1": {
			{Topic: "t", Partition: 0, Committed: 50, End: 100, Lag: 50, MemberID: "m1"},
			{Topic: "t", Partition: 1, Committed: 90, End: 100, Lag: 10, MemberID: "m1"},
		},
	}
	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())
	drive(t, m, m.FetchLagsForVisible())

	out := m.View()
	assert.Contains(t, out, "60") // total lag rendered with thousands separators
}

// ----- detail view tests -----

func TestDetail_LoadsAndShowsRows(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g1"}}
	svc.descriptions = map[string]kafka.GroupDescription{
		"g1": {
			Group:           "g1",
			State:           "Stable",
			Protocol:        "range",
			CoordinatorID:   3,
			CoordinatorHost: "broker",
			CoordinatorPort: 9092,
			Members: []kafka.GroupMember{
				{MemberID: "m1", ClientID: "c1"},
			},
		},
	}
	svc.offsets = map[string][]kafka.PartitionLag{
		"g1": {
			{Topic: "alpha", Partition: 0, Committed: 50, End: 100, Lag: 50, MemberID: "m1"},
		},
	}
	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())
	_, cmd := m.Update(keyPress("enter"))
	drive(t, m, cmd)

	d := m.Detail()
	require.NotNil(t, d)
	rows := d.Rows()
	require.Len(t, rows, 1)
	assert.Equal(t, "alpha", rows[0].Topic)
	out := d.View()
	assert.Contains(t, out, "Group · g1")
	assert.Contains(t, out, "alpha")
	assert.Contains(t, out, "broker:9092")
}

func TestDetail_TabTogglesSortMode(t *testing.T) {
	d := newDetailWithRows(t, []kafka.PartitionLag{
		{Topic: "a", Partition: 0, Lag: 5, MemberID: "m"},
		{Topic: "b", Partition: 0, Lag: 100, MemberID: "m"},
	})
	require.Equal(t, groups.SortGrouped, d.SortMode())
	d, _ = d.Update(keyPress("tab"))
	assert.Equal(t, groups.SortFlat, d.SortMode())
	d, _ = d.Update(keyPress("tab"))
	assert.Equal(t, groups.SortGrouped, d.SortMode())
}

func TestDetail_T_OneTopicJumpsToMessages(t *testing.T) {
	d := newDetailWithRows(t, []kafka.PartitionLag{
		{Topic: "alpha", Partition: 0, Lag: 1, MemberID: "m"},
	})
	d, _ = d.Update(keyPress("t"))
	a := d.ConsumeAction()
	assert.Equal(t, "alpha", a.Topic)
}

func TestDetail_T_MultipleTopicsRaisesTopicsList(t *testing.T) {
	d := newDetailWithRows(t, []kafka.PartitionLag{
		{Topic: "alpha", Partition: 0, MemberID: "m"},
		{Topic: "beta", Partition: 0, MemberID: "m"},
	})
	d, _ = d.Update(keyPress("t"))
	a := d.ConsumeAction()
	assert.Equal(t, []string{"alpha", "beta"}, a.TopicsForGroup)
}

func TestDetail_EscReturnsToList(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g1"}}
	svc.descriptions = map[string]kafka.GroupDescription{"g1": {Group: "g1", State: "Empty"}}
	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())
	_, cmd := m.Update(keyPress("enter"))
	drive(t, m, cmd)
	require.Equal(t, groups.ModeDetail, m.CurrentMode())

	_, _ = m.Update(keyPress("esc"))
	assert.Equal(t, groups.ModeList, m.CurrentMode())
}

// TestHasOverlay_DetailIsOverlay pins the host contract: while in
// ModeDetail the screen reports HasOverlay=true so the host's q/esc
// fallback yields esc to the screen (which closes detail) instead of
// also popping the groups screen — without this the user would skip
// the list view entirely with a single esc.
func TestHasOverlay_DetailIsOverlay(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g1"}}
	svc.descriptions = map[string]kafka.GroupDescription{"g1": {Group: "g1", State: "Empty"}}
	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())
	require.False(t, m.HasOverlay(), "list mode is not an overlay")

	_, cmd := m.Update(keyPress("enter"))
	drive(t, m, cmd)
	require.Equal(t, groups.ModeDetail, m.CurrentMode())
	assert.True(t, m.HasOverlay(), "detail mode must report as overlay so esc stays inside the screen")

	_, _ = m.Update(keyPress("esc"))
	assert.False(t, m.HasOverlay(), "after esc closes detail, overlay must clear")
}

func TestDetail_LargeNumbersHaveThousandsSeparators(t *testing.T) {
	d := newDetailWithRows(t, []kafka.PartitionLag{
		{Topic: "t", Partition: 0, Committed: 1234567, End: 2000000, Lag: 765433, MemberID: "m"},
	})
	out := d.View()
	assert.Contains(t, out, "1,234,567")
	assert.Contains(t, out, "2,000,000")
	assert.Contains(t, out, "765,433")
}

func TestDetail_TruncatesMembersWithMore(t *testing.T) {
	desc := kafka.GroupDescription{
		Group: "g",
		State: "Stable",
		Members: []kafka.GroupMember{
			{MemberID: "member-aaaaaaaaaaaaaaaaaaa-1"},
			{MemberID: "member-aaaaaaaaaaaaaaaaaaa-2"},
			{MemberID: "member-aaaaaaaaaaaaaaaaaaa-3"},
			{MemberID: "member-aaaaaaaaaaaaaaaaaaa-4"},
		},
	}
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g"}}
	svc.descriptions = map[string]kafka.GroupDescription{"g": desc}
	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())
	_, cmd := m.Update(keyPress("enter"))
	drive(t, m, cmd)
	d := m.Detail()
	require.NotNil(t, d)
	d.SetSize(80, 24)
	out := d.View()
	assert.Contains(t, out, "more")
}

// ----- reset flow tests -----

func TestReset_OpensFromListWithR(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g1", State: "Empty"}}
	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())

	_, _ = m.Update(keyPress("R"))
	assert.Equal(t, groups.ModeReset, m.CurrentMode())
	r := m.Reset()
	require.NotNil(t, r)
	assert.False(t, r.Express())
	assert.Equal(t, groups.StepStrategy, r.Step())
}

func TestWantsRawInput_OnlyDuringResetParams(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g1", State: "Empty"}}
	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())

	assert.False(t, m.WantsRawInput(), "list mode is not text input")

	_, _ = m.Update(keyPress("R"))
	require.Equal(t, groups.ModeReset, m.CurrentMode())
	require.Equal(t, groups.StepStrategy, m.Reset().Step())
	assert.False(t, m.WantsRawInput(), "strategy step is selection, not text")

	// j j → ResetShift, enter → StepParams (text input).
	_, _ = m.Update(keyPress("j"))
	_, _ = m.Update(keyPress("j"))
	_, _ = m.Update(keyPress("enter"))
	require.Equal(t, groups.StepParams, m.Reset().Step())
	assert.True(t, m.WantsRawInput(), "params step edits text")
}

func TestReset_ShiftRSetsExpress(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g1", State: "Empty"}}
	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())

	_, _ = m.Update(keyPress("shift+r"))
	r := m.Reset()
	require.NotNil(t, r)
	assert.True(t, r.Express())
}

func TestReset_FilteredListPassesScopeTopic(t *testing.T) {
	svc := newFakeService()
	svc.filteredGroups = map[string][]kafka.GroupListInfo{
		"orders": {{Group: "g1", State: "Empty"}},
	}
	m := groups.New(groups.Options{Service: svc, FilterTopic: "orders"})
	drive(t, m, m.Init())

	_, _ = m.Update(keyPress("R"))
	r := m.Reset()
	require.NotNil(t, r)
	scope, ok := r.Scope().(groups.ScopeWholeGroup)
	require.True(t, ok)
	assert.Equal(t, "orders", scope.Topic)
}

func TestReset_StrategyArrowKeysAndEnter(t *testing.T) {
	r := newResetModel(t)
	assert.Equal(t, kafka.ResetEarliest, r.Strategy())
	r, _ = r.Update(keyPress("j"))
	assert.Equal(t, kafka.ResetLatest, r.Strategy())
	r, _ = r.Update(keyPress("j"))
	assert.Equal(t, kafka.ResetShift, r.Strategy())
	r, _ = r.Update(keyPress("k"))
	assert.Equal(t, kafka.ResetLatest, r.Strategy())
}

func TestReset_EarliestSkipsParamsAndShowsPreview(t *testing.T) {
	svc := newFakeService()
	svc.previewByGroup = map[string]kafka.ResetPreview{
		"g1": {
			Group:    "g1",
			Strategy: kafka.ResetEarliest,
			Partitions: []kafka.PartitionResetPreview{
				{Topic: "t", Partition: 0, Committed: 5, Low: 0, High: 10, Target: 0, Diff: -5},
			},
			Summary: kafka.ResetSummary{Reconsume: 5},
		},
	}
	r := newResetModelWithSvc(t, svc, false)
	r, cmd := r.Update(keyPress("enter"))
	driveReset(t, r, cmd)
	assert.Equal(t, groups.StepPreview, r.Step())
	assert.Len(t, r.Preview().Partitions, 1)
	out := r.View()
	assert.Contains(t, out, "Preview")
	assert.Contains(t, out, "re-consumed") // summary line
}

func TestReset_ShiftRequiresParams(t *testing.T) {
	r := newResetModel(t)
	r, _ = r.Update(keyPress("j"))
	r, _ = r.Update(keyPress("j"))
	require.Equal(t, kafka.ResetShift, r.Strategy())
	r, _ = r.Update(keyPress("enter"))
	assert.Equal(t, groups.StepParams, r.Step())
}

func TestReset_ParamsValidationError(t *testing.T) {
	r := newResetModel(t)
	r, _ = r.Update(keyPress("j"))
	r, _ = r.Update(keyPress("j"))
	r, _ = r.Update(keyPress("enter"))
	require.Equal(t, groups.StepParams, r.Step())
	// erase the default "0"
	r, _ = r.Update(keyPress("backspace"))
	r, _ = r.Update(keyPress("enter"))
	assert.NotEmpty(t, r.Err())
	assert.Equal(t, groups.StepParams, r.Step())
}

func TestReset_PreviewYesCommits(t *testing.T) {
	svc := newFakeService()
	svc.previewByGroup = map[string]kafka.ResetPreview{
		"g1": {
			Group:    "g1",
			Strategy: kafka.ResetEarliest,
			Partitions: []kafka.PartitionResetPreview{
				{Topic: "t", Partition: 0, Committed: 5, Low: 0, High: 10, Target: 0, Diff: -5},
			},
			Summary: kafka.ResetSummary{Reconsume: 5},
		},
	}
	svc.commitByGroup = map[string]kafka.ResetPreview{
		"g1": svc.previewByGroup["g1"],
	}

	r := newResetModelWithSvc(t, svc, false)
	r, cmd := r.Update(keyPress("enter"))
	driveReset(t, r, cmd)

	r, cmd = r.Update(keyPress("y"))
	driveReset(t, r, cmd)
	a := r.ConsumeAction()
	assert.True(t, a.Done)
	require.NotNil(t, a.Result)
	assert.Equal(t, []string{"g1"}, svc.Committed())
}

func TestReset_PreviewNoCancels(t *testing.T) {
	svc := newFakeService()
	svc.previewByGroup = map[string]kafka.ResetPreview{
		"g1": {Group: "g1", Strategy: kafka.ResetEarliest},
	}
	r := newResetModelWithSvc(t, svc, false)
	r, cmd := r.Update(keyPress("enter"))
	driveReset(t, r, cmd)

	r, _ = r.Update(keyPress("n"))
	a := r.ConsumeAction()
	assert.True(t, a.Cancel)
	assert.Empty(t, svc.Committed())
}

func TestReset_ExpressSkipsPreviewAndCommitsImmediately(t *testing.T) {
	svc := newFakeService()
	svc.commitByGroup = map[string]kafka.ResetPreview{
		"g1": {
			Group:    "g1",
			Strategy: kafka.ResetEarliest,
			Partitions: []kafka.PartitionResetPreview{
				{Topic: "t", Partition: 0, Target: 0, Diff: -1},
			},
		},
	}
	r := newResetModelWithSvc(t, svc, true)
	require.True(t, r.Express())

	r, cmd := r.Update(keyPress("enter"))
	driveReset(t, r, cmd)

	a := r.ConsumeAction()
	assert.True(t, a.Done)
	assert.Equal(t, []string{"g1"}, svc.Committed())
	assert.Empty(t, svc.Previewed())
}

func TestReset_AdaptiveHeader_SingleTopic(t *testing.T) {
	scope := groups.ScopeWholeGroup{Group: "g", Topic: "orders"}
	got := scope.HeaderLabel(3, 1)
	assert.Equal(t, "Resetting 3 partitions in orders", got)
}

func TestReset_AdaptiveHeader_MultipleTopics(t *testing.T) {
	scope := groups.ScopeWholeGroup{Group: "g"}
	assert.Equal(t, "Resetting 5 partitions across 2 topics", scope.HeaderLabel(5, 2))
}

func TestReset_AdaptiveHeader_AllTotal(t *testing.T) {
	scope := groups.ScopeWholeGroup{Group: "g"}
	assert.Equal(t, "Resetting all partitions (4 total)", scope.HeaderLabel(4, 0))
}

func TestReset_NonEmptyGroupShowsError(t *testing.T) {
	svc := newFakeService()
	svc.previewErr = kafka.ErrNonEmptyGroup
	r := newResetModelWithSvc(t, svc, false)
	r, cmd := r.Update(keyPress("enter"))
	driveReset(t, r, cmd)
	assert.Contains(t, r.Err(), "not empty")
	assert.NotEqual(t, groups.StepDone, r.Step())
}

func TestReset_EscCancels(t *testing.T) {
	r := newResetModel(t)
	r, _ = r.Update(keyPress("esc"))
	a := r.ConsumeAction()
	assert.True(t, a.Cancel)
}

func TestReset_PreviewIncludesNote(t *testing.T) {
	svc := newFakeService()
	svc.previewByGroup = map[string]kafka.ResetPreview{
		"g1": {
			Group:    "g1",
			Strategy: kafka.ResetSpecific,
			Partitions: []kafka.PartitionResetPreview{
				{Topic: "t", Partition: 0, Committed: 5, Low: 0, High: 10, Target: 10, Diff: 5, Note: kafka.ResetNoteClampedHigh},
			},
		},
	}
	r := newResetModelWithSvc(t, svc, false)
	// pick "specific"
	for range 4 {
		r, _ = r.Update(keyPress("j"))
	}
	require.Equal(t, kafka.ResetSpecific, r.Strategy())
	r, _ = r.Update(keyPress("enter"))
	require.Equal(t, groups.StepParams, r.Step())
	r, cmd := r.Update(keyPress("enter"))
	driveReset(t, r, cmd)
	out := r.View()
	assert.Contains(t, out, kafka.ResetNoteClampedHigh)
}

// ----- helpers -----

func driveReset(t *testing.T, r *groups.ResetModel, cmd tea.Cmd) {
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
		_, follow := r.Update(msg)
		queue = append(queue, follow)
	}
}

func newResetModel(t *testing.T) *groups.ResetModel {
	t.Helper()
	return newResetModelWithSvc(t, newFakeService(), false)
}

func newResetModelWithSvc(t *testing.T, svc *fakeService, express bool) *groups.ResetModel {
	t.Helper()
	return groups.NewResetModel(groups.ResetOptions{
		Service: svc,
		Group:   "g1",
		Scope:   groups.ScopeWholeGroup{Group: "g1"},
		Express: express,
	})
}

func newDetailWithRows(t *testing.T, rows []kafka.PartitionLag) *groups.DetailModel {
	t.Helper()
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g1"}}
	svc.descriptions = map[string]kafka.GroupDescription{
		"g1": {Group: "g1", State: "Stable"},
	}
	svc.offsets = map[string][]kafka.PartitionLag{"g1": rows}
	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())
	_, cmd := m.Update(keyPress("enter"))
	drive(t, m, cmd)
	d := m.Detail()
	require.NotNil(t, d)
	return d
}

func buildModelWith(t *testing.T, gs []kafka.GroupListInfo) *groups.Model {
	t.Helper()
	svc := newFakeService()
	svc.groups = append([]kafka.GroupListInfo(nil), gs...)
	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())
	return m
}

func hasToast(m *groups.Model, needle string) bool {
	for _, t := range m.Toasts().Items() {
		if strings.Contains(t.Message, needle) {
			return true
		}
	}
	return false
}

type fakeService struct {
	mu sync.Mutex

	groups         []kafka.GroupListInfo
	listErr        error
	listN          int
	filteredGroups map[string][]kafka.GroupListInfo
	filterErr      error

	descriptions map[string]kafka.GroupDescription
	descErr      error

	offsets    map[string][]kafka.PartitionLag
	offsetsErr error

	previewByGroup map[string]kafka.ResetPreview
	previewErr     error
	previewedNames []string

	commitByGroup  map[string]kafka.ResetPreview
	commitErr      error
	committedNames []string

	deleted   []string
	deleteErr error
}

func newFakeService() *fakeService {
	return &fakeService{
		filteredGroups: map[string][]kafka.GroupListInfo{},
		descriptions:   map[string]kafka.GroupDescription{},
		offsets:        map[string][]kafka.PartitionLag{},
		previewByGroup: map[string]kafka.ResetPreview{},
		commitByGroup:  map[string]kafka.ResetPreview{},
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

func (f *fakeService) Previewed() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.previewedNames...)
}

func (f *fakeService) Committed() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.committedNames...)
}

func (f *fakeService) ListConsumerGroups(_ context.Context) ([]kafka.GroupListInfo, error) {
	f.mu.Lock()
	f.listN++
	out := append([]kafka.GroupListInfo(nil), f.groups...)
	err := f.listErr
	f.mu.Unlock()
	return out, err
}

func (f *fakeService) FilterGroupsByTopic(_ context.Context, topic string) ([]kafka.GroupListInfo, error) {
	f.mu.Lock()
	f.listN++
	out := append([]kafka.GroupListInfo(nil), f.filteredGroups[topic]...)
	err := f.filterErr
	f.mu.Unlock()
	return out, err
}

func (f *fakeService) DescribeConsumerGroup(_ context.Context, group string) (kafka.GroupDescription, error) {
	f.mu.Lock()
	d, ok := f.descriptions[group]
	err := f.descErr
	f.mu.Unlock()
	if !ok && err == nil {
		return kafka.GroupDescription{Group: group}, nil
	}
	return d, err
}

func (f *fakeService) GroupOffsets(_ context.Context, group string) ([]kafka.PartitionLag, error) {
	f.mu.Lock()
	out := append([]kafka.PartitionLag(nil), f.offsets[group]...)
	err := f.offsetsErr
	f.mu.Unlock()
	return out, err
}

func (f *fakeService) PreviewReset(_ context.Context, group string, _ kafka.ResetSpec) (kafka.ResetPreview, error) {
	f.mu.Lock()
	f.previewedNames = append(f.previewedNames, group)
	pv := f.previewByGroup[group]
	err := f.previewErr
	f.mu.Unlock()
	return pv, err
}

func (f *fakeService) ResetOffsets(_ context.Context, group string, _ kafka.ResetSpec) (kafka.ResetPreview, error) {
	f.mu.Lock()
	f.committedNames = append(f.committedNames, group)
	pv := f.commitByGroup[group]
	err := f.commitErr
	f.mu.Unlock()
	return pv, err
}

func (f *fakeService) DeleteConsumerGroup(_ context.Context, group string) error {
	f.mu.Lock()
	f.deleted = append(f.deleted, group)
	err := f.deleteErr
	f.mu.Unlock()
	return err
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
	case "shift+r":
		return tea.KeyPressMsg{Code: 'r', Mod: tea.ModShift}
	}
	if len(name) == 1 {
		r := rune(name[0])
		return tea.KeyPressMsg{Code: r, Text: string(r)}
	}
	return tea.KeyPressMsg{}
}

func TestReset_ViewRendersStrategyStepWithSelection(t *testing.T) {
	r := newResetModel(t)

	out := r.View()

	assert.Contains(t, out, "Choose strategy")
	assert.Contains(t, out, "earliest")
	assert.Contains(t, out, "latest")
	assert.Contains(t, out, "shift")
	assert.Contains(t, out, "timestamp")
	assert.Contains(t, out, "specific")
	// the default-selected strategy carries the filled marker.
	assert.Contains(t, out, "(•)")
}

func TestReset_ViewRendersParamsStepWithForm(t *testing.T) {
	r := newResetModel(t)
	// pick "shift" and advance into params via the strategy handler.
	r, _ = r.Update(keyPress("j"))
	r, _ = r.Update(keyPress("j"))
	require.Equal(t, kafka.ResetShift, r.Strategy())
	r, _ = r.Update(keyPress("enter"))
	require.Equal(t, groups.StepParams, r.Step())

	out := r.View()
	assert.Contains(t, out, "Parameters", "params step header must surface")
}
