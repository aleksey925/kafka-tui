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
// messages back through the Model. Cmds that don't deliver a value within
// driveCmdDeadline are dropped — that's how we skip [tea.Tick] cmds (the
// auto-refresh chain) without blocking on real timers.
func drive(t *testing.T, m *groups.Model, cmd tea.Cmd) {
	t.Helper()
	queue := []tea.Cmd{cmd}
	for len(queue) > 0 {
		next := queue[0]
		queue = queue[1:]
		if next == nil {
			continue
		}
		msg := runCmdNonBlocking(next)
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

const driveCmdDeadline = 50 * time.Millisecond

func runCmdNonBlocking(cmd tea.Cmd) tea.Msg {
	done := make(chan tea.Msg, 1)
	go func() { done <- cmd() }()
	select {
	case msg := <-done:
		return msg
	case <-time.After(driveCmdDeadline):
		return nil
	}
}

func TestNew_DefaultColumns(t *testing.T) {
	svc := newFakeService()
	m := groups.New(groups.Options{Service: svc})
	out := m.View()
	assert.Contains(t, out, "State")
	assert.Contains(t, out, "ID")
	assert.Contains(t, out, "Coordinator")
	assert.Contains(t, out, "Protocol")
	assert.Contains(t, out, "Members")
	assert.Contains(t, out, "Total Lag")
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

func TestTitle_AppliesAngleBracketFilter(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{
		{Group: "g1", State: "Stable", Coordinator: 1},
		{Group: "g2", State: "Empty", Coordinator: 2},
	}
	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())

	m.SetSearch("g1")

	assert.Contains(t, m.Title(), "Consumer Groups [1/2] </g1>")
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
	cmd := m.Update(keyPress("enter"))
	assert.Equal(t, groups.ModeDetail, m.CurrentMode())
	assert.NotNil(t, m.Detail())
	drive(t, m, cmd)
	d := m.Detail()
	require.NotNil(t, d)
	assert.Equal(t, "g1", d.Group())
}

func TestEsc_RaisesBackOnList(t *testing.T) {
	m := buildModelWith(t, []kafka.GroupListInfo{{Group: "g1"}})
	_ = m.Update(keyPress("esc"))
	assert.True(t, m.ConsumeAction().Back)
}

func TestRefresh_RKeyReloadsGroups(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g1"}}
	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())
	require.Equal(t, 1, svc.ListCalls())

	cmd := m.Update(keyPress("r"))
	drive(t, m, cmd)
	assert.Equal(t, 2, svc.ListCalls())
}

func TestReadOnly_BlocksDestructiveHotkeys(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g1", State: "Empty"}}
	m := groups.New(groups.Options{Service: svc, ReadOnly: true})
	drive(t, m, m.Init())

	for _, k := range []string{"R", "ctrl+d"} {
		_ = m.Update(keyPress(k))
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

	_ = m.Update(keyPress("ctrl+d"))
	require.True(t, m.ConfirmOpen())
	assert.Equal(t, "g1", m.PendingGroup())

	cmd := m.Update(keyPress("y"))
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

	_ = m.Update(keyPress("ctrl+d"))
	cmd := m.Update(keyPress("y"))
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

	_ = m.Update(keyPress("ctrl+d"))
	require.True(t, m.ConfirmOpen())
	_ = m.Update(keyPress("n"))
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
	assert.Contains(t, got, "refresh")
	assert.Contains(t, got, "delete")
	assert.Contains(t, got, "filter")
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
	assert.NotContains(t, got, "delete")
}

func TestRefreshInterval_AutoRefreshTickIssuesCommandAtDefault(t *testing.T) {
	svc := newFakeService()
	m := groups.New(groups.Options{Service: svc})
	cmd := m.AutoRefreshTick()
	require.NotNil(t, cmd, "default non-zero interval must yield a tick cmd")
}

func TestRefreshInterval_PersistedManualYieldsNilCmd(t *testing.T) {
	repo := newFakeRefreshRepo()
	repo.stored["groups"] = 0
	m := groups.New(groups.Options{Service: newFakeService(), RefreshIntervals: repo})
	assert.Nil(t, m.AutoRefreshTick(),
		"persisted Manual (0) must override the default and disable ticks")
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

// TestList_AutoFetchesLagAfterLoad pins the regression: the Members and
// Total Lag list columns were rendered as "—" because nothing called
// loadLagCmd in production. Init must chain a lag fetch after the groups
// load so users see real values without an extra interaction. Asserts on
// the cache directly so the test isn't sensitive to render formatting.
func TestList_AutoFetchesLagAfterLoad(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{
		{Group: "g1", State: "Stable", Coordinator: 1},
		{Group: "g2", State: "Stable", Coordinator: 2},
	}
	svc.offsets = map[string][]kafka.PartitionLag{
		"g1": {
			{Topic: "t", Partition: 0, Lag: 50, MemberID: "m-a"},
			{Topic: "t", Partition: 1, Lag: 10, MemberID: "m-b"},
		},
		"g2": {
			{Topic: "t", Partition: 0, Lag: 4242, MemberID: "m-c"},
		},
	}
	m := groups.New(groups.Options{Service: svc})

	drive(t, m, m.Init())

	g1Lag, g1Members, g1OK := m.CachedLag("g1")
	require.True(t, g1OK, "g1 lag must be cached without an extra FetchLagsForVisible call")
	assert.Equal(t, int64(60), g1Lag)
	assert.Equal(t, 2, g1Members)
	g2Lag, g2Members, g2OK := m.CachedLag("g2")
	require.True(t, g2OK, "g2 lag must be cached without an extra FetchLagsForVisible call")
	assert.Equal(t, int64(4242), g2Lag)
	assert.Equal(t, 1, g2Members)
}

// TestList_PrunesStaleLagAfterRefresh covers the cache-hygiene side of the
// refresh path: when a previously listed group disappears, its cached
// Members/Total Lag entries must be dropped so they can't resurface if the
// same name reappears later. We assert directly on the cache via
// [groups.Model.CachedLag] — checking m.View() alone wouldn't catch a
// regression because refreshTable iterates m.groups, not the cache.
func TestList_PrunesStaleLagAfterRefresh(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g1"}, {Group: "g2"}}
	svc.offsets = map[string][]kafka.PartitionLag{
		"g1": {{Topic: "t", Partition: 0, Lag: 5, MemberID: "m"}},
		"g2": {{Topic: "t", Partition: 0, Lag: 9, MemberID: "m"}},
	}
	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())
	_, _, hadG2 := m.CachedLag("g2")
	require.True(t, hadG2, "lag cache must be populated after the initial fetch")

	// drop g2 from the listing and refresh.
	svc.mu.Lock()
	svc.groups = []kafka.GroupListInfo{{Group: "g1"}}
	svc.mu.Unlock()
	drive(t, m, m.Update(keyPress("r")))

	_, _, stillHasG2 := m.CachedLag("g2")
	assert.False(t, stillHasG2, "pruneLagCache must drop the entry for groups that left the listing")
	_, _, stillHasG1 := m.CachedLag("g1")
	assert.True(t, stillHasG1, "live groups must keep their cached entries")
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
	cmd := m.Update(keyPress("enter"))
	drive(t, m, cmd)

	d := m.Detail()
	require.NotNil(t, d)
	rows := d.Rows()
	require.Len(t, rows, 1)
	assert.Equal(t, "alpha", rows[0].Topic)
	out := d.View()
	assert.Contains(t, out, "State:")
	assert.Contains(t, out, "Stable")
	assert.Contains(t, out, "Total Lag:")
	assert.Contains(t, out, "Protocol: range")
	assert.Contains(t, out, "alpha")
	assert.Contains(t, out, "broker:9092")
	assert.Contains(t, out, "Partition")
}

// TestDetail_TabSwitchesFocus pins the two-pane navigation: tab toggles
// keystroke focus between the topics summary and the partitions table.
func TestDetail_TabSwitchesFocus(t *testing.T) {
	d := newDetailWithRows(t, []kafka.PartitionLag{
		{Topic: "a", Partition: 0, Lag: 5, MemberID: "m"},
	})
	require.Equal(t, groups.FocusTopics, d.Focus())
	d, _ = d.Update(keyPress("tab"))
	assert.Equal(t, groups.FocusPartitions, d.Focus())
	d, _ = d.Update(keyPress("tab"))
	assert.Equal(t, groups.FocusTopics, d.Focus())
}

// TestDetail_EnterDrillsIntoPartitions covers `enter` from the topics
// pane: it moves focus to partitions without leaving the screen.
func TestDetail_EnterDrillsIntoPartitions(t *testing.T) {
	d := newDetailWithRows(t, []kafka.PartitionLag{
		{Topic: "alpha", Partition: 0, Lag: 1, MemberID: "m"},
	})
	require.Equal(t, groups.FocusTopics, d.Focus())
	d, _ = d.Update(keyPress("enter"))
	assert.Equal(t, groups.FocusPartitions, d.Focus())
}

// TestDetail_EscFromPartitionsReturnsToTopics: esc unwinds focus before
// it raises Action.Back. Without this, drilling in and pressing esc once
// would surprise the user by exiting the detail screen entirely.
func TestDetail_EscFromPartitionsReturnsToTopics(t *testing.T) {
	d := newDetailWithRows(t, []kafka.PartitionLag{
		{Topic: "alpha", Partition: 0, Lag: 1, MemberID: "m"},
	})
	d, _ = d.Update(keyPress("enter"))
	require.Equal(t, groups.FocusPartitions, d.Focus())

	d, _ = d.Update(keyPress("esc"))
	a := d.ConsumeAction()
	assert.False(t, a.Back, "esc on partitions must not trigger Back")
	assert.Equal(t, groups.FocusTopics, d.Focus())

	d, _ = d.Update(keyPress("esc"))
	a = d.ConsumeAction()
	assert.True(t, a.Back, "esc on topics raises Back")
}

// TestDetail_PartitionsRebuildOnTopicSelection: navigating the topics
// table swaps the partitions pane to the newly-selected topic without
// any extra keypress (master-detail style live preview).
func TestDetail_PartitionsRebuildOnTopicSelection(t *testing.T) {
	d := newDetailWithRows(t, []kafka.PartitionLag{
		{Topic: "alpha", Partition: 0, Lag: 1, MemberID: "ma"},
		{Topic: "beta", Partition: 0, Lag: 99, MemberID: "mb"},
	})
	require.Equal(t, "alpha", d.FocusedTopic())
	assert.Contains(t, d.View(), "Partitions · alpha")

	// j moves the topics cursor onto beta — partitions pane follows.
	d, _ = d.Update(keyPress("j"))
	assert.Equal(t, "beta", d.FocusedTopic())
	assert.Contains(t, d.View(), "Partitions · beta")
}

// TestDetail_T_JumpsToFocusedTopic: `t` no longer carries the legacy
// multi-topic semantics — it just routes to messages of the topic at
// the topics-pane cursor.
func TestDetail_T_JumpsToFocusedTopic(t *testing.T) {
	d := newDetailWithRows(t, []kafka.PartitionLag{
		{Topic: "alpha", Partition: 0, Lag: 1, MemberID: "m"},
		{Topic: "beta", Partition: 0, Lag: 1, MemberID: "m"},
	})
	d, _ = d.Update(keyPress("j")) // beta now focused
	d, _ = d.Update(keyPress("t"))
	a := d.ConsumeAction()
	assert.Equal(t, "beta", a.Topic)
}

func TestDetail_EscReturnsToList(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g1"}}
	svc.descriptions = map[string]kafka.GroupDescription{"g1": {Group: "g1", State: "Empty"}}
	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())
	cmd := m.Update(keyPress("enter"))
	drive(t, m, cmd)
	require.Equal(t, groups.ModeDetail, m.CurrentMode())

	_ = m.Update(keyPress("esc"))
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

	cmd := m.Update(keyPress("enter"))
	drive(t, m, cmd)
	require.Equal(t, groups.ModeDetail, m.CurrentMode())
	assert.True(t, m.HasOverlay(), "detail mode must report as overlay so esc stays inside the screen")

	_ = m.Update(keyPress("esc"))
	assert.False(t, m.HasOverlay(), "after esc closes detail, overlay must clear")
}

func TestDetail_HeaderShowsTotalLag(t *testing.T) {
	d := newDetailWithRows(t, []kafka.PartitionLag{
		{Topic: "t", Partition: 0, Lag: 200, MemberID: "m"},
		{Topic: "t", Partition: 1, Lag: 1000, MemberID: "m"},
		// negative is the "no committed offset" sentinel and must not pollute the sum.
		{Topic: "t", Partition: 2, Lag: -1, MemberID: "m"},
	})
	out := d.View()
	assert.Contains(t, out, "Total Lag: 1,200")
}

// TestDetail_TopicsTableShowsAggregates: the outer pane carries
// per-topic aggregates (partition count, total lag, unique member
// count) so users get a hot-spot view without drilling into each topic.
func TestDetail_TopicsTableShowsAggregates(t *testing.T) {
	d := newDetailWithRows(t, []kafka.PartitionLag{
		{Topic: "alpha", Partition: 0, Lag: 100, MemberID: "ma"},
		{Topic: "alpha", Partition: 1, Lag: 50, MemberID: "mb"},
		{Topic: "beta", Partition: 0, Lag: 5, MemberID: "mc"},
	})

	out := d.View()
	// outer table heading + each topic row; total lag for alpha is 150.
	assert.Contains(t, out, "Topics [2]")
	assert.Contains(t, out, "alpha")
	assert.Contains(t, out, "beta")
	assert.Contains(t, out, "150")
}

func TestDetail_LargeNumbersHaveThousandsSeparators(t *testing.T) {
	d := newDetailWithRows(t, []kafka.PartitionLag{
		{Topic: "t", Partition: 0, Committed: 1234567, End: 2000000, Lag: 765433, MemberID: "m"},
	})

	// the partitions pane is auto-populated for the focused topic; no
	// expansion step is needed in the new two-pane design.
	out := d.View()
	assert.Contains(t, out, "1,234,567")
	assert.Contains(t, out, "2,000,000")
	assert.Contains(t, out, "765,433")
}

func TestDetail_HeaderShowsMembersCount(t *testing.T) {
	desc := kafka.GroupDescription{
		Group: "g",
		State: "Stable",
		Members: []kafka.GroupMember{
			{MemberID: "m1"},
			{MemberID: "m2"},
			{MemberID: "m3"},
			{MemberID: "m4"},
		},
	}
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g"}}
	svc.descriptions = map[string]kafka.GroupDescription{"g": desc}
	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())
	cmd := m.Update(keyPress("enter"))
	drive(t, m, cmd)
	d := m.Detail()
	require.NotNil(t, d)
	d.SetSize(160, 24)
	assert.Contains(t, d.View(), "Members: 4")
}

// ----- reset flow tests -----

func TestReset_OpensFromListWithR(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g1", State: "Empty"}}
	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())

	_ = m.Update(keyPress("R"))
	assert.Equal(t, groups.ModeReset, m.CurrentMode())
	r := m.Reset()
	require.NotNil(t, r)
	assert.Equal(t, groups.StepStrategy, r.Step())
}

func TestWantsRawInput_OnlyDuringResetParams(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g1", State: "Empty"}}
	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())

	assert.False(t, m.WantsRawInput(), "list mode is not text input")

	_ = m.Update(keyPress("R"))
	require.Equal(t, groups.ModeReset, m.CurrentMode())
	require.Equal(t, groups.StepStrategy, m.Reset().Step())
	assert.False(t, m.WantsRawInput(), "strategy step is selection, not text")

	// j j → ResetShift, enter → StepParams (text input).
	_ = m.Update(keyPress("j"))
	_ = m.Update(keyPress("j"))
	_ = m.Update(keyPress("enter"))
	require.Equal(t, groups.StepParams, m.Reset().Step())
	assert.True(t, m.WantsRawInput(), "params step edits text")
}

// TestReset_DoneFromDetailRefreshesDetail pins the post-commit
// refresh: after a successful reset opened from inside detail, the
// detail must re-fetch so the user sees the new committed offsets
// without an extra manual `r`.
func TestReset_DoneFromDetailRefreshesDetail(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g1", State: "Empty"}}
	svc.descriptions = map[string]kafka.GroupDescription{
		"g1": {Group: "g1", State: "Empty"},
	}
	svc.offsets = map[string][]kafka.PartitionLag{
		"g1": {{Topic: "alpha", Partition: 0, Lag: 5, MemberID: "m"}},
	}
	svc.previewByGroup = map[string]kafka.ResetPreview{
		"g1": {
			Group:    "g1",
			Strategy: kafka.ResetEarliest,
			Partitions: []kafka.PartitionResetPreview{
				{Topic: "alpha", Partition: 0, Committed: 5, Low: 0, High: 10, Target: 0, Diff: -5},
			},
		},
	}
	svc.commitByGroup = map[string]kafka.ResetPreview{
		"g1": svc.previewByGroup["g1"],
	}

	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())

	// drill into detail.
	drive(t, m, m.Update(keyPress("enter")))
	require.Equal(t, groups.ModeDetail, m.CurrentMode())
	descCallsBeforeReset := svc.DescribeCalls()

	// open reset, choose earliest, confirm.
	drive(t, m, m.Update(keyPress("R")))
	require.Equal(t, groups.ModeReset, m.CurrentMode())
	drive(t, m, m.Update(keyPress("enter"))) // strategy → preview
	drive(t, m, m.Update(keyPress("y")))     // commit

	require.Equal(t, groups.ModeDetail, m.CurrentMode(), "reset must restore detail on commit")
	assert.Greater(t, svc.DescribeCalls(), descCallsBeforeReset,
		"the detail must re-fetch after a successful reset so the user sees the new offsets")
}

// TestReset_EscFromDetailReturnsToDetail pins the regression where
// canceling reset always popped back to the groups list — even if the
// flow had been opened from inside the detail view. The model now
// remembers its origin and restores it on cancel.
func TestReset_EscFromDetailReturnsToDetail(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g1", State: "Empty"}}
	svc.descriptions = map[string]kafka.GroupDescription{
		"g1": {Group: "g1", State: "Empty"},
	}
	svc.offsets = map[string][]kafka.PartitionLag{
		"g1": {{Topic: "alpha", Partition: 0, Lag: 1, MemberID: "m"}},
	}
	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())

	// open detail, then reset from inside detail.
	cmd := m.Update(keyPress("enter"))
	drive(t, m, cmd)
	require.Equal(t, groups.ModeDetail, m.CurrentMode())

	_ = m.Update(keyPress("R"))
	require.Equal(t, groups.ModeReset, m.CurrentMode())

	// esc from the strategy step cancels the flow.
	_ = m.Update(keyPress("esc"))
	assert.Equal(t, groups.ModeDetail, m.CurrentMode(),
		"reset opened from detail must restore detail on cancel, not fall back to the list")
}

// TestReset_EscFromListReturnsToList covers the inverse: a reset opened
// from the list still lands back on the list when canceled.
func TestReset_EscFromListReturnsToList(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g1", State: "Empty"}}
	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())

	_ = m.Update(keyPress("R"))
	require.Equal(t, groups.ModeReset, m.CurrentMode())

	_ = m.Update(keyPress("esc"))
	assert.Equal(t, groups.ModeList, m.CurrentMode())
}

// TestReset_FromDetailTopicsFocusUsesScopeTopic: with the topics pane
// active, R narrows the reset to that topic's partitions. The members
// list is sourced from cluster metadata (TopicsPartitions) so partitions
// the group never committed to are still in scope — without that, a
// "reset whole topic" would silently skip partitions the group hasn't
// touched yet.
func TestReset_FromDetailTopicsFocusUsesScopeTopic(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g1"}}
	svc.descriptions = map[string]kafka.GroupDescription{
		"g1": {Group: "g1", State: "Stable"},
	}
	// the group has commits only for partitions 0 and 1, but the topic
	// actually has 4 partitions (0..3).
	svc.offsets = map[string][]kafka.PartitionLag{
		"g1": {
			{Topic: "alpha", Partition: 0, Lag: 1, MemberID: "ma"},
			{Topic: "alpha", Partition: 1, Lag: 2, MemberID: "mb"},
		},
	}
	svc.topicPartitions = map[string][]int32{
		"alpha": {0, 1, 2, 3},
	}
	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())
	cmd := m.Update(keyPress("enter"))
	drive(t, m, cmd)
	d := m.Detail()
	require.NotNil(t, d)
	require.Equal(t, groups.FocusTopics, d.Focus())

	scope, ok := d.ResetScope().(groups.ScopeTopic)
	require.True(t, ok, "topics-pane reset must produce ScopeTopic")
	assert.Equal(t, "alpha", scope.Topic)
	assert.Equal(t, []kafka.TopicPartition{
		{Topic: "alpha", Partition: 0},
		{Topic: "alpha", Partition: 1},
		{Topic: "alpha", Partition: 2},
		{Topic: "alpha", Partition: 3},
	}, scope.Members, "scope must cover every partition of the topic, not just those with prior commits")
}

// TestReset_PreviewScrollsWithJK pins the scrolling-preview UX: the
// preview list is rendered through a [components.Table] so j/k advances
// the cursor when the partition list is longer than the visible area.
// Without this the manual rendering used to silently truncate.
func TestReset_PreviewScrollsWithJK(t *testing.T) {
	svc := newFakeService()
	parts := make([]kafka.PartitionResetPreview, 0, 30)
	for i := range int32(30) {
		parts = append(parts, kafka.PartitionResetPreview{
			Topic: "alpha", Partition: i,
			Committed: int64(i), Low: 0, High: int64(i + 100),
			Target: 0, Diff: -int64(i),
		})
	}
	svc.previewByGroup = map[string]kafka.ResetPreview{
		"g1": {Group: "g1", Strategy: kafka.ResetEarliest, Partitions: parts},
	}

	r := groups.NewResetModel(groups.ResetOptions{
		Service: svc,
		Group:   "g1",
		Scope:   groups.ScopeWholeGroup{Group: "g1"},
	})
	// constrain the preview viewport so only a slice of rows is visible
	// at a time — that's where scrolling matters.
	r.SetSize(120, 18)

	r, cmd := r.Update(keyPress("enter")) // strategy → preview
	driveReset(t, r, cmd)
	require.Equal(t, groups.StepPreview, r.Step())

	first := r.View()
	for range 25 {
		r, _ = r.Update(keyPress("j"))
	}
	after := r.View()
	assert.NotEqual(t, first, after,
		"j must advance the preview viewport when the list overflows the visible area")
}

// TestReset_HeaderShowsScopeCountAtStep1 pins the regression where the
// strategy step rendered "Resetting 0 partitions in topic" because the
// header relied on the (still-empty) preview. The scope already knows
// its targets — the header must reflect that count immediately so users
// don't think the scope is empty before they pick a strategy.
func TestReset_HeaderShowsScopeCountAtStep1(t *testing.T) {
	scope := groups.ScopeTopic{
		Group: "g",
		Topic: "alpha",
		Members: []kafka.TopicPartition{
			{Topic: "alpha", Partition: 0},
			{Topic: "alpha", Partition: 1},
			{Topic: "alpha", Partition: 2},
			{Topic: "alpha", Partition: 3},
		},
	}
	r := groups.NewResetModel(groups.ResetOptions{
		Service: newFakeService(),
		Group:   "g",
		Scope:   scope,
	})

	out := r.View()
	assert.Contains(t, out, "Resetting 4 partitions in alpha",
		"header at step 1 must already reflect the scope-derived partition count")
}

// TestReset_TopicScopeFallsBackToCommittedPartitions: if the metadata
// fetch fails (or is unavailable), the scope still resolves — limited
// to the partitions the group already has commits for. Degraded but
// usable rather than empty.
func TestReset_TopicScopeFallsBackToCommittedPartitions(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g1"}}
	svc.descriptions = map[string]kafka.GroupDescription{
		"g1": {Group: "g1", State: "Stable"},
	}
	svc.offsets = map[string][]kafka.PartitionLag{
		"g1": {
			{Topic: "alpha", Partition: 0, Lag: 1, MemberID: "ma"},
			{Topic: "alpha", Partition: 1, Lag: 2, MemberID: "mb"},
		},
	}
	// no entry in svc.topicPartitions → simulates a metadata gap.

	m := groups.New(groups.Options{Service: svc})
	drive(t, m, m.Init())
	cmd := m.Update(keyPress("enter"))
	drive(t, m, cmd)
	d := m.Detail()
	require.NotNil(t, d)

	scope, ok := d.ResetScope().(groups.ScopeTopic)
	require.True(t, ok)
	assert.Equal(t, []kafka.TopicPartition{
		{Topic: "alpha", Partition: 0},
		{Topic: "alpha", Partition: 1},
	}, scope.Members, "without metadata, fall back to the rows-derived partition list")
}

// TestReset_FromDetailPartitionsFocusUsesScopePartition: drilling into
// the partitions pane narrows the reset to a single (topic, partition)
// pair.
func TestReset_FromDetailPartitionsFocusUsesScopePartition(t *testing.T) {
	d := newDetailWithRows(t, []kafka.PartitionLag{
		{Topic: "alpha", Partition: 0, Lag: 1, MemberID: "ma"},
		{Topic: "alpha", Partition: 1, Lag: 2, MemberID: "mb"},
	})
	d, _ = d.Update(keyPress("enter")) // drill in
	require.Equal(t, groups.FocusPartitions, d.Focus())

	scope, ok := d.ResetScope().(groups.ScopePartition)
	require.True(t, ok, "partitions-pane reset must produce ScopePartition")
	assert.Equal(t, "alpha", scope.Topic)
	assert.Equal(t, int32(0), scope.Partition)
}

// TestReset_FromListUsesGroupScope: pressing R on the groups list always
// targets the whole group — scoped reset (per-topic / per-partition) is
// reachable from inside the detail view via the topics / partitions pane.
func TestReset_FromListUsesGroupScope(t *testing.T) {
	svc := newFakeService()
	svc.filteredGroups = map[string][]kafka.GroupListInfo{
		"orders": {{Group: "g1", State: "Empty"}},
	}
	m := groups.New(groups.Options{Service: svc, FilterTopic: "orders"})
	drive(t, m, m.Init())

	_ = m.Update(keyPress("R"))
	r := m.Reset()
	require.NotNil(t, r)
	_, ok := r.Scope().(groups.ScopeWholeGroup)
	assert.True(t, ok, "list-level reset must target the whole group regardless of FilterTopic")
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
	r := newResetModelWithSvc(t, svc)
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

	r := newResetModelWithSvc(t, svc)
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
	r := newResetModelWithSvc(t, svc)
	r, cmd := r.Update(keyPress("enter"))
	driveReset(t, r, cmd)

	r, _ = r.Update(keyPress("n"))
	a := r.ConsumeAction()
	assert.True(t, a.Cancel)
	assert.Empty(t, svc.Committed())
}

func TestReset_AdaptiveHeader_SingleTopic(t *testing.T) {
	scope := groups.ScopeTopic{Group: "g", Topic: "orders"}
	got := scope.HeaderLabel(3, 1)
	assert.Equal(t, "Resetting 3 partitions in orders", got)
}

func TestReset_AdaptiveHeader_PartitionScope(t *testing.T) {
	scope := groups.ScopePartition{Group: "g", Topic: "orders", Partition: 7}
	assert.Equal(t, "Resetting orders partition 7", scope.HeaderLabel(1, 1))
}

func TestReset_AdaptiveHeader_MultipleTopics(t *testing.T) {
	scope := groups.ScopeWholeGroup{Group: "g"}
	assert.Equal(t, "Resetting 5 partitions across 2 topics in this group", scope.HeaderLabel(5, 2))
}

// TestReset_AdaptiveHeader_PrePreview pins the intent-only label rendered
// before the preview RPC has produced counts. "every partition of every
// topic" is unambiguous; "all partitions (N total)" left users wondering
// whether the scope was the whole group or a single topic.
func TestReset_AdaptiveHeader_PrePreview(t *testing.T) {
	scope := groups.ScopeWholeGroup{Group: "g"}
	assert.Equal(t, "Resetting every partition of every topic in this group", scope.HeaderLabel(0, 0))
}

func TestReset_NonEmptyGroupShowsError(t *testing.T) {
	svc := newFakeService()
	svc.previewErr = kafka.ErrNonEmptyGroup
	r := newResetModelWithSvc(t, svc)
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
	r := newResetModelWithSvc(t, svc)
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
	return newResetModelWithSvc(t, newFakeService())
}

func newResetModelWithSvc(t *testing.T, svc *fakeService) *groups.ResetModel {
	t.Helper()
	return groups.NewResetModel(groups.ResetOptions{
		Service: svc,
		Group:   "g1",
		Scope:   groups.ScopeWholeGroup{Group: "g1"},
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
	cmd := m.Update(keyPress("enter"))
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
	descN        int

	offsets    map[string][]kafka.PartitionLag
	offsetsErr error

	topicPartitions    map[string][]int32
	topicPartitionsErr error

	previewByGroup map[string]kafka.ResetPreview
	previewErr     error
	previewedNames []string

	commitByGroup  map[string]kafka.ResetPreview
	commitErr      error
	committedNames []string

	deleted   []string
	deleteErr error
}

// fakeRefreshRepo records save/load calls per screen id so tests can verify
// the mode-aware routing (list cadence saved under "groups", detail cadence
// under "group_detail").
type fakeRefreshRepo struct {
	stored map[string]time.Duration
	saves  []refreshSaveCall
}

type refreshSaveCall struct {
	screenID string
	value    time.Duration
}

func newFakeRefreshRepo() *fakeRefreshRepo {
	return &fakeRefreshRepo{stored: map[string]time.Duration{}}
}

func (r *fakeRefreshRepo) LoadRefreshInterval(_ context.Context, screenID string) (time.Duration, bool, error) {
	d, ok := r.stored[screenID]
	return d, ok, nil
}

func (r *fakeRefreshRepo) SaveRefreshInterval(_ context.Context, screenID string, d time.Duration) error {
	r.stored[screenID] = d
	r.saves = append(r.saves, refreshSaveCall{screenID: screenID, value: d})
	return nil
}

func TestNew_PersistedIntervalsOverrideDefaults(t *testing.T) {
	repo := newFakeRefreshRepo()
	repo.stored["groups"] = 45 * time.Second
	repo.stored["group_detail"] = 2 * time.Second

	m := groups.New(groups.Options{Service: newFakeService(), RefreshIntervals: repo})

	// list mode reads listRefresher.Interval(); detail's load is exercised
	// indirectly via the mode-aware picker test below.
	assert.Equal(t, 45*time.Second, m.RefreshInterval(),
		"list mode must reflect the persisted groups cadence")
}

func TestOpenRefreshPicker_ListMode_UsesGroupsScreenID(t *testing.T) {
	repo := newFakeRefreshRepo()
	m := groups.New(groups.Options{Service: newFakeService(), RefreshIntervals: repo})

	m.OpenRefreshPicker()
	require.True(t, m.HasOverlay(), "picker must register as overlay")
	// digit 3 = 3rd preset = 5s — distinct from the 30s default so the save
	// records a real transition.
	_ = m.Update(keyPress("3"))

	require.Len(t, repo.saves, 1)
	assert.Equal(t, "groups", repo.saves[0].screenID,
		"in ModeList the picker must save under the list cadence's key")
	assert.Equal(t, 5*time.Second, repo.saves[0].value)
}

func TestOpenRefreshPicker_DetailMode_UsesGroupDetailScreenID(t *testing.T) {
	svc := newFakeService()
	svc.groups = []kafka.GroupListInfo{{Group: "g1", State: "Stable"}}
	repo := newFakeRefreshRepo()
	// force both cadences to Manual via persistence so the construction-time
	// tick chains return nil cmds — otherwise [drive] would block waiting on
	// the real interval timers.
	repo.stored["groups"] = 0
	repo.stored["group_detail"] = 0
	m := groups.New(groups.Options{Service: svc, RefreshIntervals: repo})
	drive(t, m, m.Init())
	// keyPress("enter") flips the mode synchronously inside openDetail; the
	// returned cmds (detail load + tick bootstrap) aren't needed for the
	// picker-routing assertion and are intentionally dropped on the floor.
	_ = m.Update(keyPress("enter"))
	require.Equal(t, groups.ModeDetail, m.CurrentMode(),
		"precondition: detail view must be active for the mode-aware picker test")

	m.OpenRefreshPicker()
	_ = m.Update(keyPress("5")) // 30s

	require.Len(t, repo.saves, 1)
	assert.Equal(t, "group_detail", repo.saves[0].screenID,
		"in ModeDetail the picker must save under the detail cadence's key")
	assert.Equal(t, 30*time.Second, repo.saves[0].value)
}

func newFakeService() *fakeService {
	return &fakeService{
		filteredGroups:  map[string][]kafka.GroupListInfo{},
		descriptions:    map[string]kafka.GroupDescription{},
		offsets:         map[string][]kafka.PartitionLag{},
		topicPartitions: map[string][]int32{},
		previewByGroup:  map[string]kafka.ResetPreview{},
		commitByGroup:   map[string]kafka.ResetPreview{},
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
	f.descN++
	d, ok := f.descriptions[group]
	err := f.descErr
	f.mu.Unlock()
	if !ok && err == nil {
		return kafka.GroupDescription{Group: group}, nil
	}
	return d, err
}

// DescribeCalls returns how many times DescribeConsumerGroup was
// invoked. Used by tests that assert the detail-refresh ticker fired
// (or didn't, when paused).
func (f *fakeService) DescribeCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.descN
}

func (f *fakeService) GroupOffsets(_ context.Context, group string) ([]kafka.PartitionLag, error) {
	f.mu.Lock()
	out := append([]kafka.PartitionLag(nil), f.offsets[group]...)
	err := f.offsetsErr
	f.mu.Unlock()
	return out, err
}

func (f *fakeService) TopicsPartitions(_ context.Context, topics ...string) (map[string][]int32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string][]int32, len(topics))
	for _, t := range topics {
		if ps, ok := f.topicPartitions[t]; ok {
			out[t] = append([]int32(nil), ps...)
		}
	}
	return out, f.topicPartitionsErr
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
	case "ctrl+d":
		return tea.KeyPressMsg{Code: 'd', Mod: tea.ModCtrl}
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
