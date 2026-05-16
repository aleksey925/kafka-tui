package kafka

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestResetStrategy_String(t *testing.T) {
	t.Parallel()

	cases := []struct {
		s    ResetStrategy
		want string
	}{
		{ResetEarliest, "earliest"},
		{ResetLatest, "latest"},
		{ResetShift, "shift"},
		{ResetTimestamp, "timestamp"},
		{ResetSpecific, "specific"},
		{ResetStrategy(99), "unknown"},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, tc.s.String())
	}
}

func TestClampOffset(t *testing.T) {
	t.Parallel()

	cases := []struct {
		want, low, high int64
		out             int64
		note            string
	}{
		{5, 0, 10, 5, ""},
		{-1, 0, 10, 0, ResetNoteClampedLow},
		{15, 0, 10, 10, ResetNoteClampedHigh},
		{0, 0, 0, 0, ""},
	}
	for _, tc := range cases {
		got, note := clampOffset(tc.want, tc.low, tc.high)
		assert.Equal(t, tc.out, got)
		assert.Equal(t, tc.note, note)
	}
}

func TestComputeResetTarget(t *testing.T) {
	t.Parallel()

	tp := TopicPartition{Topic: "t", Partition: 0}
	committed := int64(100)
	low, high := int64(50), int64(200)

	cases := []struct {
		name     string
		spec     ResetSpec
		wantTgt  int64
		wantNote string
	}{
		{"earliest", ResetSpec{Strategy: ResetEarliest}, low, ""},
		{"latest", ResetSpec{Strategy: ResetLatest}, high, ""},
		{"shift forward", ResetSpec{Strategy: ResetShift, Shift: 10}, 110, ""},
		{"shift backward", ResetSpec{Strategy: ResetShift, Shift: -10}, 90, ""},
		{"shift clamp low", ResetSpec{Strategy: ResetShift, Shift: -1000}, low, ResetNoteClampedLow},
		{"shift clamp high", ResetSpec{Strategy: ResetShift, Shift: 1000}, high, ResetNoteClampedHigh},
		{"specific", ResetSpec{Strategy: ResetSpecific, Offset: 150}, 150, ""},
		{"specific clamp low", ResetSpec{Strategy: ResetSpecific, Offset: 1}, low, ResetNoteClampedLow},
		{"specific clamp high", ResetSpec{Strategy: ResetSpecific, Offset: 9999}, high, ResetNoteClampedHigh},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, note, err := computeResetTarget(tp, tc.spec, committed, low, high, nil)
			require.NoError(t, err)
			assert.Equal(t, tc.wantTgt, got)
			assert.Equal(t, tc.wantNote, note)
		})
	}
}

func TestComputeResetTarget__shiftWithoutCommit__usesLow(t *testing.T) {
	t.Parallel()

	tp := TopicPartition{Topic: "t", Partition: 0}
	got, note, err := computeResetTarget(tp, ResetSpec{Strategy: ResetShift, Shift: 5}, -1, 50, 200, nil)
	require.NoError(t, err)
	assert.EqualValues(t, 55, got)
	assert.Empty(t, note)
}

func TestComputeResetTarget__timestamp(t *testing.T) {
	t.Parallel()

	tp := TopicPartition{Topic: "t", Partition: 0}
	at := kadm.ListedOffsets{
		"t": {0: kadm.ListedOffset{Topic: "t", Partition: 0, Offset: 120}},
		"u": {0: kadm.ListedOffset{Topic: "u", Partition: 0, Offset: -1}},
	}
	got, note, err := computeResetTarget(tp, ResetSpec{Strategy: ResetTimestamp}, 100, 50, 200, at)
	require.NoError(t, err)
	assert.EqualValues(t, 120, got)
	assert.Empty(t, note)

	// partition with -1 (no record at-or-after timestamp) → high with note
	got, note, err = computeResetTarget(TopicPartition{Topic: "u", Partition: 0}, ResetSpec{Strategy: ResetTimestamp}, 0, 50, 200, at)
	require.NoError(t, err)
	assert.EqualValues(t, 200, got)
	assert.Equal(t, ResetNoteTimestampNoAfter, note)

	// partition missing entirely → fallback to high with note
	got, note, err = computeResetTarget(TopicPartition{Topic: "missing", Partition: 0}, ResetSpec{Strategy: ResetTimestamp}, 0, 50, 200, at)
	require.NoError(t, err)
	assert.EqualValues(t, 200, got)
	assert.Equal(t, ResetNoteTimestampNoAfter, note)

	// empty partition (low == high) → low with NoBefore note
	got, note, err = computeResetTarget(tp, ResetSpec{Strategy: ResetTimestamp}, 0, 50, 50, kadm.ListedOffsets{})
	require.NoError(t, err)
	assert.EqualValues(t, 50, got)
	assert.Equal(t, ResetNoteTimestampNoBefore, note)
}

func TestResolveResetTargets__explicit(t *testing.T) {
	t.Parallel()

	in := []TopicPartition{
		{Topic: "a", Partition: 0},
		{Topic: "a", Partition: 1},
		{Topic: "a", Partition: 0}, // dup
	}
	out := resolveResetTargets(in, kadm.OffsetResponses{})
	assert.Equal(t, []TopicPartition{
		{Topic: "a", Partition: 0},
		{Topic: "a", Partition: 1},
	}, out)
}

func TestResolveResetTargets__fromCommits(t *testing.T) {
	t.Parallel()

	commits := kadm.OffsetResponses{}
	commits.Add(kadm.OffsetResponse{Offset: kadm.Offset{Topic: "x", Partition: 0, At: 5}})
	commits.Add(kadm.OffsetResponse{Offset: kadm.Offset{Topic: "x", Partition: 1, At: 7}})
	commits.Add(kadm.OffsetResponse{Offset: kadm.Offset{Topic: "y", Partition: 0, At: 1}})
	out := resolveResetTargets(nil, commits)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	assert.Equal(t, []TopicPartition{
		{Topic: "x", Partition: 0},
		{Topic: "x", Partition: 1},
		{Topic: "y", Partition: 0},
	}, out)
}

func TestUniqueTopicsFromTargets(t *testing.T) {
	t.Parallel()

	got := uniqueTopicsFromTargets([]TopicPartition{
		{Topic: "b", Partition: 0},
		{Topic: "a", Partition: 0},
		{Topic: "b", Partition: 1},
	})
	assert.Equal(t, []string{"a", "b"}, got)
}

func TestIsNonEmptyGroup(t *testing.T) {
	t.Parallel()

	assert.False(t, IsNonEmptyGroup(nil))
	assert.False(t, IsNonEmptyGroup(errors.New("other")))
	assert.True(t, IsNonEmptyGroup(ErrNonEmptyGroup))
	assert.True(t, IsNonEmptyGroup(kerr.NonEmptyGroup))
}

func TestClient_ListConsumerGroups__empty__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	groups, err := c.ListConsumerGroups(ctx)
	require.NoError(t, err)
	assert.Empty(t, groups)
}

func TestClient_ListConsumerGroups__withCommits__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "t1",
		Partitions:        2,
		ReplicationFactor: 1,
	}))
	produceN(t, ctx, c, "t1", 4)
	commitOffset(t, ctx, c, "g1", "t1", 0, 1)
	commitOffset(t, ctx, c, "g2", "t1", 0, 0)

	groups, err := c.ListConsumerGroups(ctx)
	require.NoError(t, err)
	names := make([]string, len(groups))
	for i, g := range groups {
		names[i] = g.Group
	}
	assert.Equal(t, []string{"g1", "g2"}, names)
}

func TestClient_DescribeConsumerGroup__empty__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "t-desc",
		Partitions:        1,
		ReplicationFactor: 1,
	}))
	produceN(t, ctx, c, "t-desc", 1)
	commitOffset(t, ctx, c, "desc-g", "t-desc", 0, 0)

	d, err := c.DescribeConsumerGroup(ctx, "desc-g")
	require.NoError(t, err)
	assert.Equal(t, "desc-g", d.Group)
	assert.Equal(t, "Empty", d.State)
	assert.Empty(t, d.Members)
}

func TestClient_DescribeConsumerGroup__missing__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	_, err := c.DescribeConsumerGroup(ctx, "no-such")
	require.Error(t, err)
}

func TestClient_GroupOffsets__lagComputation__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "lag-topic",
		Partitions:        1,
		ReplicationFactor: 1,
	}))
	produceN(t, ctx, c, "lag-topic", 10)
	commitOffset(t, ctx, c, "lag-g", "lag-topic", 0, 7)

	rows, err := c.GroupOffsets(ctx, "lag-g")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "lag-topic", rows[0].Topic)
	assert.EqualValues(t, 0, rows[0].Partition)
	assert.EqualValues(t, 7, rows[0].Committed)
	assert.EqualValues(t, 10, rows[0].End)
	assert.EqualValues(t, 3, rows[0].Lag)
}

func TestClient_FilterGroupsByTopic__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	for _, n := range []string{"alpha", "beta"} {
		require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
			Name:              n,
			Partitions:        1,
			ReplicationFactor: 1,
		}))
		produceN(t, ctx, c, n, 1)
	}
	commitOffset(t, ctx, c, "ga", "alpha", 0, 0)
	commitOffset(t, ctx, c, "gb", "beta", 0, 0)
	commitOffset(t, ctx, c, "gboth", "alpha", 0, 0)
	commitOffset(t, ctx, c, "gboth", "beta", 0, 0)

	got, err := c.FilterGroupsByTopic(ctx, "alpha")
	require.NoError(t, err)
	names := make([]string, len(got))
	for i, g := range got {
		names[i] = g.Group
	}
	sort.Strings(names)
	assert.Equal(t, []string{"ga", "gboth"}, names)
}

func TestClient_PreviewReset__earliestLatest__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "rst-el",
		Partitions:        1,
		ReplicationFactor: 1,
	}))
	produceN(t, ctx, c, "rst-el", 5)
	commitOffset(t, ctx, c, "g-el", "rst-el", 0, 3)

	pv, err := c.PreviewReset(ctx, "g-el", ResetSpec{Strategy: ResetEarliest})
	require.NoError(t, err)
	require.Len(t, pv.Partitions, 1)
	assert.EqualValues(t, 0, pv.Partitions[0].Target)
	assert.EqualValues(t, -3, pv.Partitions[0].Diff)
	assert.EqualValues(t, 3, pv.Summary.Reconsume)

	pv, err = c.PreviewReset(ctx, "g-el", ResetSpec{Strategy: ResetLatest})
	require.NoError(t, err)
	assert.EqualValues(t, 5, pv.Partitions[0].Target)
	assert.EqualValues(t, 2, pv.Partitions[0].Diff)
	assert.EqualValues(t, 2, pv.Summary.Skipped)
}

// TestClient_PreviewReset__topicScopeAllPartitions__kfake pins the
// end-to-end scope contract: when the caller supplies an explicit
// per-topic Targets list (as a topic-scope reset does), the preview
// must enumerate every requested partition — not silently fall back to
// "every partition with a commit". Previously the UI captured Targets
// from the group's commits only, which left freshly-rebalanced
// partitions out of the reset.
func TestClient_PreviewReset__topicScopeAllPartitions__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "wide",
		Partitions:        4,
		ReplicationFactor: 1,
	}))
	// produce a couple records into partition 0 only — the group only
	// commits there, but the reset scope explicitly targets all four.
	produceN(t, ctx, c, "wide", 2)
	commitOffset(t, ctx, c, "g-wide", "wide", 0, 1)

	spec := ResetSpec{
		Strategy: ResetEarliest,
		Targets: []TopicPartition{
			{Topic: "wide", Partition: 0},
			{Topic: "wide", Partition: 1},
			{Topic: "wide", Partition: 2},
			{Topic: "wide", Partition: 3},
		},
	}
	pv, err := c.PreviewReset(ctx, "g-wide", spec)
	require.NoError(t, err)
	require.Len(t, pv.Partitions, 4, "preview must include every partition in scope, not just those with commits")
	for i, p := range pv.Partitions {
		assert.EqualValues(t, i, p.Partition)
	}
}

func TestClient_PreviewReset__shiftClamping__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "rst-shift",
		Partitions:        1,
		ReplicationFactor: 1,
	}))
	produceN(t, ctx, c, "rst-shift", 5)
	commitOffset(t, ctx, c, "g-shift", "rst-shift", 0, 2)

	pv, err := c.PreviewReset(ctx, "g-shift", ResetSpec{Strategy: ResetShift, Shift: -10})
	require.NoError(t, err)
	require.Len(t, pv.Partitions, 1)
	assert.EqualValues(t, 0, pv.Partitions[0].Target)
	assert.Equal(t, ResetNoteClampedLow, pv.Partitions[0].Note)

	pv, err = c.PreviewReset(ctx, "g-shift", ResetSpec{Strategy: ResetShift, Shift: 100})
	require.NoError(t, err)
	assert.EqualValues(t, 5, pv.Partitions[0].Target)
	assert.Equal(t, ResetNoteClampedHigh, pv.Partitions[0].Note)
}

func TestClient_PreviewReset__specificClamping__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "rst-spec",
		Partitions:        1,
		ReplicationFactor: 1,
	}))
	produceN(t, ctx, c, "rst-spec", 5)
	commitOffset(t, ctx, c, "g-spec", "rst-spec", 0, 1)

	pv, err := c.PreviewReset(ctx, "g-spec", ResetSpec{Strategy: ResetSpecific, Offset: 9999})
	require.NoError(t, err)
	require.Len(t, pv.Partitions, 1)
	assert.EqualValues(t, 5, pv.Partitions[0].Target)
	assert.Equal(t, ResetNoteClampedHigh, pv.Partitions[0].Note)
}

func TestClient_PreviewReset__targets__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "tgt",
		Partitions:        3,
		ReplicationFactor: 1,
	}))
	produceTo(t, context.Background(), c, "tgt", []int32{0, 0, 1, 2, 2})
	commitOffset(t, ctx, c, "g-tgt", "tgt", 0, 0)
	commitOffset(t, ctx, c, "g-tgt", "tgt", 1, 0)
	commitOffset(t, ctx, c, "g-tgt", "tgt", 2, 0)

	// only ask for partition 1
	pv, err := c.PreviewReset(ctx, "g-tgt", ResetSpec{
		Strategy: ResetLatest,
		Targets:  []TopicPartition{{Topic: "tgt", Partition: 1}},
	})
	require.NoError(t, err)
	require.Len(t, pv.Partitions, 1)
	assert.EqualValues(t, 1, pv.Partitions[0].Partition)
}

func TestClient_ResetOffsets__commits__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "rst-do",
		Partitions:        1,
		ReplicationFactor: 1,
	}))
	produceN(t, ctx, c, "rst-do", 6)
	commitOffset(t, ctx, c, "g-do", "rst-do", 0, 4)

	pv, err := c.ResetOffsets(ctx, "g-do", ResetSpec{Strategy: ResetEarliest})
	require.NoError(t, err)
	require.Len(t, pv.Partitions, 1)
	assert.EqualValues(t, 0, pv.Partitions[0].Target)

	rows, err := c.GroupOffsets(ctx, "g-do")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.EqualValues(t, 0, rows[0].Committed)
	assert.EqualValues(t, 6, rows[0].End)
	assert.EqualValues(t, 6, rows[0].Lag)
}

func TestClient_DeleteConsumerGroup__empty__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "del-t",
		Partitions:        1,
		ReplicationFactor: 1,
	}))
	produceN(t, ctx, c, "del-t", 1)
	commitOffset(t, ctx, c, "del-g", "del-t", 0, 0)

	require.NoError(t, c.DeleteConsumerGroup(ctx, "del-g"))

	groups, err := c.ListConsumerGroups(ctx)
	require.NoError(t, err)
	for _, g := range groups {
		assert.NotEqual(t, "del-g", g.Group)
	}
}

func TestClient_DeleteConsumerGroup__nonEmpty__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "del-active",
		Partitions:        1,
		ReplicationFactor: 1,
	}))
	produceN(t, ctx, c, "del-active", 3)

	stop := startGroupConsumer(t, c, "active-g", "del-active")
	t.Cleanup(stop)
	waitForGroupState(t, ctx, c, "active-g", "Stable")

	err := c.DeleteConsumerGroup(ctx, "active-g")
	require.Error(t, err)
	assert.True(t, IsNonEmptyGroup(err))
}

func TestClient_ResetOffsets__nonEmptyGroup__error(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "rst-active",
		Partitions:        1,
		ReplicationFactor: 1,
	}))
	produceN(t, ctx, c, "rst-active", 2)

	stop := startGroupConsumer(t, c, "active-rst", "rst-active")
	t.Cleanup(stop)
	waitForGroupState(t, ctx, c, "active-rst", "Stable")

	_, err := c.PreviewReset(ctx, "active-rst", ResetSpec{Strategy: ResetEarliest})
	require.Error(t, err)
	assert.True(t, IsNonEmptyGroup(err))
}

func TestClient_DescribeConsumerGroup__withMember__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "desc-active",
		Partitions:        1,
		ReplicationFactor: 1,
	}))
	produceN(t, ctx, c, "desc-active", 1)

	stop := startGroupConsumer(t, c, "desc-act-g", "desc-active")
	t.Cleanup(stop)
	waitForGroupState(t, ctx, c, "desc-act-g", "Stable")

	d, err := c.DescribeConsumerGroup(ctx, "desc-act-g")
	require.NoError(t, err)
	assert.Equal(t, "Stable", d.State)
	require.NotEmpty(t, d.Members)
	assert.Equal(t, "consumer", d.ProtocolType)
	found := false
	for _, m := range d.Members {
		for _, t := range m.Topics {
			if t == "desc-active" {
				found = true
			}
		}
	}
	assert.True(t, found, "expected member to subscribe to desc-active")
}

// commitOffset issues a "simple" (non-member) offset commit so a group with
// the given offset shows up in subsequent admin queries. Useful because kfake
// instantiates a group on first commit.
func commitOffset(t *testing.T, ctx context.Context, c *Client, group, topic string, partition int32, offset int64) {
	t.Helper()
	offs := make(kadm.Offsets)
	offs.Add(kadm.Offset{
		Topic:       topic,
		Partition:   partition,
		At:          offset,
		LeaderEpoch: -1,
	})
	require.NoError(t, c.adm.CommitAllOffsets(ctx, group, offs))
}

// startGroupConsumer spins up a real kgo consumer that joins `group`, polls
// fetches in the background, and is closed by the returned stop func.
func startGroupConsumer(t *testing.T, c *Client, group, topic string) func() {
	t.Helper()
	opts, _, err := BuildClientOptions(c.cluster, DialOptions{
		ClientID: "group-consumer-" + group,
		ExtraOpts: []kgo.Opt{
			kgo.ConsumerGroup(group),
			kgo.ConsumeTopics(topic),
			kgo.DisableAutoCommit(),
			kgo.MetadataMinAge(10 * time.Millisecond),
			kgo.RebalanceTimeout(2 * time.Second),
		},
	})
	require.NoError(t, err)
	cl, err := kgo.NewClient(opts...)
	require.NoError(t, err)

	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer close(done)
		for {
			fetches := cl.PollFetches(ctx)
			if ctx.Err() != nil {
				return
			}
			fetches.EachError(func(string, int32, error) {})
		}
	}()
	return func() {
		cancel()
		<-done
		cl.Close()
	}
}

// waitForGroupState polls DescribeConsumerGroup until the group reports the
// requested state or the context expires.
func waitForGroupState(t *testing.T, ctx context.Context, c *Client, group, state string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		d, err := c.DescribeConsumerGroup(ctx, group)
		if err == nil && d.State == state {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for group %q state %q: %v", group, state, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatalf("group %q did not reach state %q within deadline", group, state)
}
