package kafka

import (
	"context"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestParsePartitionFilter(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   string
		want []int32
	}{
		{"", nil},
		{"  ", nil},
		{"0", []int32{0}},
		{"0,1,2", []int32{0, 1, 2}},
		{"0-4", []int32{0, 1, 2, 3, 4}},
		{"0-4,7,10-12", []int32{0, 1, 2, 3, 4, 7, 10, 11, 12}},
		{"3,1,2,3", []int32{1, 2, 3}},
		{" 0 - 2 , 5 ", []int32{0, 1, 2, 5}},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			got, err := ParsePartitionFilter(tc.in)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestParsePartitionFilter__invalid(t *testing.T) {
	t.Parallel()

	for _, in := range []string{"-1", "a", "1-", "5-3", "1,a", "1--2"} {
		t.Run(in, func(t *testing.T) {
			t.Parallel()
			_, err := ParsePartitionFilter(in)
			require.Error(t, err)
		})
	}
}

func TestParseTimestamp(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 27, 12, 30, 45, 0, time.UTC)

	cases := []struct {
		name string
		in   string
		want time.Time
	}{
		{"today", "today", time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC)},
		{"yesterday", "yesterday", time.Date(2026, 4, 26, 0, 0, 0, 0, time.UTC)},
		{"1h ago", "1h ago", now.Add(-time.Hour)},
		{"30m ago", "30m ago", now.Add(-30 * time.Minute)},
		{"45s ago", "45s ago", now.Add(-45 * time.Second)},
		{"2d ago", "2d ago", now.Add(-48 * time.Hour)},
		{"rfc3339", "2026-04-27T10:00:00Z", time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC)},
		{"plain dt", "2026-04-27 10:00:00", time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC)},
		{"date only", "2026-04-27", time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseTimestamp(tc.in, now)
			require.NoError(t, err)
			assert.True(t, got.Equal(tc.want), "got %v, want %v", got, tc.want)
		})
	}
}

func TestParseTimestamp__invalid(t *testing.T) {
	t.Parallel()
	for _, in := range []string{"", "garbage", "5x ago", "1h"} {
		t.Run(in, func(t *testing.T) {
			t.Parallel()
			_, err := ParseTimestamp(in, time.Now())
			require.Error(t, err)
		})
	}
}

func TestDetectValueFormat(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   []byte
		want ValueFormat
	}{
		{"empty", []byte{}, ValueFormatUTF8},
		{"json object", []byte(`{"k":1}`), ValueFormatJSON},
		{"json array", []byte(`[1,2,3]`), ValueFormatJSON},
		{"plain utf8", []byte("hello world"), ValueFormatUTF8},
		{"binary", []byte{0x00, 0x01, 0x02, 0xff}, ValueFormatBinary},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, DetectValueFormat(tc.in))
		})
	}
}

func TestClient_FetchLastN__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "fetch-last",
		Partitions:        2,
		ReplicationFactor: 1,
	}))

	produceN(t, ctx, c, "fetch-last", 6)

	msgs, err := c.FetchLastN(ctx, "fetch-last", 4, nil)
	require.NoError(t, err)
	assert.Len(t, msgs, 4)
	// newest-first ordering: every adjacent pair must be non-increasing.
	for i := 1; i < len(msgs); i++ {
		assert.False(t, msgs[i].Timestamp.After(msgs[i-1].Timestamp))
	}
}

func TestClient_FetchLastN__partitionFilter(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "fetch-filter",
		Partitions:        3,
		ReplicationFactor: 1,
	}))
	produceTo(t, ctx, c, "fetch-filter", []int32{0, 0, 1, 1, 2, 2})

	msgs, err := c.FetchLastN(ctx, "fetch-filter", 10, []int32{1})
	require.NoError(t, err)
	for _, m := range msgs {
		assert.EqualValues(t, 1, m.Partition)
	}
	assert.Len(t, msgs, 2)
}

func TestClient_FetchAtOffset__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "at-off",
		Partitions:        1,
		ReplicationFactor: 1,
	}))
	produceN(t, ctx, c, "at-off", 5)

	msgs, err := c.FetchAtOffset(ctx, "at-off", 0, 1, 3)
	require.NoError(t, err)
	require.Len(t, msgs, 3)
	for i, m := range msgs {
		assert.Equal(t, int64(i+1), m.Offset)
	}
}

func TestClient_FetchAtOffset__clamped(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "at-off-clamp",
		Partitions:        1,
		ReplicationFactor: 1,
	}))
	produceN(t, ctx, c, "at-off-clamp", 3)

	msgs, err := c.FetchAtOffset(ctx, "at-off-clamp", 0, -100, 10)
	require.NoError(t, err)
	assert.Len(t, msgs, 3)
}

func TestClient_FetchAtOffset__missingPartition(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "missing-p",
		Partitions:        1,
		ReplicationFactor: 1,
	}))

	_, err := c.FetchAtOffset(ctx, "missing-p", 7, 0, 1)
	require.Error(t, err)
}

func TestClient_FetchAtTimestamp__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "ts-fetch",
		Partitions:        1,
		ReplicationFactor: 1,
	}))
	// produce 3 records with explicit timestamps spaced 1 minute apart.
	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	records := []*kgo.Record{
		{Topic: "ts-fetch", Value: []byte("a"), Timestamp: base},
		{Topic: "ts-fetch", Value: []byte("b"), Timestamp: base.Add(time.Minute)},
		{Topic: "ts-fetch", Value: []byte("c"), Timestamp: base.Add(2 * time.Minute)},
	}
	require.NoError(t, c.kc.ProduceSync(ctx, records...).FirstErr())

	msgs, err := c.FetchAtTimestamp(ctx, "ts-fetch", base.Add(time.Minute), nil, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 2)
	values := []string{string(msgs[0].Value), string(msgs[1].Value)}
	assert.Equal(t, []string{"b", "c"}, values)
}

func TestClient_FetchEarlierLater__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "win",
		Partitions:        1,
		ReplicationFactor: 1,
	}))
	produceN(t, ctx, c, "win", 8)

	earlier, err := c.FetchEarlier(ctx, "win", map[int32]int64{0: 5}, 3, nil)
	require.NoError(t, err)
	require.Len(t, earlier, 3)
	assert.EqualValues(t, 2, earlier[0].Offset)
	assert.EqualValues(t, 4, earlier[len(earlier)-1].Offset)

	later, err := c.FetchLater(ctx, "win", map[int32]int64{0: 4}, 3, nil)
	require.NoError(t, err)
	require.Len(t, later, 3)
	assert.EqualValues(t, 5, later[0].Offset)
	assert.EqualValues(t, 7, later[len(later)-1].Offset)
}

func TestClient_FetchEarliest__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "earliest",
		Partitions:        1,
		ReplicationFactor: 1,
	}))
	produceN(t, ctx, c, "earliest", 5)

	msgs, err := c.FetchEarliest(ctx, "earliest", 3, nil)
	require.NoError(t, err)
	require.Len(t, msgs, 3)
	for i, m := range msgs {
		assert.Equal(t, int64(i), m.Offset)
	}
}

func TestClient_FetchAtOffsets__multiPartitionSingleClient(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "at-offsets",
		Partitions:        3,
		ReplicationFactor: 1,
	}))
	produceTo(t, ctx, c, "at-offsets", []int32{0, 0, 1, 1, 2, 2})

	msgs, err := c.FetchAtOffsets(ctx, "at-offsets", map[int32]int64{
		0: 0,
		1: 1,
	}, 5)
	require.NoError(t, err)
	parts := map[int32]int{}
	for _, m := range msgs {
		parts[m.Partition]++
	}
	assert.Equal(t, 2, parts[0])
	assert.Equal(t, 1, parts[1])
	assert.Zero(t, parts[2], "partition 2 was not requested")
}

func TestClient_FetchAtOffsets__clampsOutOfRange(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "at-offsets-clamp",
		Partitions:        1,
		ReplicationFactor: 1,
	}))
	produceN(t, ctx, c, "at-offsets-clamp", 3)

	msgs, err := c.FetchAtOffsets(ctx, "at-offsets-clamp", map[int32]int64{0: -50}, 5)
	require.NoError(t, err)
	assert.Len(t, msgs, 3)
}

func TestClient_WatermarksFor__filter(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "wmfor",
		Partitions:        3,
		ReplicationFactor: 1,
	}))
	produceTo(t, ctx, c, "wmfor", []int32{0, 1, 1, 2, 2, 2})

	got, err := c.WatermarksFor(ctx, "wmfor", []int32{1, 2})
	require.NoError(t, err)
	assert.Len(t, got, 2)
	assert.EqualValues(t, 2, got[1].High)
	assert.EqualValues(t, 3, got[2].High)
	_, ok := got[0]
	assert.False(t, ok, "partition 0 must be excluded by filter")

	all, err := c.WatermarksFor(ctx, "wmfor", nil)
	require.NoError(t, err)
	assert.Len(t, all, 3)
}

func TestClient_OffsetsForTimestamp__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "off-ts",
		Partitions:        1,
		ReplicationFactor: 1,
	}))
	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	records := []*kgo.Record{
		{Topic: "off-ts", Value: []byte("a"), Timestamp: base},
		{Topic: "off-ts", Value: []byte("b"), Timestamp: base.Add(time.Minute)},
		{Topic: "off-ts", Value: []byte("c"), Timestamp: base.Add(2 * time.Minute)},
	}
	require.NoError(t, c.kc.ProduceSync(ctx, records...).FirstErr())

	out, err := c.OffsetsForTimestamp(ctx, "off-ts", base.Add(time.Minute), nil)
	require.NoError(t, err)
	assert.EqualValues(t, 1, out[0])
}

func TestClient_Follow__kfake(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, c.CreateTopic(ctx, CreateTopicSpec{
		Name:              "live",
		Partitions:        1,
		ReplicationFactor: 1,
	}))

	session, err := c.Follow(ctx, "live", nil)
	require.NoError(t, err)
	t.Cleanup(session.Close)

	require.NoError(t, c.kc.ProduceSync(ctx,
		&kgo.Record{Topic: "live", Value: []byte("x")},
		&kgo.Record{Topic: "live", Value: []byte("y")},
	).FirstErr())

	got := collectMessages(t, session, 2, 10*time.Second)
	values := []string{string(got[0].Value), string(got[1].Value)}
	sort.Strings(values)
	assert.Equal(t, []string{"x", "y"}, values)
}

func TestClient_Follow__noPartitions__error(t *testing.T) {
	t.Parallel()

	c := newKfakeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	_, err := c.Follow(ctx, "absent", nil)
	require.Error(t, err)
}

func TestRecordToMessage__copiesBytes(t *testing.T) {
	t.Parallel()

	r := &kgo.Record{
		Topic:     "t",
		Partition: 1,
		Offset:    7,
		Timestamp: time.Unix(1, 0),
		Key:       []byte("k"),
		Value:     []byte("v"),
		Headers:   []kgo.RecordHeader{{Key: "h", Value: []byte("hv")}},
	}
	m := recordToMessage(r)
	r.Key[0] = 'x'
	r.Value[0] = 'x'
	r.Headers[0].Value[0] = 'x'

	assert.Equal(t, []byte("k"), m.Key)
	assert.Equal(t, []byte("v"), m.Value)
	require.Len(t, m.Headers, 1)
	assert.Equal(t, []byte("hv"), m.Headers[0].Value)
}

// produceN writes count records with values "msg-N" through a round-robin
// producer client, so partitions receive an even share regardless of how many
// records the topic has.
func produceN(t *testing.T, ctx context.Context, c *Client, topic string, count int) {
	t.Helper()
	opts, _, err := BuildClientOptions(c.cluster, DialOptions{
		ClientID: "produceN",
		ExtraOpts: []kgo.Opt{
			kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
			kgo.MetadataMinAge(10 * time.Millisecond),
		},
	})
	require.NoError(t, err)
	cl, err := kgo.NewClient(opts...)
	require.NoError(t, err)
	t.Cleanup(cl.Close)

	records := make([]*kgo.Record, count)
	for i := range count {
		records[i] = &kgo.Record{Topic: topic, Value: []byte("msg-" + strconv.Itoa(i))}
	}
	require.NoError(t, cl.ProduceSync(ctx, records...).FirstErr())
}

// produceTo writes one record per partition listed in `parts`. The producer
// uses a manual partitioner so the test can place records exactly.
func produceTo(t *testing.T, ctx context.Context, c *Client, topic string, parts []int32) {
	t.Helper()
	opts, _, err := BuildClientOptions(c.cluster, DialOptions{
		ClientID: "produceTo",
		ExtraOpts: []kgo.Opt{
			kgo.RecordPartitioner(kgo.ManualPartitioner()),
			kgo.MetadataMinAge(10 * time.Millisecond),
		},
	})
	require.NoError(t, err)
	cl, err := kgo.NewClient(opts...)
	require.NoError(t, err)
	t.Cleanup(cl.Close)

	records := make([]*kgo.Record, len(parts))
	for i, p := range parts {
		records[i] = &kgo.Record{Topic: topic, Partition: p, Value: []byte("p" + strconv.Itoa(int(p)))}
	}
	require.NoError(t, cl.ProduceSync(ctx, records...).FirstErr())
}

// collectMessages reads up to `n` messages from a follow session, failing the
// test if the deadline elapses first.
func collectMessages(t *testing.T, s *FollowSession, n int, timeout time.Duration) []Message {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	out := make([]Message, 0, n)
	for len(out) < n {
		select {
		case m, ok := <-s.Messages:
			if !ok {
				t.Fatal("follow channel closed before reaching n")
			}
			out = append(out, m)
		case err := <-s.Errors:
			if err != nil {
				t.Fatalf("follow error: %v", err)
			}
		case <-deadline.C:
			t.Fatalf("timed out waiting for %d follow messages, got %d", n, len(out))
		}
	}
	return out
}
