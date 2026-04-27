package kafka

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// known topic-config keys we surface in the UI.
const (
	ConfigCleanupPolicy    = "cleanup.policy"
	ConfigRetentionMs      = "retention.ms"
	ConfigMinInSyncReplica = "min.insync.replicas"
)

// TopicSummary is the lightweight per-topic snapshot used by the topics list
// screen. Lazy-loaded fields (size, configs, watermarks) live on dedicated
// helpers below.
type TopicSummary struct {
	Name       string
	Partitions int
	Replicas   int
	IsInternal bool
}

// PartitionDetail mirrors the metadata fields the topic-detail screen shows.
type PartitionDetail struct {
	Partition int32
	Leader    int32
	Replicas  []int32
	ISR       []int32
}

// TopicConfig is a single resolved topic-level config entry. Synonym chains
// (broker default → static default) are flattened into a Source string.
type TopicConfig struct {
	Key    string
	Value  string
	Source string
}

// TopicWatermarks holds the message-count math for one topic.
type TopicWatermarks struct {
	Partitions   map[int32]PartitionWatermarks
	MessageCount int64
}

// PartitionWatermarks holds the low/high offsets of a single partition.
type PartitionWatermarks struct {
	Low  int64
	High int64
}

// Count returns High - Low summed across all partitions.
func (w TopicWatermarks) Count() int64 { return w.MessageCount }

// ListTopics returns the cluster's topics. Internal topics are included so
// the UI can apply the `i` filter on top.
//
// The franz-go metadata cache is bypassed: list views must reflect very
// recent create / delete operations.
func (c *Client) ListTopics(ctx context.Context) ([]TopicSummary, error) {
	md, err := c.freshMetadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("kafka: list topics: %w", err)
	}
	out := make([]TopicSummary, 0, len(md.Topics))
	for _, t := range md.Topics.Sorted() {
		out = append(out, TopicSummary{
			Name:       t.Topic,
			Partitions: len(t.Partitions),
			Replicas:   t.Partitions.NumReplicas(),
			IsInternal: t.IsInternal,
		})
	}
	return out, nil
}

// freshMetadata issues a metadata request that bypasses the franz-go
// per-topic cache. We need uncached results in list views and after admin
// operations such as create / delete.
func (c *Client) freshMetadata(ctx context.Context, topics ...string) (kadm.Metadata, error) {
	req := kmsg.NewPtrMetadataRequest()
	if len(topics) == 0 {
		req.Topics = nil
	} else {
		for _, t := range topics {
			rt := kmsg.NewMetadataRequestTopic()
			rt.Topic = kmsg.StringPtr(t)
			req.Topics = append(req.Topics, rt)
		}
	}
	resp, err := req.RequestWith(ctx, c.kc)
	if err != nil {
		return kadm.Metadata{}, fmt.Errorf("metadata request: %w", err)
	}
	return decodeMetadata(resp), nil
}

// decodeMetadata converts a raw MetadataResponse into the kadm Metadata
// shape so callers can keep using the familiar types.
func decodeMetadata(resp *kmsg.MetadataResponse) kadm.Metadata {
	tds := make(kadm.TopicDetails, len(resp.Topics))
	for _, t := range resp.Topics {
		td := kadm.TopicDetail{
			ID:         t.TopicID,
			IsInternal: t.IsInternal,
			Partitions: make(kadm.PartitionDetails, len(t.Partitions)),
			Err:        kerr.ErrorForCode(t.ErrorCode),
		}
		if t.Topic != nil {
			td.Topic = *t.Topic
		}
		for _, p := range t.Partitions {
			td.Partitions[p.Partition] = kadm.PartitionDetail{
				Topic:           td.Topic,
				Partition:       p.Partition,
				Leader:          p.Leader,
				LeaderEpoch:     p.LeaderEpoch,
				Replicas:        p.Replicas,
				ISR:             p.ISR,
				OfflineReplicas: p.OfflineReplicas,
				Err:             kerr.ErrorForCode(p.ErrorCode),
			}
		}
		tds[td.Topic] = td
	}
	m := kadm.Metadata{
		Controller: resp.ControllerID,
		Topics:     tds,
	}
	if resp.ClusterID != nil {
		m.Cluster = *resp.ClusterID
	}
	return m
}

// TopicPartitions returns metadata for each partition of a topic. Used by the
// topic-configs screen.
func (c *Client) TopicPartitions(ctx context.Context, topic string) ([]PartitionDetail, error) {
	md, err := c.freshMetadata(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("kafka: metadata for %q: %w", topic, err)
	}
	td, ok := md.Topics[topic]
	if !ok {
		return nil, fmt.Errorf("kafka: topic %q not found", topic)
	}
	if td.Err != nil {
		return nil, fmt.Errorf("kafka: topic %q: %w", topic, td.Err)
	}
	parts := td.Partitions.Sorted()
	out := make([]PartitionDetail, len(parts))
	for i, p := range parts {
		out[i] = PartitionDetail{
			Partition: p.Partition,
			Leader:    p.Leader,
			Replicas:  p.Replicas,
			ISR:       p.ISR,
		}
	}
	return out, nil
}

// DescribeTopicConfigs returns the topic-level configs needed by the UI:
// `cleanup.policy`, `retention.ms`, `min.insync.replicas`. The returned slice
// preserves a stable order so the configs screen does not jitter.
func (c *Client) DescribeTopicConfigs(ctx context.Context, topic string) ([]TopicConfig, error) {
	rs, err := c.adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("kafka: describe configs for %q: %w", topic, err)
	}
	r, err := rs.On(topic, nil)
	if err != nil {
		return nil, fmt.Errorf("kafka: describe configs for %q: %w", topic, err)
	}
	if r.Err != nil {
		return nil, fmt.Errorf("kafka: describe configs for %q: %w", topic, r.Err)
	}

	wanted := []string{ConfigCleanupPolicy, ConfigRetentionMs, ConfigMinInSyncReplica}
	byName := make(map[string]kadm.Config, len(r.Configs))
	for _, cfg := range r.Configs {
		byName[cfg.Key] = cfg
	}

	out := make([]TopicConfig, 0, len(wanted))
	for _, key := range wanted {
		cfg, ok := byName[key]
		if !ok {
			continue
		}
		out = append(out, TopicConfig{
			Key:    cfg.Key,
			Value:  cfg.MaybeValue(),
			Source: cfg.Source.String(),
		})
	}
	return out, nil
}

// DescribeAllTopicConfigs returns the full config set for a topic — used by
// the dedicated configs screen (Task 13).
func (c *Client) DescribeAllTopicConfigs(ctx context.Context, topic string) ([]TopicConfig, error) {
	rs, err := c.adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("kafka: describe configs for %q: %w", topic, err)
	}
	r, err := rs.On(topic, nil)
	if err != nil {
		return nil, fmt.Errorf("kafka: describe configs for %q: %w", topic, err)
	}
	if r.Err != nil {
		return nil, fmt.Errorf("kafka: describe configs for %q: %w", topic, r.Err)
	}
	out := make([]TopicConfig, 0, len(r.Configs))
	for _, cfg := range r.Configs {
		out = append(out, TopicConfig{
			Key:    cfg.Key,
			Value:  cfg.MaybeValue(),
			Source: cfg.Source.String(),
		})
	}
	return out, nil
}

// TopicSize sums log-dir sizes (across all brokers and partitions) for the
// given topic. The returned int64 is the total on-disk size in bytes.
func (c *Client) TopicSize(ctx context.Context, topic string) (int64, error) {
	md, err := c.freshMetadata(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("kafka: metadata for %q: %w", topic, err)
	}
	td, ok := md.Topics[topic]
	if !ok {
		return 0, fmt.Errorf("kafka: topic %q not found", topic)
	}
	if td.Err != nil {
		return 0, fmt.Errorf("kafka: topic %q: %w", topic, td.Err)
	}
	set := kadm.TopicsSet{}
	for p := range td.Partitions {
		set.Add(topic, p)
	}
	dirs, err := c.adm.DescribeAllLogDirs(ctx, set)
	if err != nil {
		return 0, fmt.Errorf("kafka: describe log dirs for %q: %w", topic, err)
	}
	var total int64
	for _, perBroker := range dirs {
		for _, dir := range perBroker {
			if dir.Err != nil {
				continue
			}
			ps, ok := dir.Topics[topic]
			if !ok {
				continue
			}
			for _, p := range ps {
				if p.Size > 0 {
					total += p.Size
				}
			}
		}
	}
	return total, nil
}

// TopicWatermarks returns per-partition low/high watermarks plus the
// implied message count (sum of high-low). Used by the topics list and by
// the messages screen for windowing.
func (c *Client) TopicWatermarks(ctx context.Context, topic string) (TopicWatermarks, error) {
	starts, err := c.adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return TopicWatermarks{}, fmt.Errorf("kafka: list start offsets %q: %w", topic, err)
	}
	ends, err := c.adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return TopicWatermarks{}, fmt.Errorf("kafka: list end offsets %q: %w", topic, err)
	}

	startMap := starts[topic]
	endMap := ends[topic]
	w := TopicWatermarks{Partitions: make(map[int32]PartitionWatermarks, len(endMap))}
	for p, e := range endMap {
		if e.Err != nil {
			continue
		}
		s, ok := startMap[p]
		if !ok || s.Err != nil {
			continue
		}
		pw := PartitionWatermarks{Low: s.Offset, High: e.Offset}
		w.Partitions[p] = pw
		if pw.High > pw.Low {
			w.MessageCount += pw.High - pw.Low
		}
	}
	return w, nil
}

// CreateTopicSpec describes the topic to create.
type CreateTopicSpec struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Configs           map[string]string
}

// CreateTopic creates a topic with the given spec. Pass partitions=-1 and
// replicationFactor=-1 to use broker defaults (Kafka 2.4+).
func (c *Client) CreateTopic(ctx context.Context, spec CreateTopicSpec) error {
	configs := make(map[string]*string, len(spec.Configs))
	for k, v := range spec.Configs {
		configs[k] = &v
	}
	resp, err := c.adm.CreateTopic(ctx, spec.Partitions, spec.ReplicationFactor, configs, spec.Name)
	if err != nil {
		return fmt.Errorf("kafka: create topic %q: %w", spec.Name, err)
	}
	if resp.Err != nil {
		return fmt.Errorf("kafka: create topic %q: %w", spec.Name, resp.Err)
	}
	return nil
}

// DeleteTopic deletes a single topic.
func (c *Client) DeleteTopic(ctx context.Context, topic string) error {
	resp, err := c.adm.DeleteTopic(ctx, topic)
	if err != nil {
		return fmt.Errorf("kafka: delete topic %q: %w", topic, err)
	}
	if resp.Err != nil {
		return fmt.Errorf("kafka: delete topic %q: %w", topic, resp.Err)
	}
	return nil
}

// CloneProgress reports incremental progress while cloning a topic. The
// caller may receive on the channel to drive a UI progress bar.
type CloneProgress struct {
	Total  int64
	Copied int64
	Done   bool
	Err    error
}

// CloneOptions tweaks the source-topic configs that should be carried over
// to the destination during a clone.
type CloneOptions struct {
	// CopyConfigs, when non-nil, is the explicit set of topic-level configs
	// applied to the destination. When nil, no extra configs are copied.
	CopyConfigs map[string]string
	// ReplicationFactor for the destination. When zero, it falls back to the
	// source topic's replication factor.
	ReplicationFactor int16
}

// CloneTopic copies all messages from src into a freshly-created dst topic.
//
// The destination is created with the same partition count as the source. The
// caller can monitor progress via the returned channel; the channel is closed
// once cloning has finished (success or error).
func (c *Client) CloneTopic(ctx context.Context, src, dst string, opts CloneOptions) (<-chan CloneProgress, error) {
	md, err := c.freshMetadata(ctx, src)
	if err != nil {
		return nil, fmt.Errorf("kafka: clone %q→%q: source metadata: %w", src, dst, err)
	}
	srcDetail, ok := md.Topics[src]
	if !ok || srcDetail.Err != nil {
		return nil, fmt.Errorf("kafka: clone %q→%q: source not found", src, dst)
	}
	partitions, err := safeInt32(len(srcDetail.Partitions))
	if err != nil {
		return nil, fmt.Errorf("kafka: clone %q→%q: %w", src, dst, err)
	}
	rf := opts.ReplicationFactor
	if rf == 0 {
		rep, repErr := safeInt16(srcDetail.Partitions.NumReplicas())
		if repErr != nil {
			return nil, fmt.Errorf("kafka: clone %q→%q: %w", src, dst, repErr)
		}
		rf = rep
		if rf == 0 {
			rf = 1
		}
	}

	if createErr := c.CreateTopic(ctx, CreateTopicSpec{
		Name:              dst,
		Partitions:        partitions,
		ReplicationFactor: rf,
		Configs:           opts.CopyConfigs,
	}); createErr != nil {
		return nil, createErr
	}

	wm, err := c.TopicWatermarks(ctx, src)
	if err != nil {
		return nil, fmt.Errorf("kafka: clone %q→%q: watermarks: %w", src, dst, err)
	}

	progress := make(chan CloneProgress, 8)
	go c.runClone(ctx, src, dst, wm, progress)
	return progress, nil
}

func (c *Client) runClone(
	ctx context.Context,
	src, dst string,
	wm TopicWatermarks,
	progress chan<- CloneProgress,
) {
	defer close(progress)

	total := wm.MessageCount
	if total == 0 {
		progress <- CloneProgress{Total: 0, Copied: 0, Done: true}
		return
	}

	opts, _, err := BuildClientOptions(c.cluster, DialOptions{
		ClientID: DefaultClientID + "-clone",
		ExtraOpts: []kgo.Opt{
			kgo.ConsumeTopics(src),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		},
	})
	if err != nil {
		progress <- CloneProgress{Total: total, Err: err, Done: true}
		return
	}
	worker, err := kgo.NewClient(opts...)
	if err != nil {
		progress <- CloneProgress{Total: total, Err: fmt.Errorf("kafka: clone worker: %w", err), Done: true}
		return
	}
	defer worker.Close()

	endsByPartition := make(map[int32]int64, len(wm.Partitions))
	for p, w := range wm.Partitions {
		endsByPartition[p] = w.High
	}

	var copied atomic.Int64
	for {
		fetches := worker.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			progress <- CloneProgress{Total: total, Copied: copied.Load(), Err: errs[0].Err, Done: true}
			return
		}

		var batch []*kgo.Record
		fetches.EachRecord(func(r *kgo.Record) {
			rec := &kgo.Record{
				Topic:     dst,
				Partition: r.Partition,
				Key:       r.Key,
				Value:     r.Value,
				Headers:   r.Headers,
			}
			batch = append(batch, rec)
		})
		if len(batch) > 0 {
			if results := worker.ProduceSync(ctx, batch...); results.FirstErr() != nil {
				progress <- CloneProgress{Total: total, Copied: copied.Load(), Err: results.FirstErr(), Done: true}
				return
			}
			copied.Add(int64(len(batch)))
			progress <- CloneProgress{Total: total, Copied: copied.Load()}
		}

		if reachedEnd(fetches, endsByPartition) {
			progress <- CloneProgress{Total: total, Copied: copied.Load(), Done: true}
			return
		}

		if err := ctx.Err(); err != nil {
			progress <- CloneProgress{Total: total, Copied: copied.Load(), Err: err, Done: true}
			return
		}
	}
}

// safeInt32 narrows an int produced by a Kafka response into an int32 with an
// explicit bounds check (kadm reports counts as len() but the wire protocol
// is int32-bounded).
func safeInt32(n int) (int32, error) {
	const maxInt32 = int(^uint32(0) >> 1)
	if n < 0 || n > maxInt32 {
		return 0, fmt.Errorf("value %d out of int32 range", n)
	}
	return int32(n), nil
}

// safeInt16 narrows an int into int16 with the same guard as safeInt32.
func safeInt16(n int) (int16, error) {
	const maxInt16 = int(^uint16(0) >> 1)
	if n < 0 || n > maxInt16 {
		return 0, fmt.Errorf("value %d out of int16 range", n)
	}
	return int16(n), nil
}

// reachedEnd returns true when all partitions whose high watermark we
// recorded have been consumed up to (or past) that watermark.
func reachedEnd(fetches kgo.Fetches, ends map[int32]int64) bool {
	if len(ends) == 0 {
		return true
	}
	progressed := map[int32]int64{}
	fetches.EachPartition(func(ftp kgo.FetchTopicPartition) {
		// HighWatermark + LogStartOffset are populated when the broker
		// reports them. We only care about the latest fetched offset.
		if n := len(ftp.Records); n > 0 {
			last := ftp.Records[n-1]
			progressed[last.Partition] = last.Offset + 1
		}
	})
	for p, end := range ends {
		offset, seen := progressed[p]
		if !seen {
			// no records yet for this partition – check if it was already empty
			if end == 0 {
				continue
			}
			return false
		}
		if offset < end {
			return false
		}
	}
	return true
}
