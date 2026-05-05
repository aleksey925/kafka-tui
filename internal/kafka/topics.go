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

type TopicSummary struct {
	Name       string
	Partitions int
	Replicas   int
	IsInternal bool
}

type PartitionDetail struct {
	Partition int32
	Leader    int32
	Replicas  []int32
	ISR       []int32
}

// TopicConfig is a single resolved topic-level config entry. Synonym chains
// (broker default → static default) are flattened into Source.
type TopicConfig struct {
	Key    string
	Value  string
	Source string
}

type TopicWatermarks struct {
	Partitions   map[int32]PartitionWatermarks
	MessageCount int64
}

type PartitionWatermarks struct {
	Low  int64
	High int64
}

func (w TopicWatermarks) Count() int64 { return w.MessageCount }

// ListTopics returns the cluster's topics, internal ones included.
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

// freshMetadata bypasses the franz-go per-topic cache, needed in list views
// and after admin operations such as create / delete.
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

// DescribeTopicConfigs returns the UI-relevant topic configs (cleanup.policy,
// retention.ms, min.insync.replicas) in stable order.
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

// DescribeAllTopicConfigs returns the full config set for a topic.
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

// TopicSize sums log-dir sizes across all brokers and partitions in bytes.
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
// implied message count (sum of high-low).
func (c *Client) TopicWatermarks(ctx context.Context, topic string) (TopicWatermarks, error) {
	all, err := c.TopicWatermarksBatch(ctx, topic)
	if err != nil {
		// preserve topic name in error — batch helper drops it.
		return TopicWatermarks{}, fmt.Errorf("kafka: watermarks for %q: %w", topic, err)
	}
	w, ok := all[topic]
	if !ok {
		return TopicWatermarks{Partitions: map[int32]PartitionWatermarks{}}, nil
	}
	return w, nil
}

// TopicWatermarksBatch fetches per-partition low/high watermarks for many
// topics in two RPCs (ListStartOffsets, ListEndOffsets). Topics with
// per-topic broker errors are silently dropped from the result map. An
// empty topics list returns an empty map without contacting the broker.
func (c *Client) TopicWatermarksBatch(ctx context.Context, topics ...string) (map[string]TopicWatermarks, error) {
	out := make(map[string]TopicWatermarks, len(topics))
	if len(topics) == 0 {
		return out, nil
	}
	starts, err := c.adm.ListStartOffsets(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("kafka: list start offsets: %w", err)
	}
	ends, err := c.adm.ListEndOffsets(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("kafka: list end offsets: %w", err)
	}
	for _, t := range topics {
		startMap := starts[t]
		endMap := ends[t]
		if len(endMap) == 0 {
			continue
		}
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
		out[t] = w
	}
	return out, nil
}

// TopicSizesBatch returns total on-disk size (bytes) per topic via one
// Metadata RPC plus one DescribeAllLogDirs covering every (topic, partition).
func (c *Client) TopicSizesBatch(ctx context.Context, topics ...string) (map[string]int64, error) {
	out := make(map[string]int64, len(topics))
	if len(topics) == 0 {
		return out, nil
	}
	md, err := c.freshMetadata(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("kafka: metadata: %w", err)
	}
	set := kadm.TopicsSet{}
	for _, t := range topics {
		td, ok := md.Topics[t]
		if !ok || td.Err != nil {
			continue
		}
		for p := range td.Partitions {
			set.Add(t, p)
		}
	}
	if len(set) == 0 {
		return out, nil
	}
	dirs, err := c.adm.DescribeAllLogDirs(ctx, set)
	if err != nil {
		return nil, fmt.Errorf("kafka: describe log dirs: %w", err)
	}
	for _, perBroker := range dirs {
		for _, dir := range perBroker {
			if dir.Err != nil {
				continue
			}
			for topic, parts := range dir.Topics {
				for _, p := range parts {
					if p.Size > 0 {
						out[topic] += p.Size
					}
				}
			}
		}
	}
	return out, nil
}

// DescribeTopicConfigsBatch fetches the UI-relevant configs for many topics
// in a single RPC. Per-topic errors inside the batch response (e.g. ACL
// denied for one topic only) are silently dropped.
func (c *Client) DescribeTopicConfigsBatch(ctx context.Context, topics ...string) (map[string][]TopicConfig, error) {
	out := make(map[string][]TopicConfig, len(topics))
	if len(topics) == 0 {
		return out, nil
	}
	rs, err := c.adm.DescribeTopicConfigs(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("kafka: describe configs: %w", err)
	}
	wanted := []string{ConfigCleanupPolicy, ConfigRetentionMs, ConfigMinInSyncReplica}
	for _, r := range rs {
		if r.Err != nil {
			continue
		}
		byName := make(map[string]kadm.Config, len(r.Configs))
		for _, cfg := range r.Configs {
			byName[cfg.Key] = cfg
		}
		picked := make([]TopicConfig, 0, len(wanted))
		for _, key := range wanted {
			cfg, ok := byName[key]
			if !ok {
				continue
			}
			picked = append(picked, TopicConfig{
				Key:    cfg.Key,
				Value:  cfg.MaybeValue(),
				Source: cfg.Source.String(),
			})
		}
		out[r.Name] = picked
	}
	return out, nil
}

type CreateTopicSpec struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Configs           map[string]string
}

// CreateTopic creates a topic. Pass partitions=-1 and replicationFactor=-1
// to use broker defaults (Kafka 2.4+).
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

type CloneProgress struct {
	Total  int64
	Copied int64
	Done   bool
	Err    error
}

// CloneOptions tweaks how a topic clone is performed. ReplicationFactor of 0
// falls back to the source topic's replication factor.
type CloneOptions struct {
	CopyConfigs       bool
	ReplicationFactor int16
}

// CloneTopic copies all messages from src into a freshly-created dst topic
// with the same partition count. The returned channel is closed when cloning
// finishes (success or error).
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

	var configs map[string]string
	if opts.CopyConfigs {
		srcConfigs, cfgErr := c.DescribeAllTopicConfigs(ctx, src)
		if cfgErr != nil {
			return nil, fmt.Errorf("kafka: clone %q→%q: source configs: %w", src, dst, cfgErr)
		}
		configs = make(map[string]string, len(srcConfigs))
		for _, cfg := range srcConfigs {
			// only carry over explicit topic-level overrides; broker defaults
			// and static configs would otherwise be pinned on the destination.
			if cfg.Source == kmsg.ConfigSourceDynamicTopicConfig.String() && cfg.Value != "" {
				configs[cfg.Key] = cfg.Value
			}
		}
	}

	if createErr := c.CreateTopic(ctx, CreateTopicSpec{
		Name:              dst,
		Partitions:        partitions,
		ReplicationFactor: rf,
		Configs:           configs,
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
			// honor each record's Partition field so destination partition layout
			// matches the source. Without this, keyless records would be
			// re-distributed via the default sticky partitioner.
			kgo.RecordPartitioner(kgo.ManualPartitioner()),
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
	// progressed accumulates the highest offset+1 we have observed for each
	// partition across polls. PollFetches returns whatever is currently
	// available, so a partition that finished draining in an earlier poll
	// won't reappear in later fetches — without cumulative tracking the
	// terminator below would never observe completion.
	progressed := make(map[int32]int64, len(wm.Partitions))

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
			if next := r.Offset + 1; next > progressed[r.Partition] {
				progressed[r.Partition] = next
			}
		})
		if len(batch) > 0 {
			if results := worker.ProduceSync(ctx, batch...); results.FirstErr() != nil {
				progress <- CloneProgress{Total: total, Copied: copied.Load(), Err: results.FirstErr(), Done: true}
				return
			}
			copied.Add(int64(len(batch)))
			progress <- CloneProgress{Total: total, Copied: copied.Load()}
		}

		if reachedEnd(progressed, endsByPartition) {
			progress <- CloneProgress{Total: total, Copied: copied.Load(), Done: true}
			return
		}

		if err := ctx.Err(); err != nil {
			progress <- CloneProgress{Total: total, Copied: copied.Load(), Err: err, Done: true}
			return
		}
	}
}

// safeInt32 narrows an int into int32 with bounds check (kadm reports counts
// as len() but the wire protocol is int32-bounded).
func safeInt32(n int) (int32, error) {
	const maxInt32 = int(^uint32(0) >> 1)
	if n < 0 || n > maxInt32 {
		return 0, fmt.Errorf("value %d out of int32 range", n)
	}
	return int32(n), nil
}

func safeInt16(n int) (int16, error) {
	const maxInt16 = int(^uint16(0) >> 1)
	if n < 0 || n > maxInt16 {
		return 0, fmt.Errorf("value %d out of int16 range", n)
	}
	return int16(n), nil
}

// reachedEnd returns true when every partition's cumulative next-offset has
// reached (or passed) its recorded high watermark. progressed must be
// cumulative across polls — see runClone.
func reachedEnd(progressed, ends map[int32]int64) bool {
	if len(ends) == 0 {
		return true
	}
	for p, end := range ends {
		if end == 0 {
			continue
		}
		offset, seen := progressed[p]
		if !seen || offset < end {
			return false
		}
	}
	return true
}
