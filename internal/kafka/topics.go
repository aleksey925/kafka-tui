package kafka

import (
	"context"
	"fmt"
	"slices"
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

// TopicsPartitions resolves the full set of partitions for each requested
// topic via a single ListStartOffsets call (cluster metadata reused from
// franz-go's offset path). Used by the consumer-group detail screen to
// scope topic-level resets to every partition of the topic — not just the
// ones the group already has commits for.
func (c *Client) TopicsPartitions(ctx context.Context, topics ...string) (map[string][]int32, error) {
	if len(topics) == 0 {
		return map[string][]int32{}, nil
	}
	listed, err := c.adm.ListStartOffsets(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("kafka: list start offsets: %w", err)
	}
	out := make(map[string][]int32, len(topics))
	for topic, ps := range listed {
		partitions := make([]int32, 0, len(ps))
		for p, lo := range ps {
			if lo.Err != nil {
				continue
			}
			partitions = append(partitions, p)
		}
		slices.Sort(partitions)
		out[topic] = partitions
	}
	return out, nil
}

func (c *Client) TopicPartitions(ctx context.Context, topic string) ([]PartitionDetail, error) {
	md, err := c.freshMetadata(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("kafka: metadata: %w", err)
	}
	td, ok := md.Topics[topic]
	if !ok {
		return nil, ErrTopicNotFound
	}
	if td.Err != nil {
		return nil, fmt.Errorf("kafka: topic metadata: %w", td.Err)
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
		return nil, fmt.Errorf("kafka: describe configs: %w", err)
	}
	r, err := rs.On(topic, nil)
	if err != nil {
		return nil, fmt.Errorf("kafka: describe configs: %w", err)
	}
	if r.Err != nil {
		return nil, fmt.Errorf("kafka: describe configs: %w", r.Err)
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

// AlterTopicConfig sets a single topic-level configuration entry to value
// using IncrementalAlterConfigs (Kafka 2.3+). An empty value is allowed
// only when the broker accepts it; callers wanting to reset a key to its
// default should use the dedicated reset path instead (not yet exposed).
func (c *Client) AlterTopicConfig(ctx context.Context, topic, key, value string) error {
	if err := c.ensureWritable(); err != nil {
		return err
	}
	v := value
	rs, err := c.adm.AlterTopicConfigs(ctx, []kadm.AlterConfig{
		{Op: kadm.SetConfig, Name: key, Value: &v},
	}, topic)
	if err != nil {
		return fmt.Errorf("kafka: alter config: %w", err)
	}
	r, err := rs.On(topic, nil)
	if err != nil {
		return fmt.Errorf("kafka: alter config: %w", err)
	}
	if r.Err != nil {
		if r.ErrMessage != "" {
			return fmt.Errorf("kafka: alter config: %s: %w", r.ErrMessage, r.Err)
		}
		return fmt.Errorf("kafka: alter config: %w", r.Err)
	}
	return nil
}

// DescribeAllTopicConfigs returns the full config set for a topic.
func (c *Client) DescribeAllTopicConfigs(ctx context.Context, topic string) ([]TopicConfig, error) {
	rs, err := c.adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("kafka: describe configs: %w", err)
	}
	r, err := rs.On(topic, nil)
	if err != nil {
		return nil, fmt.Errorf("kafka: describe configs: %w", err)
	}
	if r.Err != nil {
		return nil, fmt.Errorf("kafka: describe configs: %w", r.Err)
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
		return 0, fmt.Errorf("kafka: metadata: %w", err)
	}
	td, ok := md.Topics[topic]
	if !ok {
		return 0, ErrTopicNotFound
	}
	if td.Err != nil {
		return 0, fmt.Errorf("kafka: topic metadata: %w", td.Err)
	}
	set := kadm.TopicsSet{}
	for p := range td.Partitions {
		set.Add(topic, p)
	}
	dirs, err := c.adm.DescribeAllLogDirs(ctx, set)
	if err != nil {
		return 0, fmt.Errorf("kafka: describe log dirs: %w", err)
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
		return TopicWatermarks{}, fmt.Errorf("kafka: watermarks: %w", err)
	}
	r, ok := all[topic]
	if !ok {
		return TopicWatermarks{Partitions: map[int32]PartitionWatermarks{}}, nil
	}
	if r.Err != nil {
		return TopicWatermarks{}, fmt.Errorf("kafka: watermarks: %w", r.Err)
	}
	return r.Value, nil
}

// TopicWatermarksBatch fetches per-partition low/high watermarks for
// many topics in two RPCs (ListStartOffsets, ListEndOffsets). A topic
// with at least one successful partition returns the partial Value
// with Err == nil; otherwise the per-topic Err carries the failure.
// Empty topics returns an empty map without contacting the broker.
func (c *Client) TopicWatermarksBatch(ctx context.Context, topics ...string) (map[string]BatchResult[TopicWatermarks], error) {
	out := make(map[string]BatchResult[TopicWatermarks], len(topics))
	if len(topics) == 0 {
		return out, nil
	}
	starts, sErr := c.adm.ListStartOffsets(ctx, topics...)
	deniedStart, isAuthStart := UnwrapKadmAuthErr(sErr)
	if sErr != nil && !isAuthStart {
		return nil, fmt.Errorf("kafka: list start offsets: %w", sErr)
	}
	ends, eErr := c.adm.ListEndOffsets(ctx, topics...)
	deniedEnd, isAuthEnd := UnwrapKadmAuthErr(eErr)
	if eErr != nil && !isAuthEnd {
		return nil, fmt.Errorf("kafka: list end offsets: %w", eErr)
	}
	denied := deniedEnd
	if denied == nil {
		denied = deniedStart
	}
	for _, t := range topics {
		startMap := starts[t]
		endMap := ends[t]
		w := TopicWatermarks{Partitions: make(map[int32]PartitionWatermarks)}
		var firstPartErr error
		for p, e := range endMap {
			if e.Err != nil {
				if firstPartErr == nil {
					firstPartErr = e.Err
				}
				continue
			}
			s, ok := startMap[p]
			if !ok || s.Err != nil {
				if firstPartErr == nil && ok {
					firstPartErr = s.Err
				}
				continue
			}
			pw := PartitionWatermarks{Low: s.Offset, High: e.Offset}
			w.Partitions[p] = pw
			if pw.High > pw.Low {
				w.MessageCount += pw.High - pw.Low
			}
		}
		if len(w.Partitions) == 0 {
			if firstPartErr != nil {
				out[t] = BatchResult[TopicWatermarks]{Err: firstPartErr}
				continue
			}
			if denied != nil {
				out[t] = BatchResult[TopicWatermarks]{Err: denied}
				continue
			}
		}
		out[t] = BatchResult[TopicWatermarks]{Value: w}
	}
	return out, nil
}

// TopicSizesBatch returns total on-disk size (bytes) per topic via one
// Metadata RPC plus one DescribeAllLogDirs. Metadata-level and
// log-dir-level failures both flow through per-topic [BatchResult.Err]
// so the caller can render a denial marker; topics neither measured
// nor errored are absent from the map.
func (c *Client) TopicSizesBatch(ctx context.Context, topics ...string) (map[string]BatchResult[int64], error) {
	out := make(map[string]BatchResult[int64], len(topics))
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
		if !ok {
			out[t] = BatchResult[int64]{Err: ErrTopicNotFound}
			continue
		}
		if td.Err != nil {
			out[t] = BatchResult[int64]{Err: td.Err}
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
	denied, isAuth := UnwrapKadmAuthErr(err)
	if err != nil && !isAuth {
		return nil, fmt.Errorf("kafka: describe log dirs: %w", err)
	}
	sizes, firstDirErr := aggregateLogDirs(dirs, set)
	for t := range set {
		if _, ok := sizes[t]; ok {
			out[t] = BatchResult[int64]{Value: sizes[t]}
			continue
		}
		if err, ok := firstDirErr[t]; ok {
			out[t] = BatchResult[int64]{Err: err}
			continue
		}
		if denied != nil {
			out[t] = BatchResult[int64]{Err: denied}
			continue
		}
		// no contribution and no error — leave absent so the UI renders
		// "—" ("unknown"), not "0 B" ("measured zero").
	}
	return out, nil
}

// aggregateLogDirs sums broker contributions per topic and records the
// first broker-level error each topic may have been affected by. The
// broker error attributes to every requested topic — the response
// shape can't tell us which topics that broker hosted — so the caller
// must prefer real data over the recorded error when both exist.
func aggregateLogDirs(dirs kadm.DescribedAllLogDirs, set kadm.TopicsSet) (sizes map[string]int64, firstDirErr map[string]error) {
	sizes = make(map[string]int64, len(set))
	firstDirErr = make(map[string]error, len(set))
	for _, perBroker := range dirs {
		for _, dir := range perBroker {
			if dir.Err != nil {
				for topic := range set {
					if _, seen := firstDirErr[topic]; !seen {
						firstDirErr[topic] = dir.Err
					}
				}
				continue
			}
			for topic, parts := range dir.Topics {
				for _, p := range parts {
					if p.Size > 0 {
						sizes[topic] += p.Size
					}
				}
			}
		}
	}
	return sizes, firstDirErr
}

// DescribeTopicConfigsBatch fetches the UI-relevant configs for many
// topics in a single RPC. Per-topic failures (typically ACL) flow
// through [BatchResult.Err]; successes carry the picked config slice
// in Value.
func (c *Client) DescribeTopicConfigsBatch(ctx context.Context, topics ...string) (map[string]BatchResult[[]TopicConfig], error) {
	out := make(map[string]BatchResult[[]TopicConfig], len(topics))
	if len(topics) == 0 {
		return out, nil
	}
	rs, err := c.adm.DescribeTopicConfigs(ctx, topics...)
	denied, isAuth := UnwrapKadmAuthErr(err)
	if err != nil && !isAuth {
		return nil, fmt.Errorf("kafka: describe configs: %w", err)
	}
	wanted := []string{ConfigCleanupPolicy, ConfigRetentionMs, ConfigMinInSyncReplica}
	for _, r := range rs {
		if r.Err != nil {
			out[r.Name] = BatchResult[[]TopicConfig]{Err: r.Err}
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
		out[r.Name] = BatchResult[[]TopicConfig]{Value: picked}
	}
	if denied != nil {
		for _, t := range topics {
			if _, ok := out[t]; !ok {
				out[t] = BatchResult[[]TopicConfig]{Err: denied}
			}
		}
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
	if err := c.ensureWritable(); err != nil {
		return err
	}
	configs := make(map[string]*string, len(spec.Configs))
	for k, v := range spec.Configs {
		configs[k] = &v
	}
	resp, err := c.adm.CreateTopic(ctx, spec.Partitions, spec.ReplicationFactor, configs, spec.Name)
	if err != nil {
		return fmt.Errorf("kafka: create topic: %w", err)
	}
	if resp.Err != nil {
		return fmt.Errorf("kafka: create topic: %w", resp.Err)
	}
	return nil
}

func (c *Client) DeleteTopic(ctx context.Context, topic string) error {
	if err := c.ensureWritable(); err != nil {
		return err
	}
	resp, err := c.adm.DeleteTopic(ctx, topic)
	if err != nil {
		return fmt.Errorf("kafka: delete topic: %w", err)
	}
	if resp.Err != nil {
		return fmt.Errorf("kafka: delete topic: %w", resp.Err)
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
	if err := c.ensureWritable(); err != nil {
		return nil, err
	}
	md, err := c.freshMetadata(ctx, src)
	if err != nil {
		return nil, fmt.Errorf("kafka: clone: source metadata: %w", err)
	}
	srcDetail, ok := md.Topics[src]
	if !ok || srcDetail.Err != nil {
		return nil, ErrTopicNotFound
	}
	partitions, err := safeInt32(len(srcDetail.Partitions))
	if err != nil {
		return nil, fmt.Errorf("kafka: clone: %w", err)
	}
	rf := opts.ReplicationFactor
	if rf == 0 {
		rep, repErr := safeInt16(srcDetail.Partitions.NumReplicas())
		if repErr != nil {
			return nil, fmt.Errorf("kafka: clone: %w", repErr)
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
			return nil, fmt.Errorf("kafka: clone: source configs: %w", cfgErr)
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
		return nil, fmt.Errorf("kafka: clone: watermarks: %w", err)
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

	// send respects ctx so an abandoned receiver (UI closed, parent ctx
	// not yet canceled) can't deadlock us mid-clone. returns false when
	// the context is gone — caller should bail out instead of retrying.
	send := func(p CloneProgress) bool {
		select {
		case progress <- p:
			return true
		case <-ctx.Done():
			return false
		}
	}

	total := wm.MessageCount
	if total == 0 {
		send(CloneProgress{Total: 0, Copied: 0, Done: true})
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
		send(CloneProgress{Total: total, Err: err, Done: true})
		return
	}
	worker, err := kgo.NewClient(opts...)
	if err != nil {
		send(CloneProgress{Total: total, Err: fmt.Errorf("kafka: clone worker: %w", err), Done: true})
		return
	}
	defer worker.Close()

	// only track partitions that actually have records to copy. an empty
	// partition (e.g. high=low=N after retention) never produces a record,
	// so leaving it here would block reachedEnd forever.
	endsByPartition := make(map[int32]int64, len(wm.Partitions))
	for p, w := range wm.Partitions {
		if w.High <= w.Low {
			continue
		}
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
			send(CloneProgress{Total: total, Copied: copied.Load(), Err: errs[0].Err, Done: true})
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
				// preserve event-time so timestamp lookups and retention
				// on the clone behave the same as on the source.
				Timestamp: r.Timestamp,
			}
			batch = append(batch, rec)
			if next := r.Offset + 1; next > progressed[r.Partition] {
				progressed[r.Partition] = next
			}
		})
		if len(batch) > 0 {
			if results := worker.ProduceSync(ctx, batch...); results.FirstErr() != nil {
				send(CloneProgress{Total: total, Copied: copied.Load(), Err: results.FirstErr(), Done: true})
				return
			}
			copied.Add(int64(len(batch)))
			if !send(CloneProgress{Total: total, Copied: copied.Load()}) {
				return
			}
		}

		if reachedEnd(progressed, endsByPartition) {
			send(CloneProgress{Total: total, Copied: copied.Load(), Done: true})
			return
		}

		if err := ctx.Err(); err != nil {
			send(CloneProgress{Total: total, Copied: copied.Load(), Err: err, Done: true})
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
