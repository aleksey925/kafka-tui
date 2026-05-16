// Catalog of topic-level config metadata. Initially seeded from a snapshot
// of Redpanda Console's /api/topics/<name>/configuration endpoint and now
// hand-maintained — add or update entries here when Kafka introduces new
// keys we want to surface in the UI.

package configcatalog

var entries = map[string]Entry{
	"max.compaction.lag.ms": {
		Key:      "max.compaction.lag.ms",
		Category: "Compaction",
		Type:     TypeDuration,
		Doc:      "The maximum time a message will remain ineligible for compaction in the log. Only applicable for logs that are being compacted.",
	},
	"min.cleanable.dirty.ratio": {
		Key:      "min.cleanable.dirty.ratio",
		Category: "Compaction",
		Type:     TypeRatio,
		Doc:      "This configuration controls how frequently the log compactor will attempt to clean the log (assuming log compaction is enabled). By default we will avoid cleaning a log where more than 50% of the log has been compacted. This ratio bounds the maximum space wasted in the log by duplicates (at 50% at most 50% of the log could be duplicates). A higher ratio will mean fewer, more efficient cleanings but will mean more wasted space in the log. If the max.compaction.lag.ms or the min.compaction.lag.ms configurations are also specified, then the log compactor considers the log to be eligible for compaction as soon as either: (i) the dirty ratio threshold has been met and the log has had dirty (uncompacted) records for at least the min.compaction.lag.ms duration, or (ii) if the log has had dirty (uncompacted) records for at most the max.compaction.lag.ms period.",
	},
	"min.compaction.lag.ms": {
		Key:      "min.compaction.lag.ms",
		Category: "Compaction",
		Type:     TypeDuration,
		Doc:      "The minimum time a message will remain uncompacted in the log. Only applicable for logs that are being compacted.",
	},
	"compression.gzip.level": {
		Key:      "compression.gzip.level",
		Category: "Compression",
		Type:     TypeInteger,
		Doc:      "The compression level to use if compression.type is set to `gzip`.",
	},
	"compression.lz4.level": {
		Key:      "compression.lz4.level",
		Category: "Compression",
		Type:     TypeInteger,
		Doc:      "The compression level to use if compression.type is set to `lz4`.",
	},
	"compression.type": {
		Key:        "compression.type",
		Category:   "Compression",
		Type:       TypeSelect,
		Doc:        "Specify the final compression type for a given topic. This configuration accepts the standard compression codecs ('gzip', 'snappy', 'lz4', 'zstd'). It additionally accepts 'uncompressed' which is equivalent to no compression; and 'producer' which means retain the original compression codec set by the producer.",
		EnumValues: []string{"uncompressed", "producer", "gzip", "lz4", "snappy", "zstd"},
	},
	"compression.zstd.level": {
		Key:      "compression.zstd.level",
		Category: "Compression",
		Type:     TypeInteger,
		Doc:      "The compression level to use if compression.type is set to `zstd`.",
	},
	"max.message.bytes": {
		Key:      "max.message.bytes",
		Category: "Message Handling",
		Type:     TypeByteSize,
		Doc:      "The largest record batch size allowed by Kafka (after compression if compression is enabled). If this is increased and there are consumers older than 0.10.2, the consumers' fetch size must also be increased so that they can fetch record batches this large. In the latest message format version, records are always grouped into batches for efficiency. In previous message format versions, uncompressed records are not grouped into batches and this limit only applies to a single record in that case.",
	},
	"message.downconversion.enable": {
		Key:      "message.downconversion.enable",
		Category: "Message Handling",
		Type:     TypeBoolean,
		Doc:      "This configuration controls whether down-conversion of message formats is enabled to satisfy consume requests. When set to `false`, broker will not perform down-conversion for consumers expecting an older message format. The broker responds with `UNSUPPORTED_VERSION` error for consume requests from such older clients. This configurationdoes not apply to any message format conversion that might be required for replication to followers.",
	},
	"message.format.version": {
		Key:      "message.format.version",
		Category: "Message Handling",
		Type:     TypeString,
		Doc:      "[DEPRECATED] Specify the message format version the broker will use to append messages to the logs. The value of this config is always assumed to be `3.0` if `inter.broker.protocol.version` is 3.0 or higher (the actual config value is ignored). Otherwise, the value should be a valid ApiVersion. Some examples are: 0.10.0, 1.1, 2.8, 3.0. By setting a particular message format version, the user is certifying that all the existing messages on disk are smaller or equal than the specified version. Setting this value incorrectly will cause consumers with older versions to break as they will receive messages with a format that they don't understand.",
	},
	"message.timestamp.after.max.ms": {
		Key:      "message.timestamp.after.max.ms",
		Category: "Message Handling",
		Type:     TypeInteger,
		Doc:      "This configuration sets the allowable timestamp difference between the message timestamp and the broker's timestamp. The message timestamp can be later than or equal to the broker's timestamp, with the maximum allowable difference determined by the value set in this configuration. If message.timestamp.type=CreateTime, the message will be rejected if the difference in timestamps exceeds this specified threshold. This configuration is ignored if message.timestamp.type=LogAppendTime.",
	},
	"message.timestamp.before.max.ms": {
		Key:      "message.timestamp.before.max.ms",
		Category: "Message Handling",
		Type:     TypeInteger,
		Doc:      "This configuration sets the allowable timestamp difference between the broker's timestamp and the message timestamp. The message timestamp can be earlier than or equal to the broker's timestamp, with the maximum allowable difference determined by the value set in this configuration. If message.timestamp.type=CreateTime, the message will be rejected if the difference in timestamps exceeds this specified threshold. This configuration is ignored if message.timestamp.type=LogAppendTime.",
	},
	"message.timestamp.difference.max.ms": {
		Key:      "message.timestamp.difference.max.ms",
		Category: "Message Handling",
		Type:     TypeDuration,
		Doc:      "[DEPRECATED] The maximum difference allowed between the timestamp when a broker receives a message and the timestamp specified in the message. If message.timestamp.type=CreateTime, a message will be rejected if the difference in timestamp exceeds this threshold. This configuration is ignored if message.timestamp.type=LogAppendTime.",
	},
	"message.timestamp.type": {
		Key:        "message.timestamp.type",
		Category:   "Message Handling",
		Type:       TypeSelect,
		Doc:        "Define whether the timestamp in the message is message create time or log append time. The value should be either `CreateTime` or `LogAppendTime`",
		EnumValues: []string{"CreateTime", "LogAppendTime"},
	},
	"follower.replication.throttled.replicas": {
		Key:      "follower.replication.throttled.replicas",
		Category: "Replication",
		Type:     TypeString,
		Doc:      "A list of replicas for which log replication should be throttled on the follower side. The list should describe a set of replicas in the form [PartitionId]:[BrokerId],[PartitionId]:[BrokerId]:... or alternatively the wildcard '*' can be used to throttle all replicas for this topic.",
	},
	"leader.replication.throttled.replicas": {
		Key:      "leader.replication.throttled.replicas",
		Category: "Replication",
		Type:     TypeString,
		Doc:      "A list of replicas for which log replication should be throttled on the leader side. The list should describe a set of replicas in the form [PartitionId]:[BrokerId],[PartitionId]:[BrokerId]:... or alternatively the wildcard '*' can be used to throttle all replicas for this topic.",
	},
	"min.insync.replicas": {
		Key:      "min.insync.replicas",
		Category: "Replication",
		Type:     TypeInteger,
		Doc:      "When a producer sets acks to \"all\" (or \"-1\"), this configuration specifies the minimum number of replicas that must acknowledge a write for the write to be considered successful. If this minimum cannot be met, then the producer will raise an exception (either NotEnoughReplicas or NotEnoughReplicasAfterAppend).\nWhen used together, `min.insync.replicas` and `acks` allow you to enforce greater durability guarantees. A typical scenario would be to create a topic with a replication factor of 3, set `min.insync.replicas` to 2, and produce with `acks` of \"all\". This will ensure that the producer raises an exception if a majority of replicas do not receive a write.",
	},
	"unclean.leader.election.enable": {
		Key:      "unclean.leader.election.enable",
		Category: "Replication",
		Type:     TypeBoolean,
		Doc:      "Indicates whether to enable replicas not in the ISR set to be elected as leader as a last resort, even though doing so may result in data loss.Note: In KRaft mode, when enabling this config dynamically, it needs to wait for the unclean leader electionthread to trigger election periodically (default is 5 minutes). Please run `kafka-leader-election.sh` with `unclean` option to trigger the unclean leader election immediately if needed.",
	},
	"cleanup.policy": {
		Key:        "cleanup.policy",
		Category:   "Retention",
		Type:       TypeSelect,
		Doc:        "This config designates the retention policy to use on log segments. The \"delete\" policy (which is the default) will discard old segments when their retention time or size limit has been reached. The \"compact\" policy will enable log compaction, which retains the latest value for each key. It is also possible to specify both policies in a comma-separated list (e.g. \"delete,compact\"). In this case, old segments will be discarded per the retention time and size configuration, while retained segments will be compacted.",
		EnumValues: []string{"delete", "compact", "compact,delete"},
	},
	"delete.retention.ms": {
		Key:      "delete.retention.ms",
		Category: "Retention",
		Type:     TypeDuration,
		Doc:      "The amount of time to retain delete tombstone markers for log compacted topics. This setting also gives a bound on the time in which a consumer must complete a read if they begin from offset 0 to ensure that they get a valid snapshot of the final stage (otherwise delete tombstones may be collected before they complete their scan).",
	},
	"file.delete.delay.ms": {
		Key:      "file.delete.delay.ms",
		Category: "Retention",
		Type:     TypeDuration,
		Doc:      "The time to wait before deleting a file from the filesystem",
	},
	"local.retention.bytes": {
		Key:      "local.retention.bytes",
		Category: "Retention",
		Type:     TypeInteger,
		Doc:      "The maximum size of local log segments that can grow for a partition before it deletes the old segments. Default value is -2, it represents `retention.bytes` value to be used. The effective value should always be less than or equal to `retention.bytes` value.",
	},
	"local.retention.ms": {
		Key:      "local.retention.ms",
		Category: "Retention",
		Type:     TypeInteger,
		Doc:      "The number of milliseconds to keep the local log segment before it gets deleted. Default value is -2, it represents `retention.ms` value is to be used. The effective value should always be less than or equal to `retention.ms` value.",
	},
	"retention.bytes": {
		Key:      "retention.bytes",
		Category: "Retention",
		Type:     TypeByteSize,
		Doc:      "This configuration controls the maximum size a partition (which consists of log segments) can grow to before we will discard old log segments to free up space if we are using the \"delete\" retention policy. By default there is no size limit only a time limit. Since this limit is enforced at the partition level, multiply it by the number of partitions to compute the topic retention in bytes. Additionally, retention.bytes configuration operates independently of \"segment.ms\" and \"segment.bytes\" configurations. Moreover, it triggers the rolling of new segment if the retention.bytes is configured to zero.",
	},
	"retention.ms": {
		Key:      "retention.ms",
		Category: "Retention",
		Type:     TypeDuration,
		Doc:      "This configuration controls the maximum time we will retain a log before we will discard old log segments to free up space if we are using the \"delete\" retention policy. This represents an SLA on how soon consumers must read their data. If set to -1, no time limit is applied. Additionally, retention.ms configuration operates independently of \"segment.ms\" and \"segment.bytes\" configurations. Moreover, it triggers the rolling of new segment if the retention.ms condition is satisfied.",
	},
	"index.interval.bytes": {
		Key:      "index.interval.bytes",
		Category: "Storage Internals",
		Type:     TypeByteSize,
		Doc:      "This setting controls how frequently Kafka adds an index entry to its offset index. The default setting ensures that we index a message roughly every 4096 bytes. More indexing allows reads to jump closer to the exact position in the log but makes the index larger. You probably don't need to change this.",
	},
	"preallocate": {
		Key:      "preallocate",
		Category: "Storage Internals",
		Type:     TypeBoolean,
		Doc:      "True if we should preallocate the file on disk when creating a new log segment.",
	},
	"remote.log.copy.disable": {
		Key:      "remote.log.copy.disable",
		Category: "Storage Internals",
		Type:     TypeBoolean,
		Doc:      "Determines whether tiered data for a topic should become read only, and no more data uploading on a topic. Once this config is set to true, the local retention configuration (i.e. local.retention.ms/bytes) becomes irrelevant, and all data expiration follows the topic-wide retention configuration(i.e. retention.ms/bytes).",
	},
	"remote.storage.enable": {
		Key:      "remote.storage.enable",
		Category: "Storage Internals",
		Type:     TypeBoolean,
		Doc:      "To enable tiered storage for a topic, set this configuration as true. You can not disable this config once it is enabled. It will be provided in future versions.",
	},
	"segment.bytes": {
		Key:      "segment.bytes",
		Category: "Storage Internals",
		Type:     TypeByteSize,
		Doc:      "This configuration controls the segment file size for the log. Retention and cleaning is always done a file at a time so a larger segment size means fewer files but less granular control over retention.",
	},
	"segment.index.bytes": {
		Key:      "segment.index.bytes",
		Category: "Storage Internals",
		Type:     TypeByteSize,
		Doc:      "This configuration controls the size of the index that maps offsets to file positions. We preallocate this index file and shrink it only after log rolls. You generally should not need to change this setting.",
	},
	"segment.jitter.ms": {
		Key:      "segment.jitter.ms",
		Category: "Storage Internals",
		Type:     TypeDuration,
		Doc:      "The maximum random jitter subtracted from the scheduled segment roll time to avoid thundering herds of segment rolling",
	},
	"segment.ms": {
		Key:      "segment.ms",
		Category: "Storage Internals",
		Type:     TypeDuration,
		Doc:      "This configuration controls the period of time after which Kafka will force the log to roll even if the segment file isn't full to ensure that retention can delete or compact old data.",
	},
	"flush.messages": {
		Key:      "flush.messages",
		Category: "Write Caching",
		Type:     TypeInteger,
		Doc:      "This setting allows specifying an interval at which we will force an fsync of data written to the log. For example if this was set to 1 we would fsync after every message; if it were 5 we would fsync after every five messages. In general we recommend you not set this and use replication for durability and allow the operating system's background flush capabilities as it is more efficient. This setting can be overridden on a per-topic basis (see the per-topic configuration section).",
	},
	"flush.ms": {
		Key:      "flush.ms",
		Category: "Write Caching",
		Type:     TypeDuration,
		Doc:      "This setting allows specifying a time interval at which we will force an fsync of data written to the log. For example if this was set to 1000 we would fsync after 1000 ms had passed. In general we recommend you not set this and use replication for durability and allow the operating system's background flush capabilities as it is more efficient.",
	},
}
