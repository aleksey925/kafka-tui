package tui

import (
	"context"
	"log/slog"

	"github.com/aleksey925/kafka-tui/internal/state"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/produce"
)

// stateHistory adapts the ctx-bearing [state.Store] to the synchronous
// [produce.History] interface the produce screen consumes. Errors are
// surfaced via slog instead of bubbling up; the produce form already treats
// missing entries as "no history" so a logged read failure degrades to that.
type stateHistory struct {
	store *state.Store
	log   *slog.Logger
}

// NewStateHistory wraps store as a [produce.History] sink.
func NewStateHistory(store *state.Store, log *slog.Logger) produce.History {
	if log == nil {
		log = slog.Default()
	}
	return &stateHistory{store: store, log: log}
}

func (s *stateHistory) LastForTopic(topic string) (produce.Entry, bool) {
	entry, ok, err := s.store.LastProduceForTopic(context.Background(), topic)
	if err != nil {
		s.log.Warn("history: last produce for topic failed", "topic", topic, "err", err)
		return produce.Entry{}, false
	}
	if !ok {
		return produce.Entry{}, false
	}
	return toProduceEntry(entry), true
}

func (s *stateHistory) Recent(n int) []produce.Entry {
	rows, err := s.store.RecentProduce(context.Background(), n)
	if err != nil {
		s.log.Warn("history: recent produces failed", "n", n, "err", err)
		return nil
	}
	out := make([]produce.Entry, 0, len(rows))
	for _, r := range rows {
		out = append(out, toProduceEntry(r))
	}
	return out
}

func (s *stateHistory) Add(entry produce.Entry) {
	if err := s.store.AddProduce(context.Background(), fromProduceEntry(entry), 0); err != nil {
		s.log.Warn("history: add produce failed", "topic", entry.Topic, "err", err)
	}
}

func toProduceEntry(e state.ProduceEntry) produce.Entry {
	return produce.Entry{
		Cluster:     e.Cluster,
		Topic:       e.Topic,
		Key:         e.Key,
		Value:       e.Value,
		Headers:     e.Headers,
		Partition:   e.Partition,
		Compression: e.Compression,
		Timestamp:   e.Timestamp,
	}
}

func fromProduceEntry(e produce.Entry) state.ProduceEntry {
	return state.ProduceEntry{
		Cluster:     e.Cluster,
		Topic:       e.Topic,
		Key:         e.Key,
		Value:       e.Value,
		Headers:     e.Headers,
		Partition:   e.Partition,
		Compression: e.Compression,
		Timestamp:   e.Timestamp,
	}
}
