package tui

import (
	"context"
	"log/slog"

	"github.com/aleksey925/kafka-tui/internal/state"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/produce"
)

// stateHistory adapts the ctx-bearing [state.Store] to the synchronous
// [produce.History] interface. Errors are logged instead of bubbling up;
// the produce form treats missing entries as "no history". `histSize` is
// the cap passed to [state.Store.AddProduce] on every insert so the
// on-disk produce_history table stays trimmed even though the in-memory
// produce form keeps a smaller working set.
//
// histSize == 0 is forwarded to [state.Store.AddProduce] verbatim, which
// means "do not trim" (the on-disk table will grow indefinitely). The
// production wiring in cmd/kafka-tui/main.go forces a non-zero default so
// this only happens in tests that explicitly opt into unbounded growth.
type stateHistory struct {
	store    *state.Store
	histSize int
	log      *slog.Logger
}

func NewStateHistory(store *state.Store, histSize int, log *slog.Logger) produce.History {
	if log == nil {
		log = slog.Default()
	}
	return &stateHistory{store: store, histSize: histSize, log: log}
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
	if err := s.store.AddProduce(context.Background(), fromProduceEntry(entry), s.histSize); err != nil {
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
