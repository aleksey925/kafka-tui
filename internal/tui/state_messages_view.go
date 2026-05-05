package tui

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aleksey925/kafka-tui/internal/state"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/messages"
)

// stateMessagesView adapts [state.Store] to [messages.ViewStateRepository].
// Read/write errors are logged and degrade to "no persistence" — failures
// must never block the user from opening the screen.
type stateMessagesView struct {
	store *state.Store
	log   *slog.Logger
}

func NewStateMessagesView(store *state.Store, log *slog.Logger) messages.ViewStateRepository {
	if log == nil {
		log = slog.Default()
	}
	return &stateMessagesView{store: store, log: log}
}

func (s *stateMessagesView) LoadMessagesView(ctx context.Context, cluster, topic string) (messages.ViewState, bool, error) {
	row, ok, err := s.store.LoadMessagesView(ctx, cluster, topic)
	if err != nil {
		s.log.Warn("view state: load failed", "cluster", cluster, "topic", topic, "err", err)
		return messages.ViewState{}, false, nil
	}
	if !ok {
		return messages.ViewState{}, false, nil
	}
	return messages.ViewState{
		SeekMode:   messages.SeekMode(row.SeekMode),
		Partition:  row.Partition,
		Offset:     row.Offset,
		Timestamp:  row.Timestamp,
		HasPart:    row.HasPart,
		Partitions: row.Partitions,
	}, true, nil
}

func (s *stateMessagesView) SaveMessagesView(ctx context.Context, cluster, topic string, view messages.ViewState) error {
	if err := s.store.SaveMessagesView(ctx, cluster, topic, state.MessagesView{
		SeekMode:   int(view.SeekMode),
		Partition:  view.Partition,
		Offset:     view.Offset,
		Timestamp:  view.Timestamp,
		HasPart:    view.HasPart,
		Partitions: view.Partitions,
	}); err != nil {
		s.log.Warn("view state: save failed", "cluster", cluster, "topic", topic, "err", err)
		return fmt.Errorf("view state save: %w", err)
	}
	return nil
}
