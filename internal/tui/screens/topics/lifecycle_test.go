package topics_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aleksey925/kafka-tui/internal/kafka"
	"github.com/aleksey925/kafka-tui/internal/tui/screens/topics"
)

// blockingSvc wraps fakeService and blocks ListTopics until the test releases
// it. Lets us pin a loadCmd in flight while we close the screen.
type blockingSvc struct {
	*fakeService
	release chan struct{}
}

func (b *blockingSvc) ListTopics(ctx context.Context) ([]kafka.TopicSummary, error) {
	select {
	case <-b.release:
	case <-ctx.Done():
		return nil, fmt.Errorf("blockingSvc.ListTopics: %w", ctx.Err())
	}
	return b.fakeService.ListTopics(ctx)
}

// TestRefresh_CloseDropsInflightLoad is a regression test for stale loadCmd
// results overwriting a freshly-mounted topics screen. The host swaps screens
// on every push/pop, so a slow refresh from a popped screen otherwise reaches
// the new instance as a TopicsLoadedMsg and clobbers its data.
func TestRefresh_CloseDropsInflightLoad(t *testing.T) {
	svc := &blockingSvc{
		fakeService: newFakeService([]kafka.TopicSummary{{Name: "alpha"}}, nil),
		release:     make(chan struct{}),
	}
	m := topics.New(topics.Options{Service: svc})

	// kick a refresh off — actRefresh is invoked indirectly through the
	// `r` key. The returned cmd is loadCmd which now blocks on svc.
	cmd := m.Update(keyPress("r"))
	require.NotNil(t, cmd, "pressing 'r' must yield a load command")

	msgCh := make(chan tea.Msg, 1)
	go func() { msgCh <- cmd() }()

	m.Close()
	close(svc.release)

	select {
	case msg := <-msgCh:
		assert.Nil(t, msg,
			"a load that was in flight when Close() was called must not "+
				"dispatch a TopicsLoadedMsg — bubbletea would route it to "+
				"the next active screen and clobber its fresh data")
	case <-time.After(2 * time.Second):
		t.Fatal("loadCmd never returned after Close + release")
	}
}
