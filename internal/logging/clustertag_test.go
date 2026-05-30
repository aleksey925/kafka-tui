package logging_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/logging"
)

func TestHandler_Handle__clusterSet__appendsAttribute(t *testing.T) {
	logger, buf := newTestLogger(t)

	logging.SetCluster("prod-eu")
	t.Cleanup(func() { logging.SetCluster("") })
	logger.Info("hello")

	out := buf.String()
	assert.Contains(t, out, "msg=hello")
	assert.Contains(t, out, "cluster=prod-eu")
}

func TestHandler_Handle__clusterUnset__omitsAttribute(t *testing.T) {
	logger, buf := newTestLogger(t)

	logging.SetCluster("")
	logger.Info("startup")

	assert.Contains(t, buf.String(), "msg=startup")
	assert.NotContains(t, buf.String(), "cluster=")
}

func TestHandler_Handle__withInnerGroupsAndAttrs__preservesAll(t *testing.T) {
	logger, buf := newTestLogger(t)
	logger = logger.With("component", "topics").WithGroup("op")

	logging.SetCluster("staging")
	t.Cleanup(func() { logging.SetCluster("") })
	logger.Info("loaded", "topics", 3)

	out := buf.String()
	assert.Contains(t, out, "component=topics")
	assert.Contains(t, out, "op.topics=3")
	assert.Contains(t, out, "cluster=staging")
}

func TestSetCluster__concurrent__noRace(t *testing.T) {
	logger, _ := newTestLogger(t)
	t.Cleanup(func() { logging.SetCluster("") })

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			logging.SetCluster("a")
			logger.Info("write")
		}()
		go func() {
			defer wg.Done()
			logging.SetCluster("b")
			logger.Info("write")
		}()
	}
	wg.Wait()
}

func TestHandler_Enabled__delegatesToInner(t *testing.T) {
	inner := slog.NewTextHandler(&strings.Builder{}, &slog.HandlerOptions{Level: slog.LevelWarn})
	h := logging.NewHandler(inner)
	assert.False(t, h.Enabled(context.Background(), slog.LevelInfo))
	assert.True(t, h.Enabled(context.Background(), slog.LevelError))
}

func newTestLogger(t *testing.T) (*slog.Logger, *bytes.Buffer) {
	t.Helper()
	buf := &bytes.Buffer{}
	inner := slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(logging.NewHandler(inner)), buf
}
