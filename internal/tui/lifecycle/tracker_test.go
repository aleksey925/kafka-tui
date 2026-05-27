package lifecycle_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aleksey925/kafka-tui/internal/tui/lifecycle"
)

func TestTracker_BumpIncrementsAndReturnsNewGen(t *testing.T) {
	tr := lifecycle.New()

	gen1 := tr.Bump()
	gen2 := tr.Bump()

	assert.Equal(t, uint64(1), gen1)
	assert.Equal(t, uint64(2), gen2)
	assert.True(t, tr.Validate(gen2), "current gen validates")
	assert.False(t, tr.Validate(gen1), "stale gen rejected")
}

func TestTracker_DispatchPairsContextWithBump(t *testing.T) {
	tr := lifecycle.New()

	ctx, gen := tr.Dispatch()

	assert.Same(t, tr.Context(), ctx, "Dispatch returns the screen-scoped ctx")
	assert.Equal(t, uint64(1), gen, "Dispatch bumps the generation")
}

func TestTracker_CloseCancelsContext(t *testing.T) {
	tr := lifecycle.New()
	ctx := tr.Context()

	tr.Close()

	assert.Error(t, ctx.Err(), "context must be canceled after Close")
}

func TestTracker_CloseIsIdempotent(t *testing.T) {
	tr := lifecycle.New()
	tr.Close()
	assert.NotPanics(t, tr.Close, "second Close must not panic")
}

func TestTracker_GenReadsWithoutBumping(t *testing.T) {
	tr := lifecycle.New()
	_ = tr.Bump()

	g1 := tr.Gen()
	g2 := tr.Gen()

	assert.Equal(t, uint64(1), g1)
	assert.Equal(t, uint64(1), g2, "Gen must not bump")
}

func TestTracker_ContextStableAcrossBumps(t *testing.T) {
	tr := lifecycle.New()
	ctx1 := tr.Context()
	_ = tr.Bump()
	ctx2 := tr.Context()

	assert.Same(t, ctx1, ctx2, "ctx is stable across bumps")
}
