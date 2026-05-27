// Package lifecycle provides screen-scoped async lifecycle protection
// per the "Async lifecycle and stale results" contract in CLAUDE.md.
//
// A [Tracker] pairs a generation counter (filters stale results at the
// handler) with a screen-scoped context (kills background goroutines
// when the screen closes). Hold it as a named field on a screen model
// — `track *Tracker` — so the lifecycle methods stay private to the
// screen rather than leaking onto its public surface, then pick the
// narrowest method per call site:
//
//   - [Tracker.Bump] — generation-only; for paths that stamp a gen on
//     their result or just need to invalidate prior in-flight stamps
//   - [Tracker.Context] — context-only; for paths whose cancellation
//     is purely ctx-driven
//   - [Tracker.Dispatch] — sugar for the (ctx, gen) pair when both are
//     needed (e.g. a Follow session that wires ctx and stamps gen)
//   - [Tracker.Validate] in the result handler to drop stale results
//   - [Tracker.Close] from the screen's Close method to terminate
//     background goroutines holding the context
//
// Tracker is single-threaded — safe under Bubble Tea's serialized
// Update loop, not safe for concurrent calls.
package lifecycle

import "context"

type Tracker struct {
	gen    uint64
	ctx    context.Context //nolint:containedctx // tied to screen lifecycle
	cancel context.CancelFunc
}

// New returns a Tracker with a fresh background-derived context.
func New() *Tracker {
	ctx, cancel := context.WithCancel(context.Background())
	return &Tracker{ctx: ctx, cancel: cancel}
}

// Bump increments the generation and returns the new value. Use it
// when a cmd only needs a gen to stamp onto its result, or when a
// caller (e.g. stopFollow) bumps purely to invalidate stamps that are
// still in flight.
func (t *Tracker) Bump() uint64 {
	t.gen++
	return t.gen
}

// Dispatch is sugar for callers that need both the screen-scoped ctx
// and a fresh gen on the same async cmd. The ctx is invariant across
// dispatches and survives until [Tracker.Close].
func (t *Tracker) Dispatch() (context.Context, uint64) {
	return t.ctx, t.Bump()
}

// Validate reports whether gen matches the current generation. Stale
// results (mismatched) should be dropped by the handler before they
// touch model state.
func (t *Tracker) Validate(gen uint64) bool {
	return gen == t.gen
}

// Gen returns the current generation without bumping it. Useful when
// a continuation command needs to inherit its parent's generation
// instead of starting a new one.
func (t *Tracker) Gen() uint64 {
	return t.gen
}

// Context returns the screen-scoped context for callers that hold
// long-lived background work (e.g. follow sessions) and need to
// listen for cancellation directly.
func (t *Tracker) Context() context.Context {
	return t.ctx
}

// Close cancels the context, terminating goroutines that listen on
// it. Idempotent — safe to call multiple times.
func (t *Tracker) Close() {
	if t.cancel != nil {
		t.cancel()
		t.cancel = nil
	}
}
