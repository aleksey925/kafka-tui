package logging

import (
	"context"
	"log/slog"
	"sync/atomic"
)

// currentCluster holds the active cluster name observed by [Handler].
// It's a process-global because the app talks to one cluster at a time
// and every slog call site benefits from the tag without having to
// thread the name through its construction.
var currentCluster atomic.Pointer[string]

// SetCluster sets (or clears, with "") the cluster name attached to
// subsequent log records routed through [Handler].
func SetCluster(name string) {
	if name == "" {
		currentCluster.Store(nil)
		return
	}
	currentCluster.Store(&name)
}

// Handler wraps another [slog.Handler] and adds a "cluster" attribute
// to each record when one is set via [SetCluster]. With no cluster set
// the attribute is omitted entirely so startup / picker records stay
// clean.
type Handler struct {
	inner slog.Handler
}

func NewHandler(inner slog.Handler) *Handler {
	return &Handler{inner: inner}
}

func (h *Handler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *Handler) Handle(ctx context.Context, r slog.Record) error {
	if name := currentCluster.Load(); name != nil {
		r.AddAttrs(slog.String("cluster", *name))
	}
	return h.inner.Handle(ctx, r) //nolint:wrapcheck // pass-through wrapper; slog.Handler contract expects the inner error verbatim
}

func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &Handler{inner: h.inner.WithAttrs(attrs)}
}

func (h *Handler) WithGroup(name string) slog.Handler {
	return &Handler{inner: h.inner.WithGroup(name)}
}
