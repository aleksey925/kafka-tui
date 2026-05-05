// Package clipboard exports copied text via native CLI tools (pbcopy /
// xclip / wl-copy) and/or OSC 52 escape sequences. Method selects the
// transport: "native", "osc52", "auto" (run both, succeed if either does),
// or "off".
package clipboard

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

type Method string

const (
	MethodAuto   Method = "auto"
	MethodNative Method = "native"
	MethodOSC52  Method = "osc52"
	MethodOff    Method = "off"
)

// ParseMethod normalises a user-facing string into a [Method]. Empty input
// returns [MethodAuto].
func ParseMethod(s string) (Method, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "auto":
		return MethodAuto, nil
	case "native":
		return MethodNative, nil
	case "osc52":
		return MethodOSC52, nil
	case "off", "none", "disabled":
		return MethodOff, nil
	default:
		return "", fmt.Errorf("clipboard: unknown method %q (allowed: auto, native, osc52, off)", s)
	}
}

// Clipboard implementations must be safe for concurrent use — copy hotkeys
// may overlap with auto-refresh goroutines.
type Clipboard interface {
	Copy(ctx context.Context, payload string) error
}

type Options struct {
	Method Method
	Native Clipboard
	OSC52  Clipboard
}

// New constructs a [Clipboard] for the requested method.
func New(opts Options) Clipboard {
	method := opts.Method
	if method == "" {
		method = MethodAuto
	}
	if method == MethodOff {
		return noopClipboard{}
	}

	native := opts.Native
	if native == nil {
		native = DefaultNative()
	}
	osc52 := opts.OSC52
	if osc52 == nil {
		osc52 = DefaultOSC52()
	}

	switch method {
	case MethodNative:
		return native
	case MethodOSC52:
		return osc52
	default:
		return parallelClipboard{native: native, osc52: osc52}
	}
}

type noopClipboard struct{}

func (noopClipboard) Copy(_ context.Context, _ string) error { return nil }

// parallelClipboard fans Copy out to both transports concurrently. Success
// requires at least one transport to succeed; if both fail, their errors
// are joined.
type parallelClipboard struct {
	native Clipboard
	osc52  Clipboard
}

func (p parallelClipboard) Copy(ctx context.Context, payload string) error {
	var (
		wg                  sync.WaitGroup
		nativeErr, osc52Err error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		nativeErr = p.native.Copy(ctx, payload)
	}()
	go func() {
		defer wg.Done()
		osc52Err = p.osc52.Copy(ctx, payload)
	}()
	wg.Wait()

	if nativeErr == nil || osc52Err == nil {
		return nil
	}
	return errors.Join(
		fmt.Errorf("native: %w", nativeErr),
		fmt.Errorf("osc52: %w", osc52Err),
	)
}
