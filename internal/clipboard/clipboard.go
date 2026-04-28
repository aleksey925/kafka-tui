// Package clipboard exports copied text to the user's clipboard.
//
// Three transport methods are supported, selected via [Method]:
//
//   - "native"  → invoke the platform's native CLI tool (pbcopy / xclip /
//     wl-copy), populating the local desktop clipboard.
//   - "osc52"   → emit an OSC 52 escape sequence on the terminal so the
//     terminal emulator (including SSH-forwarded ones) writes the payload
//     into the user's clipboard.
//   - "auto"    → run both transports in parallel and succeed when either
//     does.
//   - "off"     → discard the payload (no-op).
//
// The package is consumed via the [Clipboard] interface so screen code can
// pass a clipboard mock in tests.
package clipboard

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// Method selects the transport. Values are matched case-insensitively;
// unknown values fall back to [MethodAuto] so a typo in config does not
// disable copy entirely.
type Method string

const (
	// MethodAuto runs the native and OSC 52 transports in parallel.
	MethodAuto Method = "auto"
	// MethodNative shells out to pbcopy / xclip / wl-copy.
	MethodNative Method = "native"
	// MethodOSC52 emits an OSC 52 escape sequence.
	MethodOSC52 Method = "osc52"
	// MethodOff disables clipboard entirely (Copy is a no-op).
	MethodOff Method = "off"
)

// ParseMethod normalises a user-facing string into a [Method]. Empty input
// returns [MethodAuto]; unknown input returns ("", error) so callers can
// surface a config validation error.
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

// Clipboard is the contract consumed by screen code. Implementations must
// be safe for concurrent use — copy hotkeys may overlap with auto-refresh
// goroutines.
type Clipboard interface {
	Copy(ctx context.Context, payload string) error
}

// Options configures [New].
type Options struct {
	// Method picks the transport. Empty defaults to [MethodAuto].
	Method Method
	// Native, when non-nil, overrides the default native transport. Tests
	// inject a fake here; production code leaves it nil so [DefaultNative]
	// is constructed lazily.
	Native Clipboard
	// OSC52, when non-nil, overrides the default OSC 52 transport.
	OSC52 Clipboard
}

// New constructs a [Clipboard] for the requested method.
//
// For [MethodAuto] both transports are run in parallel and the resulting
// [Clipboard] reports success when either does. For [MethodOff] it returns
// a no-op. For [MethodNative] / [MethodOSC52] only that transport is used.
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

// noopClipboard satisfies [Clipboard] without copying anywhere.
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
