// Package keymap is the single source of truth for screen key
// bindings. Each screen builds a slice of [Binding] entries; the same
// slice drives (a) the dispatcher that handles keystrokes, (b) the
// bottom-bar key hints, and (c) the `?` overlay's help sections.
//
// The point is to make documentation drift impossible by construction:
// a keystroke that the dispatcher recognizes MUST exist in the
// bindings slice, and any binding in the slice MUST be exposed in the
// help overlay. Adding a new shortcut is one append, removing one is
// one delete — there is no parallel list to keep in sync.
//
// This package is intentionally dependency-free with respect to the
// rendering layer: it knows nothing about lipgloss styles, help
// sections, or the chrome. Conversion of [Binding] slices into
// presentation types lives in the consuming packages (`help`,
// `layout`) so the dependency arrow points one way: rendering depends
// on keymap, never the reverse.
package keymap

import (
	"slices"
	"strconv"
	"strings"

	tea "charm.land/bubbletea/v2"
)

// Binding declares a single keystroke (or alias group) and what
// happens when it fires.
type Binding struct {
	// Keys is one or more keystroke strings (matching tea.KeyPressMsg.String()
	// values) that all trigger this binding. The first entry is the
	// canonical form shown in help / hints.
	Keys []string

	// Label is the short human-readable description shown next to the
	// key in both the bottom hints bar and the help overlay.
	Label string

	// Category groups the binding inside the `?` overlay (e.g.
	// "Browse", "Filtering"). Empty means the binding is not surfaced
	// in help — used for hidden aliases like arrow keys forwarded to
	// table components.
	Category string

	// Hint reports whether the binding should appear in the bottom
	// key-hints bar. The bar is space-constrained, so most bindings
	// stay help-only; flagging Hint=true promotes them.
	Hint bool

	// Handler runs when one of [Keys] is pressed. May return nil. The
	// dispatcher forwards the result to the host. Mutually exclusive
	// with [HandlerMsg]; if both are set, HandlerMsg wins.
	Handler func() tea.Cmd

	// HandlerMsg is an alternative to [Handler] for bindings whose
	// implementation needs to forward the original [tea.KeyPressMsg]
	// into a downstream component (typically a form or table).
	HandlerMsg func(tea.KeyPressMsg) tea.Cmd
}

// Display returns the canonical "key" string used in help and hints
// (aliases joined by " / ").
func (b Binding) Display() string {
	switch len(b.Keys) {
	case 0:
		return ""
	case 1:
		return b.Keys[0]
	default:
		return strings.Join(b.Keys, " / ")
	}
}

// hasHandler reports whether the binding has any executable handler
// attached. Bindings without a handler are advertise-only — they
// appear in help and hints but the dispatcher skips them so the
// caller falls through to whatever fallback owns the keystroke
// (typically a global shortcut or a child component's input).
func (b Binding) hasHandler() bool {
	return b.Handler != nil || b.HandlerMsg != nil
}

// Dispatch finds the binding whose Keys contain msg.String() and
// invokes its handler. Returns (cmd, true) when a binding matched and
// fired; (nil, false) otherwise so the caller can fall through to
// default routing (e.g. forwarding to a table component).
//
// Bindings declaring HandlerMsg get the original keystroke event;
// bindings declaring only Handler are dispatched without it.
// Advertise-only bindings (no handler at all) silently fall through
// — they exist solely to surface the keystroke in help/hints when
// some other layer (the host, a child component) actually owns it.
func Dispatch(bindings []Binding, msg tea.KeyPressMsg) (tea.Cmd, bool) {
	key := msg.String()
	for _, b := range bindings {
		if !b.hasHandler() {
			continue
		}
		if !slices.Contains(b.Keys, key) {
			continue
		}
		if b.HandlerMsg != nil {
			return b.HandlerMsg(msg), true
		}
		return b.Handler(), true
	}
	return nil, false
}

// Validate enforces the bindings-table invariants: no empty key list,
// no empty label, no duplicate key across the slice. Designed to be
// called from a unit test per screen so a malformed table fails CI
// rather than shipping silently broken help.
func Validate(bindings []Binding) error {
	seen := make(map[string]string, len(bindings)*2)
	var errs []string
	for i, b := range bindings {
		if len(b.Keys) == 0 {
			errs = append(errs, fmtIdx(i)+": empty Keys")
			continue
		}
		if strings.TrimSpace(b.Label) == "" {
			errs = append(errs, fmtIdx(i)+" ("+b.Display()+"): empty Label")
		}
		for _, k := range b.Keys {
			if prev, ok := seen[k]; ok {
				errs = append(errs, "duplicate key "+k+": already bound by "+prev)
				continue
			}
			seen[k] = b.Display() + " (" + b.Label + ")"
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return validationError(strings.Join(errs, "; "))
}

type validationError string

func (e validationError) Error() string { return string(e) }

func fmtIdx(i int) string {
	return "binding[" + strconv.Itoa(i) + "]"
}
