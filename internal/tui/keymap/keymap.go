// Package keymap is the single source of truth for screen key bindings.
// Each screen builds a slice of [Binding] entries; the same slice drives
// the dispatcher, the bottom-bar key hints, and the `?` overlay's sections.
// The package is dependency-free with respect to rendering — conversion to
// presentation types lives in `help` and `layout`.
package keymap

import (
	"slices"
	"strconv"
	"strings"

	tea "charm.land/bubbletea/v2"
)

// Binding declares a single keystroke (or alias group) and what happens
// when it fires.
type Binding struct {
	// Keys is one or more keystroke strings (matching tea.KeyPressMsg.String()
	// values). Order matters: the first entry is canonical for [Display];
	// pick it by context (vim-first for lists, tab-first for form fields).
	Keys []string

	// DisplayKeys, if non-empty, overrides Keys for [Display] only —
	// dispatch still uses Keys. Used to suppress protocol-duplicate aliases
	// of the same physical key from the rendered hint. Must be a subset of
	// Keys; enforced by [Validate].
	DisplayKeys []string

	Label string

	// Category groups the binding inside the `?` overlay. Empty hides the
	// binding from help — used for hidden aliases.
	Category string

	// Hint promotes the binding into the bottom key-hints bar.
	Hint bool

	// Handler runs when one of [Keys] is pressed. Mutually exclusive with
	// [HandlerMsg]; if both are set, HandlerMsg wins.
	Handler func() tea.Cmd

	// HandlerMsg is an alternative for bindings whose implementation needs
	// to forward the original [tea.KeyPressMsg] into a downstream component.
	HandlerMsg func(tea.KeyPressMsg) tea.Cmd
}

// FocusToggle is the canonical binding for two-pane focus switching:
// tab and shift+tab share one handler. Multi-field cycles use paired
// next/prev bindings instead.
func FocusToggle(label, category string, handler func() tea.Cmd) Binding {
	return Binding{
		Keys:     []string{"tab", "shift+tab"},
		Label:    label,
		Category: category,
		Hint:     true,
		Handler:  handler,
	}
}

func (b Binding) Display() string {
	keys := b.Keys
	if len(b.DisplayKeys) > 0 {
		keys = b.DisplayKeys
	}
	switch len(keys) {
	case 0:
		return ""
	case 1:
		return keys[0]
	default:
		return strings.Join(keys, " / ")
	}
}

// hasHandler reports whether the binding is dispatchable. Advertise-only
// bindings (no handler) appear in help/hints but fall through to whatever
// other layer owns the keystroke.
func (b Binding) hasHandler() bool {
	return b.Handler != nil || b.HandlerMsg != nil
}

// Dispatch finds the binding whose Keys contain msg.String() and invokes
// its handler. Returns (nil, false) when no binding matched, so the caller
// can fall through to default routing.
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

// Validate enforces invariants: no empty key list, no empty label, no
// duplicate key, and that tab is always bound together with shift+tab
// (either as a single toggle or as paired next/prev bindings) so
// selection navigation works both ways on every screen. Called from
// per-screen unit tests to fail CI on a malformed table.
func Validate(bindings []Binding) error {
	seen := make(map[string]string, len(bindings)*2)
	var hasTab, hasShiftTab bool
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
			switch k {
			case "tab":
				hasTab = true
			case "shift+tab":
				hasShiftTab = true
			}
		}
		for _, dk := range b.DisplayKeys {
			if !slices.Contains(b.Keys, dk) {
				errs = append(errs, fmtIdx(i)+" ("+b.Display()+"): DisplayKeys "+dk+" not in Keys")
			}
		}
	}
	if hasTab && !hasShiftTab {
		errs = append(errs, "tab is bound without shift+tab — selection navigation must work in both directions")
	}
	if hasShiftTab && !hasTab {
		errs = append(errs, "shift+tab is bound without tab — selection navigation must work in both directions")
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
