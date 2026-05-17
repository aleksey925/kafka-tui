package components

import (
	tea "charm.land/bubbletea/v2"

	"github.com/aleksey925/kafka-tui/internal/tui/keymap"
)

// ScrollCursor lets a screen route the helper's vertical keys
// (j/k/ctrl+f/b/pgup/pgdn/home/end) through its own cursor callbacks
// instead of the viewport's built-in scroll/cursor methods.
//
// Use this when the screen's cursor lives in a space that differs from
// the viewport's visual line index — e.g. a list with section headers
// the cursor must skip, or a log buffer where the cursor is on logical
// match lines rather than wrapped visual lines.
//
// When ScrollCursor is non-nil, every Move/PageStep/ToTop/ToBottom
// callback must be set. When ScrollCursor is nil, the helper falls back
// to its built-in mode (see ScrollBindings docs).
type ScrollCursor struct {
	Move     func(delta int)
	PageStep func() int
	ToTop    func()
	ToBottom func()
}

// scrollMode controls how the shared scroll keymap's handlers route
// vertical motion. Exactly one branch is taken per handler call; the
// branch is re-evaluated on every keypress so a screen that toggles its
// viewport's cursor on / off does not need to rebuild bindings.
type scrollMode struct {
	cursor     *ScrollCursor // explicit-callback mode: route through screen
	windowOnly bool          // window-scroll mode: ignore any viewport cursor
}

// ScrollBindings returns the canonical viewport scroll keymap routed
// against v. It is the single source of truth for j/k/ctrl+f/b/pgup/pgdn/
// home/end and h/l, so every screen with a viewport shows the same keys
// with the same labels in the `?` help.
//
// Default mode (cursor == nil): when v carries a cursor (v.Cursor() >= 0),
// the vertical keys manipulate that cursor via SetCursor — the viewport
// scrolls as a side effect of EnsureCursorVisible, so the window
// auto-follows. When v has no cursor (read-only viewers like the message
// detail body), the same keys scroll the window directly. The mode is
// checked at handler time, so a screen that toggles cursor on/off does
// not need to rebuild its bindings.
//
// Explicit-callback mode (cursor != nil): vertical keys go through the
// screen's callbacks, which is required when the cursor's coordinate
// system differs from the viewport's visual line index. The viewport is
// still consulted for h/l geometry.
//
// h/l are emitted only when wrap is off, so the help screen advertises
// them only when they actually pan the window. Wrap toggling (w) is
// intentionally excluded — content pre-wrapping is the screen's
// responsibility, so each screen declares its own w binding.
func ScrollBindings(v *Viewport, cursor *ScrollCursor) []keymap.Binding {
	return scrollBindings(v, scrollMode{cursor: cursor})
}

// WindowScrollBindings is the window-only variant: vertical keys scroll
// the visible window even when the viewport carries a cursor. Used by
// the form's textarea / list panning path — the field's caret / row
// cursor is part of the rendered content and is addressed through the
// form's own INSERT-mode bindings, not through j/k at the screen level.
// The returned bindings are dispatched by the form via [keymap.Dispatch],
// so the keys / labels / categories stay in lockstep with [ScrollBindings].
func WindowScrollBindings(v *Viewport) []keymap.Binding {
	return scrollBindings(v, scrollMode{windowOnly: true})
}

func scrollBindings(v *Viewport, mode scrollMode) []keymap.Binding {
	move := func(delta int) {
		switch {
		case mode.cursor != nil:
			mode.cursor.Move(delta)
		case mode.windowOnly, v.Cursor() < 0:
			v.ScrollBy(delta)
		default:
			moveViewportCursor(v, delta)
		}
	}
	page := func(sign int) {
		switch {
		case mode.cursor != nil:
			mode.cursor.Move(sign * mode.cursor.PageStep())
		case mode.windowOnly, v.Cursor() < 0:
			if sign > 0 {
				v.PageDown()
			} else {
				v.PageUp()
			}
		default:
			moveViewportCursor(v, sign*v.PageStep())
		}
	}
	toTop := func() {
		switch {
		case mode.cursor != nil:
			mode.cursor.ToTop()
		case mode.windowOnly, v.Cursor() < 0:
			v.ScrollToTop()
		default:
			v.SetCursor(0)
		}
	}
	toBottom := func() {
		switch {
		case mode.cursor != nil:
			mode.cursor.ToBottom()
		case mode.windowOnly, v.Cursor() < 0:
			v.ScrollToBottom()
		default:
			if last := v.TotalLines() - 1; last >= 0 {
				v.SetCursor(last)
			}
		}
	}
	bs := []keymap.Binding{
		{Keys: []string{"j", "down"}, Label: "scroll down", Category: "Movement", Handler: func() tea.Cmd { move(+1); return nil }},
		{Keys: []string{"k", "up"}, Label: "scroll up", Category: "Movement", Handler: func() tea.Cmd { move(-1); return nil }},
		{Keys: []string{"ctrl+f", "pgdown"}, Label: "page down", Category: "Movement", Handler: func() tea.Cmd { page(+1); return nil }},
		{Keys: []string{"ctrl+b", "pgup"}, Label: "page up", Category: "Movement", Handler: func() tea.Cmd { page(-1); return nil }},
		{Keys: []string{"home"}, Label: "scroll to top", Category: "Movement", Handler: func() tea.Cmd { toTop(); return nil }},
		{Keys: []string{"end"}, Label: "scroll to bottom", Category: "Movement", Handler: func() tea.Cmd { toBottom(); return nil }},
	}
	if !v.Wrap() {
		bs = append(bs,
			keymap.Binding{Keys: []string{"h", "left"}, Label: "scroll left", Category: "Movement", Handler: func() tea.Cmd { v.HScrollBy(-v.HStep()); return nil }},
			keymap.Binding{Keys: []string{"l", "right"}, Label: "scroll right", Category: "Movement", Handler: func() tea.Cmd { v.HScrollBy(+v.HStep()); return nil }},
		)
	}
	return bs
}

func moveViewportCursor(v *Viewport, delta int) {
	next := max(v.Cursor()+delta, 0)
	if last := v.TotalLines() - 1; next > last {
		next = last
	}
	v.SetCursor(next)
}
