# Design decisions

## Keybindings: text input

All text inputs (form fields, `/` filter, `:` command bar) share one edit
contract implemented in `internal/tui/lineedit` and behave like **readline's
emacs mode** — same shortcuts, same word-boundary rules (`unix-word-rubout`:
skip whitespace, act on the non-whitespace run), same line-bounded semantics
(`ctrl+u` / `ctrl+k` stop at `\n`, not at the buffer edge).

The supported set: `ctrl+a` / `ctrl+e` / `home` / `end`, `ctrl+u` / `ctrl+k`,
`ctrl+w` / `alt+backspace`, `alt+b` / `alt+f`, arrow keys, `backspace` /
`delete`. Anything not listed here is not part of the contract.

## Forms: view mode vs edit mode

Modal forms (`components/form.go`) have two modes:

- **NORMAL** (view) — no field is being edited; keys are form-level (navigate
  between fields, clear form, send, etc).
- **INSERT** (edit) — a single field is being edited; keys are forwarded to
  that field via `lineedit`.

`ctrl+u` is bound at both levels — in NORMAL it clears the whole form, in
INSERT it is the readline kill-to-line-start on the focused field. Mode is
exclusive, so they never collide. This also resolves the previous `ctrl+r`
conflict (see below).

## Reserved global shortcuts

Owned by the host dispatcher (`key_dispatch.go`) and not to be rebound by
screens:

- `ctrl+c` — quit
- `:` — command bar
- `/` — filter prompt
- `?` — help
- `ctrl+r` — toggle auto-refresh

Globals whose semantics do not apply inside an overlay (e.g. `ctrl+r` on a form
that has nothing to refresh) are **skipped** when the active screen reports
`HasOverlay() == true`, so the key falls through instead of silently firing a
no-op global.

## Editor handoff

Forms that allow opening the focused field in `$EDITOR` use `ctrl+o` (Open).
`ctrl+e` is reserved for "move to end of line" everywhere.
