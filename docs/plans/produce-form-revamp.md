# Produce form revamp

## Overview

Rework the produce screen (`internal/tui/screens/produce/`) to be more
comfortable for real use: bigger usable area, proper text editing with cursor
navigation, more discoverable headers UX, segmented compression picker, and a
two-column layout with an opt-in fullscreen mode for any single field.

## Context

- Primary files: `internal/tui/screens/produce/produce.go`,
  `internal/tui/components/form.go`
- Tests: `internal/tui/screens/produce/produce_test.go`,
  `internal/tui/components/form_test.go`
- Theme palette: `internal/tui/theme/`
- Layout chrome: `internal/tui/layout/` (header, status, key hints) — width and
  height are passed in via `SetSize`; the screen owns body layout.

## Pain points (current state)

1. **Form area is cramped.** `View()` centers a `lipgloss` box sized strictly
   to its content (`produce.go:563`). The form never grows into the available
   space, so value/headers always feel tight.
2. **Text editing is broken.** `updateText` in `form.go:270` only handles
   `backspace`, `enter` (textarea only) and appends `key.Text`. There is no
   in-line cursor: arrow keys, `home/end`, `delete`, `ctrl+w`, etc. do
   nothing. Editing the middle of an existing string is impossible.
3. **Headers UX is opaque.** `FieldList` (`form.go:317`) uses `ctrl+a` /
   `ctrl+d` to add/remove and has no in-row editing. The hotkeys are barely
   discoverable and the same field-kind disagrees with how text fields behave.
4. **Compression dropdown looks bulky.** When focused, `FieldDropdown` expands
   into a vertical radio list (`form.go:229`). The user wants a compact inline
   slider (`◂ snappy ▸`) that opens a popup list on demand.

## Design decisions (locked)

### Layout — two columns + fullscreen toggle

- **Left column (~28–32 cols, fixed):** `Topic`, `Partition`, `Compression`,
  `Key`. Compact one-line meta fields, always visible.
- **Right column (rest of width):** `Headers` (top, ~30% height) + `Value`
  (bottom, ~70% height). Both visible simultaneously by default.
- **Fixed 30/70 split** between headers and value — does not "breathe" when
  focus moves between them. If content overflows its slot, the field scrolls
  internally; an indicator like `… +N more lines (shift++ to expand)` hints at
  the fullscreen mode.
- **Tab order** wraps top-to-bottom through left column, then top-to-bottom
  through right column: Topic → Partition → Compression → Key → Headers →
  Value → (back to Topic). `shift+tab` reverses.

### Fullscreen mode (mode B)

- Toggled by `shift++` / `shift+-`. Both keys cycle the same two-state
  carousel: any press flips between mode A (split) and mode B (fullscreen).
  Multiple presses simply keep flipping.
- In mode B: a tab strip at the top of the screen lists **all six fields**
  (`Topic`, `Partition`, `Compression`, `Key`, `Headers`, `Value`) with the
  active one highlighted. Below the strip the active field uses the full
  remaining height/width.
- `tab` / `shift+tab` in mode B switches the active tab (i.e. the active
  field). Tab is therefore *never* inserted into a textarea body — if literal
  tabs are needed in JSON, the user pastes them or uses `$EDITOR` (`ctrl+e`).
- `esc` in mode B drops back to mode A. `esc` in mode A closes the form (as
  today).

### Compression as segmented inline picker

- New field kind `FieldSegmented` (or extend `FieldDropdown` with a "compact"
  render mode — naming TBD during implementation):
  - **Default render:** single line `Compression: ◂ snappy ▸`. ←/→ cycle the
    value in place.
  - **`enter` opens a popup** with the full vertical list (current dropdown
    UI). ←/→ or ↑/↓ navigate, `enter` confirms, `esc` cancels.
- Replaces the current always-expanded radio list, gives the left column a
  compact, single-line footprint.
- **In fullscreen mode (mode B):** when the active tab is `Compression`,
  the field renders as the expanded vertical list (popup view), not the
  compact slider. Compact form is for the cramped left column in mode A;
  fullscreen has the room and the user wants to see all options at once.

### Text input — proper cursor

Rework `FieldText` and `FieldTextarea` so editing matches normal expectations:

- Left/right arrows move the cursor; `home`/`end` to line bounds.
- `backspace` / `delete` operate at the cursor.
- `FieldTextarea`: up/down arrows move between lines; `enter` inserts newline
  at cursor (today only appends).
- Cursor renders inline (block/bar) at its position, not at the trailing edge.
- Implementation choice (decide during impl): write a minimal cursor model in
  `form.go`, **or** integrate `charmbracelet/bubbles` `textinput` / `textarea`
  if compatible with `charm.land/bubbletea/v2`. Prefer the latter if it works
  out-of-the-box; the in-house version is the fallback.

### Headers UX

**Not finalized.** Direction we lean toward: keep `FieldList` shape (one entry
per header) but add per-row in-line editing (same cursor work as text fields)
and surface visible affordances `[+ add]` / `[− remove]` plus more discoverable
hotkeys (`enter` on last entry → add new, `backspace` on empty entry →
remove). To be revisited after the text-input rework lands so we can reuse
its cursor logic.

## Known caveats

- `shift++` / `shift+-` work in both kitty and Apple Terminal (verified by user
  via lazygit). No fallback hotkey planned.

## Implementation order

Each step is shippable on its own and lands with tests + `make lint && make
test`.

### Task 1 — Compression as segmented picker

**Files:**
- Modify: `internal/tui/components/form.go` (new field kind / compact render +
  popup state)
- Modify: `internal/tui/components/form_test.go`
- Modify: `internal/tui/screens/produce/produce.go` (use the new kind for
  compression)
- Modify: `internal/tui/screens/produce/produce_test.go`

- [ ] add segmented inline render: `◂ value ▸`, ←/→ cycle in place
- [ ] `enter` opens popup overlay with full options list; `esc` cancels;
      `enter` confirms
- [ ] update produce form to use segmented kind for `Compression`
- [ ] tests: cycling, popup open/close, value persistence

### Task 2 — Cursor-based text editing

**Files:**
- Modify: `internal/tui/components/form.go` (cursor state per text field,
  rewritten `updateText`, cursor-aware render)
- Modify: `internal/tui/components/form_test.go`

- [ ] decide: vendor `bubbles/textinput`+`textarea` or implement minimal
      cursor model in-house (single decision applies to both kinds)
- [ ] support: ←/→, `home`/`end`, `delete`, mid-string `backspace`,
      mid-string insert; for textarea also ↑/↓ and `enter` at cursor
- [ ] cursor renders at its real position, not trailing
- [ ] tests covering all editing operations on multi-line content

### Task 3 — Headers UX

**Files:**
- Modify: `internal/tui/components/form.go` (in-row editing for `FieldList`)
- Modify: `internal/tui/components/form_test.go`
- Modify: `internal/tui/screens/produce/produce.go` (visible add/remove
  affordances; updated key hints)

- [ ] reuse cursor model from Task 2 for in-row editing
- [ ] `enter` on the last entry adds a new empty entry; `backspace` on an
      empty entry removes it (besides the existing `ctrl+a` / `ctrl+d`)
- [ ] render `[+ add]` / `[− remove]` affordances inside the field
- [ ] tests for the new shortcuts and rendering

### Task 4 — Two-column layout + fullscreen mode

**Files:**
- Modify: `internal/tui/screens/produce/produce.go` (layout, mode state,
  hotkeys, esc behavior, key hints)
- Modify: `internal/tui/screens/produce/produce_test.go`

- [ ] split form rendering into left meta column (Topic / Partition /
      Compression / Key) and right column (Headers + Value, fixed 30/70)
- [ ] right-column overflow indicator with `shift++` hint
- [ ] fullscreen mode: top tab strip listing all six fields, active highlighted
- [ ] hotkeys: `shift++` / `shift+-` toggle mode (carousel); in mode B `tab`
      cycles the active field
- [ ] in mode B, force segmented Compression field into expanded popup form
      so it renders as the vertical option list, not the compact slider
- [ ] `esc` in mode B → back to mode A; `esc` in mode A → close form
- [ ] update `KeyHints()` and the trailing hint line in `View()`
- [ ] tests: mode toggle, tab cycling in mode B, esc behavior in both modes

## Task 5 — NORMAL / INSERT modal editing

To support literal `tab` insertion in the textarea (and to give the form a
predictable command surface for things like add/remove header), the produce
screen gains two explicit modes — borrowed from vim/k9s.

### Spec

**NORMAL mode (default).**
- `tab` / `shift+tab` move focus between fields.
- `+` / `_` toggle fullscreen (existing).
- `enter` is contextual:
  - text / textarea → enter INSERT for the focused field.
  - segmented (Compression) → open popup (its native action).
  - list (Headers) → enter INSERT for the current row; if the list is
    empty, add a new row first then INSERT.
- On the Headers field only:
  - `=` adds a new empty row and enters INSERT on it.
  - `-` removes the focused row.
- `ctrl+s` / `ctrl+shift+s` / `ctrl+e` / `ctrl+r` / `ctrl+p` / `ctrl+n` work
  as today, mode-agnostic.
- `esc` cascade: popup → close popup; fullscreen → split; otherwise → close
  form.
- Printable letters/digits and editing keys (backspace/delete/arrows) are
  ignored (they only do work in INSERT).

**INSERT mode.**
- Entered via `enter` on a text-like field; left via `esc`.
- All printable keys (including `+`, `-`, `=`, `_`) are inserted literally.
- `tab` in textarea → inserts `\t`. `tab` in single-line text → commit and
  navigate to the next field, returning to NORMAL.
- `shift+tab` → commit and navigate to the previous field, returning to
  NORMAL (in both text and textarea).
- `enter` in textarea → newline at cursor. `enter` in single-line text →
  commit and return to NORMAL on the same field.
- `esc` → return to NORMAL on the same field, no commit needed.

**Visual.**
- A `[NORMAL]` / `[INSERT]` badge sits in the trailing hint line.
- The text caret (`▌`) is rendered only in INSERT — in NORMAL the value is
  shown without a cursor; the focused indicator (`▸ Label` + accent colour)
  is enough to mark which field is active.

### Files

- Modify: `internal/tui/screens/produce/produce.go` (mode state, mode-aware
  routing, mode badge, contextual Enter, `=` / `-` for headers).
- Modify: `internal/tui/components/form.go` (`SetEditing(bool)` to hide
  caret in NORMAL; drop the “enter on last → add row” shortcut shipped in
  Task 3 — replaced by NORMAL `=`).
- Modify: tests across both packages — `typeText` helper enters INSERT
  before typing; new tests cover mode transitions and the new `=` / `-`
  shortcuts.

### Steps

- [ ] drop `enter on last list entry → add new row` in `form.go`; remove
      / invert the corresponding tests
- [ ] add `editing` flag + `SetEditing(bool)` to `Form`; render caret only
      when editing
- [ ] add `Mode` enum + state to `produce.Model`, default NORMAL
- [ ] mode-aware `handleKey`: NORMAL forwards only navigation/commands;
      INSERT forwards typing/cursor keys to the form, intercepts
      tab/enter/shift+tab for context-specific commit logic
- [ ] enter NORMAL → INSERT on `enter` (text/textarea/list); on segmented,
      open popup instead
- [ ] `=` adds row on Headers in NORMAL (and enters INSERT on the new row);
      `-` removes the focused row
- [ ] mode badge in `View()`; update key-hint strings for both modes
- [ ] update `typeText` helper to auto-enter INSERT; update existing tests
      that rely on the old always-INSERT behaviour
- [ ] new tests: NORMAL→INSERT via Enter, esc returns to NORMAL, tab
      inserts in textarea INSERT, single-line tab commits, `=` adds
      header, `-` removes header, NORMAL letters are ignored, fullscreen
      `+` still works in NORMAL but is literal in INSERT
- [ ] `make lint && make test`

## What's intentionally NOT in this plan

- Soft auto-zoom / dynamic ratio when focus moves (rejected — user wants stable
  layout in mode A).
- `ctrl+z` / `ctrl+f` / `alt+enter` for fullscreen (rejected — `shift++/-`
  decided).
- Graduated zoom levels (rejected — binary toggle only).
- Tabs containing only "large" fields in fullscreen (rejected — all six
  fields are tabs).
