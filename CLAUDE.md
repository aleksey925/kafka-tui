# Design decisions

## Single source of behavior

When a behavior must be uniform across the app (edit semantics, paste
sanitization, global key handling, etc.), it lives in one shared place that
all consumers route through. New screens and inputs reuse that
implementation rather than re-deriving it.

Why: the contracts described below (readline emacs mode for text input,
plain-text sanitization for paste, the reserved globals set) hold by
default, not by discipline. If every screen had to re-implement edit or
paste handling, the contract would drift the moment somebody forgot a
detail. Implementation form is not prescribed (shared module, common base,
component wrapper — whatever fits); the rule is that there is exactly one
implementation and everything goes through it.

Instances of this rule below: **Text input** (one edit contract), **Reserved
global shortcuts** (one dispatcher), **Paste** (one sanitization point),
**Handing the terminal off** (one handoff path for full-screen subprocesses).

## Text input

*Applies the single-source rule above: one shared edit contract for every
text input in the app.*

All text inputs (form fields, `/` filter, `:` command bar) share one edit
contract: **readline emacs mode** — including `unix-word-rubout` word
boundaries and line-bounded kills (`ctrl+u` / `ctrl+k` stop at `\n`, not at
the buffer edge). Anything outside readline emacs mode is not part of the
contract.

## Forms: NORMAL / INSERT modes

Modal forms are modal in the vim sense:

- **NORMAL** — no field is being edited, keys are form-level (navigate,
  clear, send).
- **INSERT** — one field is being edited, keys go to that field.

Modes are exclusive, so the same key can mean different things at each level
without collision (e.g. `ctrl+u` clears the form in NORMAL and kills to
line-start in INSERT). Transitions between modes are explicit — the user
presses a key to enter or leave INSERT.

## Reserved global shortcuts

*Applies the single-source rule above: one dispatcher owns these keys; no
screen rebinds them locally.*

A fixed set of keys is owned by the host dispatcher and not rebindable by
screens:

- `ctrl+c` — quit
- `:` — command bar
- `/` — filter prompt
- `?` — help
- `ctrl+r` — toggle auto-refresh

Deviation: when the active screen has an overlay (e.g. a modal form), a
global is skipped if its action does not apply in that context (e.g.
`ctrl+r` auto-refresh has nothing to refresh inside a form). The key falls
through to the overlay instead of firing a no-op global.

## Handing the terminal off

*Applies the single-source rule above: one shared handoff path for any
full-screen subprocess.*

When the app launches an external full-screen process (editor, pager, etc.),
it must cleanly release the terminal before that process runs and restore it
after. Otherwise the user comes back to a corrupted screen — the TUI and the
child end up fighting over the same tty.

Convention for editor-style handoffs: `ctrl+o` (open) — `ctrl+e` is reserved
for "move to end of line" everywhere.

## Paste

We rely on the terminal's bracketed-paste. `ctrl+v` is never bound by us —
that would either duplicate or break the terminal's native paste shortcut.
Users use whatever their terminal expects (`cmd+v`, `ctrl+shift+v`, etc.).

All pasted content goes through a single sanitization point that treats it
as plain text only (this is the **single-source rule** above, applied to
paste): escape sequences and control characters in the payload can't be
interpreted or corrupt the rendered output. Paste into a single-line field
is flattened (newlines and tabs become spaces) rather than rejected or
truncated. Non-text controls (dropdowns, segmented selectors) ignore paste.

Deviation: in modal forms, paste auto-transitions NORMAL → INSERT when the
focused field is text-like — the only implicit mode crossing. Without it the
user would paste into a field they haven't entered.
