## What this is

kafka-tui is a terminal client for Apache Kafka in the spirit of k9s. It lets
developers browse topics, produce messages, and inspect consumer groups
without leaving the shell.

## Design decisions

### Reference UX: k9s

The app's UX is largely modeled on k9s — navigation flow, modal browsing, and
shortcut conventions. Where a k9s shortcut maps cleanly to a Kafka concept, we
reuse it; where there is no direct analogue, we follow the same shape
(single-letter actions in browse contexts, capitals for mutating variants,
`ctrl+` only when a modifier is genuinely needed) rather than invent a new
style.

### Single source of behavior

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
**Handing the terminal off** (one handoff path for full-screen subprocesses),
**Bounded display** (one viewport for vertical overflow, one truncate helper
for horizontal), **Toast / flash routing** (one flash bar for every screen's
toasts), **Tab navigation** (one paired contract for forward/backward).

### Text input

*Applies the single-source rule above: one shared edit contract for every
text input in the app.*

All text inputs (form fields, `/` filter, `:` command bar) share one edit
contract: **readline emacs mode** — including `unix-word-rubout` word
boundaries and line-bounded kills (`ctrl+u` / `ctrl+k` stop at `\n`, not at
the buffer edge). Anything outside readline emacs mode is not part of the
contract.

### Forms: NORMAL / INSERT modes

Modal forms are modal in the vim sense:

- **NORMAL** — no field is being edited, keys are form-level (navigate,
  clear, send).
- **INSERT** — one field is being edited, keys go to that field.

Modes are exclusive, so the same key can mean different things at each level
without collision (e.g. `ctrl+u` clears the form in NORMAL and kills to
line-start in INSERT). Transitions between modes are explicit — the user
presses a key to enter or leave INSERT.

### Reserved global shortcuts

*Applies the single-source rule above: one dispatcher owns these keys; no
screen rebinds them locally.*

A fixed set of keys is owned by the host dispatcher and not rebindable by
screens:

- `ctrl+c` — quit
- `:` — command bar
- `/` — filter prompt
- `?` — help
- `ctrl+r` — open the refresh-interval picker (presets + custom). On screens
  with no auto-refresh concept (forms, message detail, …) the key is a
  no-op rather than mounting an inert popup.

Deviation: when the active screen has an overlay (e.g. a modal form, or the
picker itself), a global is skipped if its action does not apply in that
context. The key falls through to the overlay instead of firing a no-op
global — that's how the picker's own `esc` closes it without the dispatcher
trying to remount one on top.

### Filter clear

When a screen has an applied filter (the `/` prompt is closed and rows are
filtered), `esc` both clears the filter and pops the screen in the same
keypress; `ctrl+u` clears the filter and stays on the screen.

Why: this matches k9s, where `esc` on a filtered view is a single "clear
and go back" action and `ctrl+u` is the readline-style "wipe the buffer
without navigating". Splitting `esc` into a two-press cascade would lose
the muscle memory users bring from k9s, so the asymmetry between `esc`
and `ctrl+u` here is deliberate, not a forgotten early return.

### Tab navigation

*Applies the single-source rule above: one paired contract for
forward/backward selection navigation, never one half of it.*

Screens with more than one selection target (panes, form fields, a
popup's list-and-input split) advertise `tab` (forward) and `shift+tab`
(backward) together — never one without the other.

Why: users carry `tab` / `shift+tab` from forms and editors universally,
so binding one without the other leaves the reverse silently
unavailable.

### Handing the terminal off

*Applies the single-source rule above: one shared handoff path for any
full-screen subprocess.*

When the app launches an external full-screen process (editor, pager, etc.),
it must cleanly release the terminal before that process runs and restore it
after. Otherwise the user comes back to a corrupted screen — the TUI and the
child end up fighting over the same tty.

Convention for editor-style handoffs: `e` (open). `ctrl+e` must never be used
— it is reserved for "move to end of line" in text input.

### Paste

We rely on the terminal's bracketed-paste. `ctrl+v` is never bound by us —
that would either duplicate or break the terminal's native paste shortcut.
Users use whatever their terminal expects (`cmd+v`, `ctrl+shift+v`, etc.).

All pasted content goes through a single sanitization point that treats it
as plain text only (this is the **single-source rule** above, applied to
paste): escape sequences and control characters in the payload can't be
interpreted or corrupt the rendered output. Within a screen, paste is
routed to whichever sub-overlay currently owns a text buffer (picker,
inline prompt, command bar); with no text-owning overlay open the event is
dropped. Paste into a single-line field is flattened (newlines and tabs
become spaces) rather than rejected or truncated. Non-text controls
(dropdowns, segmented selectors) ignore paste.

Deviation: in modal forms, paste auto-transitions NORMAL → INSERT when the
focused field is text-like — the only implicit mode crossing. Without it the
user would paste into a field they haven't entered.

### Bounded display

*Applies the single-source rule above: content that can exceed its allotted
space is handled in one of two canonical ways — neither is reinvented per
screen.*

Long content is bounded along the dimension it overflows on:

- **Vertical overflow** (textareas, message detail, log tail, multi-row
  lists) — rendered through a bounded viewport with a single scroll keymap:
  `j` / `k`, `pgup` / `pgdn`, `ctrl+b` / `ctrl+f`, `home` / `end`, plus
  `w` to toggle wrap. When a cursor is present (INSERT in
  forms, the selected row in logs) the window auto-follows it; otherwise
  it stays put and responds to explicit scroll keys.
- **Horizontal overflow** (table cells, frame chrome, single-line previews
  like message-list values) — clipped with a single trailing `…`, never
  `...`. Truncation is ANSI-aware so styled cells fit their column without
  bleeding into the next.

New screens don't add their own ellipsis style or their own scroll keymap:
bounding happens by content shape, not per screen.

Deviation: the row-based table component has its own row-cursor scroll
(filter / sort / select are baked into the row model), so its vertical
scroll math isn't the shared viewport. Its **cell content** still routes
through the shared truncate helper, so visually the table participates in
the rule even though its scroll path is separate.

### Compound shortcuts via popup menus

*Applies the single-source rule above: every popup menu in the app
shares one implementation (navigation, digit selection, cancel
semantics).*

Where a screen-level action has 2-5 close variants (e.g. copy: record /
key / value / headers; or seek: latest / earliest / from offset / …),
we don't burn a separate key per variant or hide them behind a chord
prefix. The entry key opens a popup menu listing every variant, each
with a digit shortcut (1-N).

Why: the popup is **discoverable** — new users navigate with arrows +
Enter and see every variant labeled — and **fast** — experienced users
get chord-level speed because the digit confirms in a single keypress
(`c` then `2` to copy the key). One key learnt at the screen level,
not N keys scattered across actions; the variant list lives in one
visible place instead of in the help screen.

How to apply: when an action splits into a small fixed set of variants
and a clean one-letter mapping per variant isn't obvious (or clashes
with existing keys like j/k/h navigation), route through the shared
menu component from the screen's entry shortcut. While the popup is
open it owns the input stream — the screen's own bindings (including
digits used elsewhere as view-mode toggles) are suspended until the
menu confirms or cancels.

### Confirm for destructive actions

*Applies the single-source rule above: every "are you sure" modal in
the app shares one key contract (no default cursor, no enter binding,
explicit letter keys).*

A mutation that is **irreversible or causes data loss** is gated
through a confirm modal that shows the minimum context needed to
identify what is about to happen (cluster, topic, group, src→dst,
etc.). A mutation that is reversible (create topic, …) fires
directly from its NORMAL-mode shortcut without a modal — popping a
confirm for trivially-undoable work just trains users to mash `y`.

The confirm contract:

- `y` is the only commit key; cancel is `n` or `esc`.
- No default cursor, no enter binding — a reflexive enter must not
  fire the action.
- Multi-variant confirms (e.g. send & close vs send & keep open) add
  another explicit letter (`k` for keep). Never digits — digit
  popups are the *picker* contract for speed, not the *confirm*
  contract for deliberation.
- The modal owns input while open; the screen's own bindings are
  suspended until it confirms or cancels.
- Validation runs **before** the modal opens: an invalid form
  surfaces an inline error and the modal never mounts, so the user
  isn't asked to confirm a request the broker would have rejected.
- Read-only clusters short-circuit the entry shortcut with a toast,
  also before the modal opens. The UI check is for UX (don't open a
  modal the user can't commit); the load-bearing guard is in the
  kafka package — every mutating method returns [kafka.ErrReadOnly]
  on a read-only cluster. The two layers are intentionally redundant:
  a UI bug or a future direct call still cannot mutate the cluster.

Why: the same y/n muscle memory carries across every destructive
action (delete topic, delete group, save config, produce, clone).
The "no default + different finger" geometry (entry key is `s` /
`ctrl+d`, commit key is `y`) is what breaks the muscle-memory class
of accidents — a popup with default-confirm collapses back into
"one keystroke away" and adds chrome without adding safety.

Deviation: the reset-offsets flow has its own multi-step preview →
`y/Y` commit because the preview *is* the context — reusing the
generic confirm would lose the per-partition diff that's the whole
point of previewing.

How to apply: when adding a new mutation, classify it: irreversible
or data-loss → confirm modal with minimum identifying context;
reversible → direct NORMAL shortcut. The confirm primitive is
existing; multi-variant cases extend it by adding a letter, not by
swapping in the picker menu.

### Toast / flash routing

*Applies the single-source rule above: one flash bar above the body chrome
displays every screen's toasts.*

Errors, warnings, and one-off success messages don't render inline inside
screens. Each screen pushes them onto its toast queue; the host promotes
the most recent live toast into a global flash bar above the body chrome.

Why: inline rendering means each screen reinvents its own error rectangle
(layout, styling, dismiss key, expiry). One bar gives every screen the
same visual language, the same dismiss conventions, the same auto-expiry,
and lets background events (config reload, async failures) surface
without each screen needing a dedicated banner slot.

Deviation: forms blocking on save/send keep a local error inline until
the user fixes it. Their **success** toast is forwarded to the screen the
user lands on after the form closes — not the popped form itself.

How to apply: if the active screen has no toast queue, route the toast to
the screen the user will land on next (the parent / listing screen). A
toast with no surface to land on is a dropped error.

Toasts at warn / error severity are also mirrored to slog so the operator
can recover them after the flash bar expired. The flash bar is for the
moment; the log is for the post-mortem. The mirror lives inside the
shared toast component so screens don't have to remember to also call
slog at every push site — pushing into the queue is the single
operation, mirroring is automatic.

### Auto-refresh: quiet by default

Tick-driven refreshes are **silent** — no success toast on the auto cycle.
Only user-initiated `r` surfaces a confirmation. Recurring or permanent
warnings (broker ACL denial on a batch RPC, etc.) dedup across ticks so
they fire once, not every cycle.

Why: a toast on every tick is noise — it competes with the user's actual
attention and trains them to ignore the flash bar. The asymmetry (`r` is
loud, auto is quiet) keeps the bar useful for things that actually
changed.

### Async lifecycle and stale results

A screen with background work (fetches, dials, RPCs, periodic ticks) must
protect against results that arrive after the screen has moved on — a
different seek, a closed session, a popped screen. Late arrivals that
mutate model state are silent corruption: they don't crash, they show
the wrong data or restart loops that should have died.

Two mechanisms:

- **Generation counter** — bumped on every dispatch, stamped on the
  result message, checked by the handler. Mismatched results are dropped
  before touching state.
- **Screen-scoped context** — held by the screen, cancelled on `Close`.
  Background commands listen on it; late messages from a popped screen
  produce a nil cmd or explicitly close their resource (e.g. a Follow
  session that slipped through closes its connection so its goroutine
  doesn't outlive the model).

Use whichever fits the shape of the work: counter when there are many
concurrent dispatches (seek variants, fetch retries, tick chains),
context when the work is bound to a single long-lived resource.

Generation counters that are scoped to a sub-model (one that gets
re-instantiated on entry — `DetailModel`, popups, etc.) reset to zero on
each re-entry, so the counter alone cannot tell apart a stale result
from a previous instance whose Gen happens to collide. In that case the
result must also carry the entity identity it was issued for (group
name, topic name, …) and the handler must verify it matches the live
model before applying.

Why: tests don't reliably catch the failure modes here — they're tied to
timing. A new screen that omits this protection won't fail in CI; it
will fail when a user pops the screen mid-fetch on a slow broker.

How to apply: any goroutine or background command that calls the network
or sleeps for a tick — stamp it or scope it. No exceptions for "this
fetch is fast enough to not race".

### Placeholder pipeline

*Applies the single-source rule above: one pipeline resolves placeholders in
every input, regardless of where the input came from.*

Strings supplied through any source (YAML config, YAML clusters, CLI flags)
support `${env:...}`, `${file:...}`, `${vault:...}` placeholders. Resolution
is a single staged pipeline; phase order is load-bearing:

1. **env+file** runs on every input, including CLI-supplied targets. This
   is what materializes the vault client's own address and token before the
   vault phase reads them.
2. **vault** runs over every input using a lazy client so the client is
   built only when an actual `${vault:...}` is encountered.
3. **completeness check** scans every input for any remaining `${...}`. A
   leftover placeholder is a hard startup error rather than a value
   silently passed to runtime.

Why: vault.address / vault.token themselves may be supplied as `${env:...}`
or `${file:...}` placeholders, so the env+file phase must materialize them
before the vault phase builds the client from the resolved vault config. Symmetrically,
`${vault:...}` can legitimately appear in CLI flag values (e.g. a SASL
password), so the vault phase must walk CLI targets, not only YAML.
`${vault:...}` is NOT allowed in vault.address / vault.token themselves —
that would be a self-referential lookup.

Deviation: CLI-supplied values are resolved **once at process start and
frozen**. YAML is re-read from disk on every watcher reload, so a rotated
vault secret referenced from YAML picks up the new value on next reload;
the same `${vault:...}` referenced from a CLI flag does not. This is by
design — CLI flags model "process invocation context" and shouldn't drift
mid-process.

### Optional subsystems and graceful degradation

Non-essential subsystems — persistence stores (history, view state,
refresh intervals), clipboard, file watcher, broker metadata, ACL probes
— are **nil-safe**. A missing or failed source disables that one feature,
never the screen.

Pattern: subsystem available → full experience; subsystem absent or
failing → fallback view (rows-derived counts instead of metadata, "no
persistence" mode instead of a crash, blank clipboard slot instead of
refusing to copy). The screen always mounts.

Why: the binary runs in very different environments — a developer's
laptop with full broker access, a CI box with no clipboard, an oncall's
hardened jumpbox with no writable state directory. Optional means the
same binary survives all of them; required means the app refuses to
start on perfectly usable setups.

How to apply: when adding a new dependency to a screen, the nil case is
part of the design, not an afterthought. If the subsystem cannot be
optional (brokers, auth), that's an explicit deviation worth flagging in
review.

### Cluster loading: per-cluster soft fail

A single broken cluster (vault outage for its secret, unresolved
placeholder, TLS conflict, bad SASL mechanism) does not abort startup.
Each cluster runs through its load pipeline independently and failures
are quarantined alongside successes; the picker surfaces broken
clusters as non-selectable rows with the failure reason so the user
sees what's wrong and can edit the YAML, while every other cluster
stays usable.

Why: clusters are often many and unrelated (dev, staging, multiple
production regions). A vault outage that touches only one cluster's
secrets used to deny access to all of them — and oncall doesn't have
time to debug YAML during an incident.

Deviation: global config (vault settings, CLI flags) stays hard-fail.
The vault client cannot dial without a resolved address, and a bad CLI
flag is explicit user input — degrading either situation would just
defer the same error into confusing runtime failures.

### CLI inline cluster: separate namespace from YAML

`--brokers` creates an inline cluster for the session. Its name is
always auto-generated with a `-cli` suffix (random prefix) — never
taken from `--cluster` or any user-controlled source. `--cluster` is a
pure selector that names which already-loaded cluster to auto-connect
to at startup.

Why: when both flags could name the inline cluster, name collisions
with `clusters.yaml` were possible and the previous "silent override"
behavior was dangerous — a user typo could replace a fully-configured
production cluster (TLS, SASL, read-only) with a stripped-down inline
one without the user noticing. Separating the namespaces removes the
collision class entirely: inline names are unguessable, YAML names are
unconstrained, and the two never meet.

Deviation: inline-cluster persistent state (per-(cluster, topic) view
state, etc.) does not survive between runs — the random part of the
name changes each launch. Inline is by definition transient; for
persistent configuration the user puts the cluster in `clusters.yaml`.

### Config-value normalization

Every enum-shaped config value (log level, compression, clipboard
method, SASL mechanism, cluster color) passes through one shared
normalization point that trims whitespace and case-folds against a
canonical allowlist. Downstream parsers receive canonical input only
and reject anything else strictly. The same normalization is reused by
CLI flags so YAML and CLI behave identically.

Why: nothing about case sensitivity or whitespace handling should
depend on where a value happened to enter the system. Having one
allowlist + one matcher means adding an enum is a single-place change
and the answer to "is this value accepted?" never depends on the
caller.

Deviation: severity of an invalid value is per-field, not per-source.
Cosmetic / non-critical fields (color, log level, compression,
clipboard method) warn and fall back to a safe default so a YAML typo
doesn't deny startup. Auth-critical fields (SASL mechanism) hard-fail
the cluster because a wrong value would surface much later as a
confusing handshake error. CLI flags always hard-fail — interactive
input deserves an immediate, explicit error.

## Project commands

- `make build` — build the binary into `dist/`
- `make install` — build and copy the binary to `~/.local/bin`
- `make snapshot` — produce a multi-platform snapshot release via goreleaser
- `make lint` — run formatters and linters via `prek`
- `make test` — run the test suite
- `make race` — run tests with the race detector
- `make cover` — run tests with coverage and print a per-function summary
- `make deps` — refresh `go.mod` and the vendored deps
- `make clean` — remove `dist/` and the coverage profile
