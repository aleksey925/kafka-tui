# Messages screen seek redesign

## Overview

Rework the seek/filter UX of the messages screen
(`internal/tui/screens/messages/`) to cleanly separate three orthogonal
axes that are currently mashed together under ad-hoc hotkeys:

1. **Seek** — where in the topic to read from (`latest`, `earliest`,
   `from/to offset`, `from/to timestamp`, `live`).
2. **Partitions** — which partitions are in scope.
3. **Smart filter** *(stub for now, full design later)* — server-side
   predicate over `record.*` fields, applied during a scan of the topic.

Live tail stops being a parallel toggle and becomes one of the seek
modes. The `g`-prefix stops carrying data operations and is reserved
for cursor motion (`gg`, `G`) only. The `/` text search keeps its
current display-only semantics.

### Why redo this now

The current screen has every primitive in place but presents a confused
model: `f` toggles live independently of seek; `g o/t/p` would change
what is fetched while `gg/G` change cursor — same prefix, two
semantics; the only way to alter partitions is the placeholder `g p`;
there is no UX for "view *up to* offset/timestamp X". When the smart
filter feature lands later, slotting it into the current ad-hoc layout
would compound the confusion. This redesign does the structural
cleanup once so smart filter has an obvious home and so each axis
has its own predictable hotkey.

### Reference model

The mental model is borrowed from kafbat-ui and similar mature Kafka
UIs, where partition selection, seek/range, and predicate filtering
are three independent controls. The user explicitly cited kafbat as
the model during discussion. We keep the terminal-native key-driven
flavour but adopt the same separation of concerns.

## Context

- Primary files:
  - `internal/tui/screens/messages/messages.go` (host model, hotkeys,
    follow logic, filter state)
  - `internal/tui/screens/messages/detail.go` (unaffected, but key
    routing in messages must keep delegating in `ModeDetail`)
  - `internal/kafka/messages.go` (Service interface; `ParseTimestamp`,
    `ParsePartitionFilter` already exist and stay)
  - `internal/kafka/client.go` (will gain three new operations)
- New component:
  - `internal/tui/components/menu.go` — popup list with digit shortcuts
    and arrow/Tab navigation.
- Tests:
  - `internal/tui/screens/messages/messages_test.go`
  - `internal/tui/components/menu_test.go` (new)
  - `internal/kafka/messages_test.go`

## Pain points (current state)

1. **`g`-prefix carries data operations.** `g o` / `g t` / `g p` change
   what is fetched from Kafka. `gg` / `G` move the table cursor. Two
   semantics under one prefix.
2. **`g o/t/p` are placeholders.** `openJumpForm` (messages.go:666) just
   pushes "pending" toasts. The Kafka layer (`ParsePartitionFilter`,
   `ParseTimestamp`, `JumpToOffset`, `JumpToTimestamp`,
   `JumpToPartition`, `loadAt*Cmd`) is fully implemented but unreachable
   from the live UI.
3. **Live tail is orthogonal.** `f` toggles `Follow` independent of
   everything else. There is no clear story for "I just jumped to
   offset X, am I still in live mode?".
4. **No `to offset`, no `to timestamp`, no `earliest`.** The Service
   interface gives forward seek and arbitrary-direction paging, but
   there is no UX for the symmetric "view ending at X" case.
5. **Filter axis is invisible.** `m.filter` is hidden state. Users have
   no way to inspect it, only to overwrite it via the (unimplemented)
   `g p` flow.

## Design decisions (locked)

### Hotkey layout

| Key | Action |
|---|---|
| `s` | open seek popup (single popup, two-stage content: mode list → mode-specific input) |
| `f` | open smart filter (stub — info toast and short description for now) |
| `P` | open partition filter form |
| `/` | text search inside the loaded window (unchanged) |
| `p` | produce (unchanged across the project) |
| `r` | resend (unchanged) |
| `Enter` | open detail (unchanged) |
| `[` / `]` | page step within the current seek window |
| `gg` / `G` | cursor top / bottom (unchanged) |
| `Esc` / `q` | back |
| `:` | command bar (host) |

**Removed:** `f` as live-tail toggle (live becomes seek mode 7), the
entire `g o/t/p` data-op prefix.

#### Why this layout

- **`s` for seek (single key, no chord).** Considered `g s` (vim-prefix
  chord) and `:seek` (command-bar only). Single key wins because seek
  is a frequent operation and chords add muscle-memory tax; the chord
  alternative also re-introduced the `g`-prefix-with-data-ops problem
  we are explicitly removing. `:seek` stays available as a fallback
  via the command bar but is not the primary path.
- **`f` for smart filter, not live toggle.** Live becomes one of seven
  seek modes, so a dedicated key is unnecessary. The user explicitly
  asked not to keep a quick path to live. That frees the most
  filter-shaped letter (`f`) for the heaviest filter-shaped feature
  (smart filter — the daily driver once implemented).
- **`P` (capital) for partitions, not `f p` chord.** We considered
  making `f` a "filter family" prefix (`f p` for partitions, `f k` for
  key-pattern, etc.). Rejected once smart filter entered the picture:
  smart filter collapses key/header/value pattern filters into one
  expression, so the family never grows beyond partitions. With only
  one filter dimension besides smart filter, a single-key `P` is
  cleaner than a chord. Capital `P` was chosen because `p` (lowercase)
  is produce on every screen and stays so.
- **`p` stays produce.** Considered rebinding `p` → `n` on this
  screen ("`n` = create new" matches the topics screen's `n` for "new
  topic"). Rejected: produce keeps the same letter on every screen
  for muscle-memory consistency, and freeing `p` was no longer needed
  once `P` covered partitions. Same reason `r` stays resend.
- **`gg` / `G` kept; `g o/t/p` removed.** `gg` and `G` are pure cursor
  motion (visual operations on already-loaded data). `g o/t/p` was a
  *data-fetch* operation (changes what is loaded from Kafka) hidden
  under the same prefix. Splitting them by semantics keeps the model
  honest: `g`-prefix means "cursor", `s` means "fetch".
- **No quick-toggle for live.** User-confirmed: live is just one of
  the seek modes, accessed via `s` then `7` (or arrow + Enter). Two
  keystrokes for an operation that, in practice, runs for minutes —
  acceptable.
- **`/` unchanged.** Text search over the *currently loaded* window
  is conceptually different from server-side filtering and stays a
  display-only operation. Smart filter complements it: `/` for
  "what's already on screen", smart filter for "what is anywhere in
  the topic".

### Seek popup — single window, two stages

`s` opens one popup. The popup's content swaps between two stages:

**Stage 1 — mode picker:**

```
1. latest
2. earliest
3. from offset
4. to offset
5. from timestamp
6. to timestamp
7. live
```

Navigation: digits `1`-`7` select directly; `↑`/`↓` or `Tab`/`Shift+Tab`
move the cursor; `Enter` confirms. Cursor pre-positions on the active
seek mode if one is set.

**Stage 2 — parameter input** (replaces stage 1 inside the same
popup; same border, same screen real estate, just different body):

| Mode | Input field |
|---|---|
| `latest` | (none — dispatched immediately, popup closes) |
| `earliest` | (none — dispatched immediately, popup closes) |
| `from offset` | one text field, accepts `partition:offset` or just `offset` |
| `to offset` | one text field, accepts `partition:offset` or just `offset` |
| `from timestamp` | one text field, formats per `kafka.ParseTimestamp` |
| `to timestamp` | one text field, formats per `kafka.ParseTimestamp` |
| `live` | (none — dispatched immediately, popup closes) |

`Enter` submits, `Esc` returns to stage 1. From stage 1 `Esc` closes
the popup.

**No `N` field anywhere.** The fetch size is governed by the screen's
existing `m.pageSize`; `[` / `]` continues to step the window.
`latest` / `earliest` mean "go to the tail / head of the topic" — the
amount shown is one page.

#### Why a single popup with two stages (rather than the alternatives)

Three UX flows were considered:

- **A. Two separate modals** — popup menu, then a fresh form opens.
  Two windows for a single user intent.
- **B. Split-pane popup** — modes on the left, parameters live on the
  right, both visible at all times.
- **C. Accordion** — menu items expand to reveal their parameters
  inline.

Picked: **two stages within a single popup window**. Visual continuity
of one window (no second modal opening), parameters appear only for
the chosen mode (less to read), and no new layout primitives are
needed beyond `components.Menu` plus the existing `components.Form`.
Split-pane (B) was rejected as too much new infra for marginal UX
gain on narrow terminals; accordion (C) was rejected because variable
popup height complicates layout and pushes mode-specific knowledge
into the menu component.

#### Why no `N` parameter

Earlier sketches included `N` per mode. User feedback: `latest` and
`earliest` mean "go to end / start of the topic" — count is implicit.
`m.pageSize` already exists as the screen-wide page size knob, and
`[` / `]` already steps the window. Adding `N` to every form would
mean asking the user to fill a number they almost never want to
override, with the value duplicated across modes. So: no `N`.

#### Why `Esc` from stage 2 returns to stage 1

User-confirmed: after typing into a form and realising the chosen mode
was wrong, the user can back-step to the menu and pick another mode
without re-pressing `s`. This is the standard "wizard" navigation
pattern.

#### Why pre-fill from the selected row

When the user is staring at a message and presses `s` → `from offset`,
the answer is almost always "from this row". Pre-filling makes the
common case zero-edit. Empty pre-fill when nothing is selected (e.g.
after a fresh open with no fetch yet) is acceptable — the user types.

**Pre-fill on stage 2:**

- `from offset` / `to offset`: if a row is selected, pre-fill
  `partition:offset` with that row's coordinates; otherwise empty.
- `from timestamp` / `to timestamp`: if a row is selected, pre-fill
  with that row's timestamp formatted as RFC 3339; otherwise empty.

**Validation:** on submit, parse the field; on parse error push a
toast (`"invalid offset: expected partition:offset or offset"` /
`"invalid timestamp: ..."`) and keep the popup open so the user can
fix the input.

### Offset input semantics

- `partition:offset` (e.g. `3:1234`) → seek that one partition to that
  exact offset.
- `offset` alone (e.g. `1234`) → seek **every partition in scope**
  (current `m.filter`, or all partitions when filter is empty) to
  `offset 1234`, clamped to the partition's actual range:
  - if `1234 > latestWatermark` → use `latestWatermark`;
  - if `1234 < earliestWatermark` → use `earliestWatermark`.

This is "fuzzy multi-partition jump" — admittedly imperfect because
offsets are not comparable across partitions, but it is the natural
single-field UX and matches the user's intent ("approximately around
this position").

The clamp requires reading watermarks per partition before
dispatching the fetch; this drives a new `Service.PartitionWatermarks`
method (see below).

#### Why one field, not two

Considered: separate `partition` and `offset` text fields. Rejected
in favour of a single field with the `partition:offset` convention,
matching the broader project preference for "one field per stage 2".
The colon-separated form is familiar from Kafka CLI tooling
(`topic:partition:offset`). The `offset`-only shortcut covers the
case where the user has a number from a log line and does not care
which partition.

#### Why clamp instead of error

User-confirmed reasoning: an offset-only jump is intentionally fuzzy.
A user who wants strict semantics writes `partition:offset`. For the
loose form, hard errors when the offset is out of range for some
partition would feel pedantic — the user clearly wants "around here";
clamping to the watermark gives them the closest reasonable position.
Trade-off accepted: results across partitions may not align in
calendar time, but that is inherent to multi-partition reads.

### Partition filter form (`P`)

- One text field accepting the syntax already supported by
  `kafka.ParsePartitionFilter`: `0-4,7,10-12`, empty = all partitions.
- Pre-fills with the current `m.filter` rendered back into the same
  syntax.
- Submit → `m.filter` is replaced and a fresh fetch is dispatched in
  the current seek mode.
- Validation toast on parse error, same pattern as offset/timestamp.

### Smart filter stub (`f`)

- Pressing `f` opens a small modal (Confirm-style or Menu-style)
  showing a static description:

  > **Smart filter — coming soon.**
  >
  > Will scan the entire topic (within current seek + partition
  > scope) applying a predicate over `record.key`, `record.value`,
  > `record.headers`, `record.partition`, `record.offset`,
  > `record.timestamp`. Boolean operators and string methods are
  > supported. Results stream into the table as matches are found.

- No keybinding to do anything else; `Esc` closes.
- This is purely a stub so users discover the feature exists. The real
  implementation is a separate plan (expression engine choice,
  `Service.Scan` streaming method, multi-line editor, history,
  cancellation, progress indicator).

#### Captured intent for the future implementation

The stub exists because the full feature is large and out of scope
here, but the design intent agreed during discussion needs to be
preserved so the future plan inherits it intact:

- **Why this feature.** Users want to find messages anywhere in the
  topic by content, not just inside the loaded window. The text `/`
  search only narrows the visible page; smart filter actually
  *retrieves* matching records from the topic.
- **Why expression-based, not per-axis hotkeys.** A single expression
  language collapses every "filter by key prefix", "filter by header
  X", "filter by value field Y" use case into one feature. That is
  why the redesign does **not** introduce dedicated hotkeys for those
  axes — they are subsumed by smart filter once it lands.
- **Record model in the expression.** `record.key` and `record.value`
  exposed as both raw bytes and (when applicable) parsed JSON, e.g.
  `record.value.userId == "123"`. `record.headers` as
  `map[string]string`. `record.partition`, `record.offset`,
  `record.timestamp` as primitives.
- **Expression engine candidate.** `expr-lang/expr` (Go-native,
  active, JS-flavoured syntax with method calls). CEL was considered
  (more strict and capable but heavier ecosystem fit) and Lua
  (`gopher-lua`, less intuitive syntax for this audience). The final
  pick belongs to the smart-filter plan, but `expr-lang/expr` is the
  current frontrunner and the stub copy is written assuming that
  flavour.
- **Execution model.** Server-side scan: consume from Kafka within
  the current seek + partition scope, evaluate the predicate per
  record, stream matches into the table. Hard limits on scan size
  and elapsed time (e.g. ≤ 100k records or ≤ 60s, whichever first)
  to keep behaviour predictable on huge topics. Cancellable. Progress
  indicator surfaces "scanned X / matched Y".
- **UI shape.** Multi-line expression editor (needs a textarea-style
  field which `components.Form` does not yet have — that is part of
  the future work, not this plan). History of recent expressions,
  reusing the existing `internal/tui/filterhistory` pattern from `/`.
- **Why `f` and not another key.** `f` is the most natural "filter"
  letter; freed by removing the live-tail toggle. Smart filter is the
  daily-driver filter once implemented, so it earns the prime letter.
- **Header line.** When smart filter is active, the chrome header
  reflects it: `smart filter: record.key.startsWith("u-")`.

### Header / chrome — visible state

A new line in the screen body header surfaces the active configuration
so users can see what they are looking at without opening any modals:

```
seek: latest  •  partitions: 0-4,7  •  smart filter: —
```

Updated on every state change. Compact, single line, truncated on
narrow terminals.

#### Why a visible status line

Originally partitions and seek mode were hidden state — `m.filter` was
not surfaced anywhere, and "what seek mode am I in" had no answer at
all. With three axes now actively configurable, the user must be able
to see the current configuration without opening any modal. This
matches kafbat-ui's persistent display of partition selector + seek
mode at the top of its messages page (the user explicitly cited it).
A single compact line is the minimal terminal-friendly equivalent.

### `Mode` enum

Add two sub-modes:

```go
const (
    ModeList Mode = iota
    ModeDetail
    ModeSeek      // popup is open (stage 1 or 2 internally)
    ModePartitions
    // ModeSmartFilter is reserved for later; the stub re-uses ModeList
    // by pushing a transient toast/confirm.
)
```

`HasOverlay()` returns true for both new modes; `SearchAvailable()`
returns false; the host blocks `:` and `/` while either is active,
mirroring the existing `ModeDetail` rules.

### Service interface — new operations

```go
// FetchEarliest reads pageSize messages forward starting at the
// earliest available offset of each requested partition.
FetchEarliest(ctx context.Context, topic string, n int, partitions []int32) ([]kafka.Message, error)

// PartitionWatermarks returns [earliest, latest] offsets per
// partition. Used to clamp single-number offset jumps and to compute
// page-step bounds.
PartitionWatermarks(ctx context.Context, topic string, partitions []int32) (map[int32]Watermarks, error)

// OffsetsForTimestamp returns, per partition, the offset of the first
// message with timestamp >= ts. Used for `to timestamp` seek (combined
// with FetchEarlier).
OffsetsForTimestamp(ctx context.Context, topic string, ts time.Time, partitions []int32) (map[int32]int64, error)
```

```go
type Watermarks struct {
    Earliest int64
    Latest   int64
}
```

Implementation:

- `FetchEarliest` — `kgo.Client` with manual offsets set to the
  earliest watermark per partition (use `kadm.Client.ListStartOffsets`
  to read them, then issue a normal forward fetch loop).
- `PartitionWatermarks` — `kadm.Client.ListStartOffsets` +
  `kadm.Client.ListEndOffsets` joined per partition.
- `OffsetsForTimestamp` — `kadm.Client.ListOffsetsAfterMilli`.

Tests use `kfake` as everywhere else.

### `to offset` / `to timestamp` realisation

No new fetch primitive needed:

- `to offset partition:N` →
  `FetchEarlier(topic, baseline={partition: N+1}, pageSize, partitions=[partition])`.
- `to offset N` (no partition) →
  for each partition `p` in scope: `clamp(N, watermarks[p])`, build
  `baseline = {p: clamped+1 for each p}`, then
  `FetchEarlier(topic, baseline, pageSize, partitions=scope)`.
- `to timestamp T` →
  `OffsetsForTimestamp(topic, T, partitions)` → use returned offsets as
  `baseline + 1` for `FetchEarlier`.

The single existing `FetchEarlier` covers all three "go-backward" seek
modes; the new methods only assemble the right baseline.

### `[` / `]` behaviour by seek mode

| Mode | `[` (earlier) | `]` (later) |
|---|---|---|
| `latest` | normal step back | already at tail — toast "end of topic" |
| `earliest` | already at head — toast "start of topic" | normal step forward |
| `from offset` | normal step back | normal step forward |
| `to offset` | normal step back | hit boundary → toast "end of seek window" |
| `from timestamp` | normal step back | normal step forward |
| `to timestamp` | normal step back | hit boundary → toast "end of seek window" |
| `live` | disabled — toast "paused live to step" + flip mode to `latest` | same |

Boundary detection in `to-*` modes uses the captured target offsets as
a hard right edge.

> **Note.** This table lists chosen defaults rather than user-confirmed
> answers — the user did not weigh in on `[`/`]` per mode during the
> discussion. The defaults follow the principle "`to-*` is a hard
> right edge; `live` cannot coexist with stepping". If the behaviour
> feels wrong in practice, treat it as a follow-up tweak rather than a
> contract.

### Persistence of seek mode and partition filter

Seek configuration and partition filter survive restarts so users
return to the same view they left. Stored in the existing
`internal/state/` SQLite layer (the same place the rest of the app
already keeps cross-session state).

**Key.** `(cluster_name, topic)`. Per-cluster, per-topic. Switching
cluster or topic loads its own remembered configuration; identical
topic names across clusters do not collide.

**What is stored.**

- Seek mode (one of: `latest`, `earliest`, `from offset`, `to offset`,
  `from timestamp`, `to timestamp`) with its parameters:
  - offset modes: explicit-partition flag + offset value (and partition
    when explicit);
  - timestamp modes: the timestamp.
- Partition filter — the same syntax `kafka.ParsePartitionFilter`
  accepts (`0-4,7,10-12`), empty meaning "all partitions".

**What is NOT stored.** `live` mode. Live tail is an active background
operation, not a "view position", and starting straight into it on
launch would consume from the broker without explicit user intent.
If the last selected mode was `live`, the persisted record holds
whatever seek mode preceded it (or `latest` if there was none).

**Write trigger.** On every successful submit of the seek popup
(stage 2 → dispatch) and on every successful submit of the partition
filter form. Failed parses do not write.

**Read trigger.** On screen open (`messages.New` or whenever a topic
is entered). The screen restores the stored mode + parameters +
partition filter and dispatches the corresponding fetch as if the user
had just submitted them.

**Stale data — silent clamp.** Stored offsets/timestamps may be out
of range by the time the user returns (retention, compaction, broker
state changes). The screen falls back to the nearest valid position
without surfacing an error toast:

- offset below `earliest` watermark → clamp to `earliest`;
- offset above `latest` watermark → clamp to `latest`;
- timestamp before earliest record → behaves as `earliest`;
- timestamp after latest record → behaves as `latest`;
- partition in stored filter that no longer exists → drop it from
  the set; if the resulting set is empty, treat as "all partitions".

The header line still shows the *effective* configuration after the
clamp, so the user sees what is actually loaded. No toast — restart
should be invisible when the position is still valid and quietly
self-correcting when it isn't.

**Schema.** New SQLite table, e.g.:

```sql
CREATE TABLE messages_view_state (
    cluster_name TEXT NOT NULL,
    topic        TEXT NOT NULL,
    seek_mode    TEXT NOT NULL,
    seek_params  TEXT NOT NULL,  -- JSON-encoded mode-specific params
    partitions   TEXT NOT NULL,  -- raw filter syntax, "" = all
    updated_at   INTEGER NOT NULL,
    PRIMARY KEY (cluster_name, topic)
);
```

A small repository in `internal/state/` exposes
`LoadMessagesView(cluster, topic)` and
`SaveMessagesView(cluster, topic, view)`. The screen receives the
repository through its `Options` (alongside `Service`, `Now`, etc.)
so tests can inject an in-memory or temp-file implementation.

#### Why per (cluster, topic)

User-confirmed: a topic name like `events` exists in many clusters and
means different things in each. Keying by topic alone would collide;
keying per-cluster keeps the configurations isolated.

#### Why exclude `live` from persistence

User-confirmed: re-entering live on launch would start a long-running
background read without the user explicitly asking for it on this
session. Easier to opt back in (`s` → `7`) than to opt out of an
unintended live tail. The fallback to the previous seek mode also
gives users a sensible "last known position" rather than always
booting to `latest`.

#### Why silent clamp and not a toast

User-confirmed: stale-on-restore is the common case for
timestamp/offset modes against topics with retention. A toast on every
launch would be noise. The header line already surfaces the effective
state, so the user can notice the shift if it matters; otherwise the
quiet correction is preferable.

### Action types

`Action` (host-facing) gains nothing; the host already routes the
existing `Back` / `Produce` cases. New popup state stays internal to
the screen.

## Out of scope (this plan)

- Full smart-filter implementation (expression language, streaming
  scan, editor) — separate plan.
- Topic-screen hotkey unification (`p` vs `n` consistency) — flagged
  during discussion, deferred.
- Display filters by key/header/value pattern — collapsed into smart
  filter and deferred with it.
- (none — seek persistence is in scope, see below.)

## Rejected alternatives (quick index)

Captured for future-self / new contributors who wonder why the design
isn't shaped differently:

| Considered | Rejected because |
|---|---|
| Rebind `p` → `n` on messages screen for "new message" symmetry with topics screen's "new topic" | Produce keeps the same letter on every screen; freeing `p` was unnecessary once `P` (capital) covered partitions. |
| `g s` chord for seek (vim-style prefix) | Re-introduces the `g`-prefix-with-data-ops mixing we are explicitly removing. |
| `:seek` as the only entry (no dedicated key) | Seek is frequent; pure command-bar access adds friction. `:seek` stays available as a fallback but is not the primary path. |
| `f` as "filter family" prefix (`f p` partitions, `f k` key, `f h` header, …) | Smart filter collapses key/header/value pattern filtering into one expression. The filter family never grows beyond partitions, so a chord prefix is overkill. |
| Quick-toggle key for live tail (parallel to seek) | User-rejected. Live is a seek mode; two keystrokes via `s 7` are acceptable for an operation that runs for minutes. |
| Two separate modals (menu, then form) | Lost visual continuity — two windows appear for one user intent. |
| Split-pane popup (modes left, params right) | Too much new layout work; bad on narrow terminals. |
| Accordion menu (param row expands inline under selected mode) | Variable popup height complicates layout; pushes mode-specific knowledge into the menu component. |
| `N` parameter per mode | `m.pageSize` already governs window size; `[`/`]` already steps. Adding `N` to every form duplicates the knob the user almost never overrides. |
| Two fields for offset (separate `partition` and `offset`) | One field with `partition:offset` syntax matches the existing "single field per stage 2" rule and is familiar from Kafka CLI tooling. |
| Hard error on out-of-range offset for "fuzzy" form | The single-number offset form is intentionally fuzzy; clamp to watermarks gives the closest reasonable position rather than refusing. The strict form is `partition:offset`. |
| Display filters as a separate dimension (key match, header match, etc. with their own hotkeys) | Subsumed by smart filter — same expressions cover all of them. |
| Always-visible kafbat-style partition selector (separate UI control) | Compact single-line header surfacing all three axes covers the same need without a dedicated layout primitive. |

## Implementation roadmap

Each step lands as an independently buildable, testable, lintable
chunk. Order matters — earlier steps unblock later ones.

### Step 1 — extend `Service` (Kafka layer)

- Add `Watermarks` struct.
- Add `PartitionWatermarks`, `FetchEarliest`, `OffsetsForTimestamp` to
  the `Service` interface in `internal/kafka/client.go` (or wherever
  it lives) and implement them on `*kafka.Client`.
- Update the messages-screen `Service` interface to embed the new
  methods.
- Update the in-test fake (`fakeService` in `messages_test.go`) and
  the produce-screen fake if needed.
- New unit tests against `kfake` for each operation.

### Step 2 — `components.Menu`

- New file `internal/tui/components/menu.go`.
- Type with: `Items []MenuItem`, current cursor, configurable styles.
- API: `New(items, opts...) *Menu`, `Update(msg)`, `View()`,
  `Selected() (int, MenuItem, bool)`, `SetCursor(i)`,
  `Focused() int`.
- Hotkeys: digit `1`-`9` jumps directly to the n-th item; `↑`/`↓` or
  `j`/`k` or `Tab`/`Shift+Tab` move; `Enter` confirms; `Esc` cancels
  (consumed by caller).
- Styling pulled from the theme (`Styles.MenuTitle`,
  `Styles.MenuItem`, `Styles.MenuItemActive`, `Styles.MenuDigit`).
- Tests: navigation, digit shortcuts, cursor wrap-around, render.

### Step 3 — seek popup, modes without offset (stages of one popup)

Implements the seek menu and the four parameter-less / single-text
modes: `latest`, `earliest`, `from timestamp`, `to timestamp`, `live`.

- Add `ModeSeek` and an internal stage enum (`stageMenu`,
  `stageInput`).
- `s` opens the popup at `stageMenu` with cursor on the active mode.
- Selecting a parameter-less mode dispatches the corresponding
  `Service` call and closes the popup.
- Selecting a timestamp mode advances to `stageInput` with one text
  field, pre-filled from the selected row if any.
- `Esc` from `stageInput` returns to `stageMenu`; `Esc` from
  `stageMenu` closes.
- Update `KeyHints()` for `ModeSeek`.
- Tests: open via `s`, navigate menu with digits and arrows, submit
  parameter-less modes, submit timestamp modes (valid + invalid
  input), back-navigation between stages.

### Step 4 — offset modes with watermark clamp

Implements `from offset` and `to offset`.

- Parser for the input field: accepts `partition:offset` or `offset`;
  emits a struct describing whether the partition was specified.
- On submit:
  - explicit partition → direct `FetchAtOffset` (forward) or
    `FetchEarlier` baseline (backward).
  - implicit (number only) → call `PartitionWatermarks`, clamp per
    partition, then dispatch.
- Toast on parse error.
- Tests: explicit-partition happy path, implicit-partition clamp
  (above latest, below earliest, in-range), parse error.

### Step 5 — partition filter form (`P`)

- New `ModePartitions`.
- One-field form pre-filled with current `m.filter` rendered back to
  string syntax.
- Submit → parse via `kafka.ParsePartitionFilter`, replace
  `m.filter`, dispatch a fresh fetch in the current seek mode.
- Tests: open, edit, submit, parse-error toast.

### Step 6 — header line for active state

- Render `seek: ...  •  partitions: ...  •  smart filter: ...` near
  the top of the body, above the table.
- Re-render on every state change.
- Truncates on narrow widths.
- Tests: snapshot of the header line in several states.

### Step 7 — smart filter stub (`f`)

- New tiny modal (re-uses `components.Confirm` or `components.Menu`
  with a single "OK" item) showing the description block from the
  Design section.
- `Esc` closes; nothing else does anything.
- Wire `f` to open it.
- Test: pressing `f` shows the modal; `Esc` closes.

### Step 8 — chrome cleanup and removal of old hotkeys

- Drop `f` as live toggle (live now lives in seek menu).
- Drop the entire `gPrimed` / `handleGPrefix` flow except for the
  `gg` cursor-top behaviour. Move `gg` directly to the table key path
  (or keep a tiny dedicated prefix only for `gg`).
- Update `KeyHints()` for `ModeList` to reflect the new bindings.
- Remove `openJumpForm` and the `JumpToOffset` / `JumpToTimestamp` /
  `JumpToPartition` exported helpers if no longer needed by tests
  (the new seek-popup tests should drive the same flows through the
  popup).
- Update `messages_test.go` to remove obsolete `TestJumpTo*` tests
  and replace them with seek-popup-driven tests.

### Step 9 — `[` / `]` boundary handling per mode

- Track current seek mode in screen state (`m.seekMode` enum).
- Adjust `loadEarlier` / `loadLater` to short-circuit at the
  appropriate boundary and surface a toast.
- `[` / `]` in `live` → toast "paused live to step" and flip mode to
  `latest` before stepping.
- Tests: each mode's boundary behaviour.

### Step 10 — persistence of seek mode and partition filter

- New table `messages_view_state` via a migration in
  `internal/state/` (follow the existing migrations pattern in that
  package; do not hand-edit older migrations).
- New repository type with `LoadMessagesView(cluster, topic)` /
  `SaveMessagesView(cluster, topic, view)`. The view struct encodes
  seek mode + params (JSON in the `seek_params` column) + partition
  filter string.
- Wire the repository into `messages.Options` and through whatever
  bootstrap currently constructs the screen. Tests inject a temp-file
  SQLite via `t.TempDir()`, same as other state tests.
- On screen open: load the record; if absent, default to `latest` +
  no partition filter. If present, apply silent-clamp logic against
  fresh watermarks (and topic metadata for partitions) before
  dispatching the initial fetch.
- On every successful seek submit and partition-filter submit: write
  through to the repository. `live` mode skips the write so the
  previously stored seek survives.
- Header line reflects the effective (post-clamp) configuration.
- Tests:
  - round-trip save/load for each seek mode;
  - clamp behaviour: offset below earliest, offset above latest,
    timestamp out of range on both sides, dropped/missing partitions;
  - `live` does not overwrite the stored record;
  - cluster + topic isolation (same topic name in two clusters keeps
    independent state).

## Testing strategy

- All Kafka-touching behaviour goes through `kfake` (no
  testcontainers). New Service methods get a dedicated test file or a
  new section in `internal/kafka/messages_test.go`.
- Screen tests use the existing `drive(t, m, cmd)` queue-pump helper.
  Time injection: `Options.Now` for the screen, fixed value in tests.
- Component tests (`Menu`) use golden-style assertions on `View()`.

## Open follow-ups (not blocking this plan)

- Smart filter full design — separate plan once we settle on the
  expression engine and scanning model.
- Surface the active seek mode in the page-level `:config sources`
  view.
