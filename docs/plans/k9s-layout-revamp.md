# k9s-style layout revamp

## Overview

Rework the global TUI chrome and the way every screen renders its body to
match the visual conventions of [k9s](https://k9scli.io/): three-pane header
with cluster info / keybindings / logo, command prompt that appears at the
**top**, framed body with title and breadcrumb baked into the border, a
persistent one-line **flash bar** at the bottom for transient feedback, and
properly aligned tables that never shift when values change length.

Reference (k9s):

```
+-------------------------------------------------+
| ClusterInfo  |  Menu <key> Desc  |   ASCII logo |  <- header (~7 rows, no border)
+-------------------------------------------------+
| :command                                        |  <- prompt, only when active
+-- Topics(prod-eu)[42] -------- breadcrumb > ----+
| NAME      PARTITIONS  REPLICAS  ...             |
| > selected row (full-width inverted background) |
| ...                                             |
+-------------------------------------------------+
| flash: ✓ topic created · 6 partitions            |  <- 1 line, always reserved
+-------------------------------------------------+
```

## Context

- Primary chrome files: `internal/tui/app.go`, `internal/tui/layout/layout.go`
- Theme: `internal/tui/theme/theme.go`
- Reusable components: `internal/tui/components/{table,toasts,confirm,form}.go`
- Screens: `internal/tui/screens/{clusters,topics,messages,produce,groups,logs,configsrc}/`
- Plan: each screen owns `KeyHints()` already — those entries get rendered in
  the header (middle pane) instead of the bottom row.

## Pain points (current state)

1. **Layout doesn't fill the screen.** `app.go:render()` joins parts with
   newlines, body height is `height - 3`, the rest is empty space.
2. **Command prompt is at the bottom**, opposite of k9s convention.
3. **Key hints are a single thin row at the bottom**, hard to scan, easy to
   miss new bindings on screen change.
4. **No frame around the body** — header/body/hints visually run together.
5. **Each screen renders its own counter/header line** (`headerLine()`,
   `counterLine()`), inconsistent and duplicated logic.
6. **Tables shift columns** when a single value is wider than its declared
   width (`components/table.go:padCell` returns content as-is when over). No
   truncation, no proportional flex.
7. **Selection uses `[ ]`/`[✓]`/`> ` prefixes** instead of full-row inverted
   background — visually noisy and out of place for single-select tables.
8. **No flash bar.** Toasts are rendered inline within each screen body
   (`components/toasts`), which pushes the table down. No global feedback
   channel.
9. **Status indicator on clusters screen lies.** The `●` swatch on the
   clusters list is colored by the *user-configured* `Cluster.Color` — looks
   like a connectivity light but isn't. Real status is buried in a text
   column.
10. **Numeric columns are left-aligned**, so partition counts, offsets, and
    lag values are visually unstable.

## Design decisions (locked)

### Chrome layout (top → bottom)

```
Header        (~6–7 rows, no border, three side-by-side panes)
Prompt        (1 row, ONLY when mode == command/search; otherwise absent)
Frame         (rounded border, fills remaining height; title+breadcrumb in top edge)
  └─ Body     (screen content; selection = inverted full-row background)
Flash bar     (1 row, ALWAYS reserved; empty by default)
```

### Header — three panes via `lipgloss.JoinHorizontal`

- **Left (ClusterInfo, ~30% width):** `Context: …`, `Cluster: ● <name> (<rtt>)`,
  `Refresh: auto 5s | manual | paused`, `Mode: read-only|read-write`,
  `Filter: <buffer>|—`. Keys are right-aligned label color, values are
  highlight color.
- **Middle (Menu, ~50% width):** keybindings of the active screen as
  `<key>  Description` items, packed into 2–4 columns based on width. Source:
  `screen.KeyHints()`. Always shows global bindings too (`<:>` Command,
  `<?>` Help, `<q>` Quit).
- **Right (Logo + version, ~20% width):** small ASCII logo (3–4 rows) plus
  `kafka-tui  v0.4.2  (a1b2c3d)`.

### Prompt

- Single line directly **below** the header, **above** the frame, only
  visible when `Mode == ModeCommand || ModeSearch`.
- Same renderer (`layout.CommandLine`) — only the position changes.

### Frame

- New helper `internal/tui/layout/frame.go` exposing
  `Frame(opts FrameOpts, body string) string` with:
  - `lipgloss.RoundedBorder()` around `body`.
  - `Title` (left) and `Breadcrumb` (right) embedded into the **top border
    line** via custom border builder (manual top-row composition).
  - `Focused bool` toggles `frame.border` vs `frame.focus` color.
  - `Width`, `Height` parameters drive the body region.
- Each screen exposes new accessor methods so the host can build the title
  without re-implementing it:
  - `Title() string` — e.g. `"Topics(prod-eu)[42, +3 internal hidden]"`
  - `Breadcrumb() string` — e.g. `"orders.events ⇢ 12 part."`
- Per-screen `headerLine()` / `counterLine()` are deleted.

### Flash bar (new)

- New file `internal/tui/layout/flash.go`:
  - `type FlashLevel int` (`FlashInfo|FlashOK|FlashWarn|FlashErr`).
  - `type Flash struct { Text string; Level FlashLevel; ExpiresAt time.Time }`.
  - `Render(styles, Flash, width int) string` — always returns one line,
    blank if `Text == ""` or expired.
- New `tui.FlashMsg{ Text, Level, TTL }` (Bubble Tea message). Any screen
  emits via `func() tea.Msg { return tui.FlashMsg{...} }`.
- `tui.Model` owns the active flash and a `tea.Tick` to clear it.
- Existing `components/toasts.go` is removed; per-screen toast push sites
  become `FlashMsg` cmds. Sticky-toast tests are migrated to flash assertions.

### Table component

- New `Column` fields:
  - `Flex bool` (this column shares the leftover width).
  - `MinWidth int` (lower bound when shrinking).
  - `Align lipgloss.Position` (already exists, preserved).
- `computeWidths()`:
  - Fixed-width columns take their declared width.
  - Flex columns split the remaining width evenly (respecting `MinWidth`).
  - If total fixed > available, fixed columns shrink proportionally down to
    their `MinWidth`.
- `padCell()` truncates with `…` when content exceeds the column width
  (truncation is byte-rune-safe and respects ANSI styling).
- Column separator becomes a single space (was two).
- **Selection cursor**: instead of `> ` prefix, the entire row is rendered
  with `Styles.Cursor` (already exists: bg=accent, fg=background) — applied
  via `lipgloss.NewStyle().Width(totalWidth).Reverse(true).Render(line)`.
  Multi-select checkboxes only appear when `WithMultiSelect()` is enabled.
- Numeric / right-aligned columns get their alignment from the screen's
  column definitions — no more left-aligned offsets/lag/counts.

### Per-screen changes

#### Clusters (`screens/clusters/`)

- Remove the user-color swatch from the row's first column. Replace with a
  **status dot** colored from `m.statuses[name]`:
  green=`StatusOK`, red=`StatusFailed`, yellow=`StatusChecking`, gray=
  `StatusUnknown`.
- Move the user-configured cluster color to a thin vertical bar `▎` rendered
  inside the `NAME` cell as a prefix (still useful for visual grouping, but
  unmistakably a tag, not a status light).
- Failed rows additionally render with `status.error` foreground.
- `Status` text column shows the latency: `ok  118ms`, `failed  timeout 5s`.
- Rename `Brokers` cell to truncate at column width with `…` (currently
  overflows).

#### Topics (`screens/topics/`)

- Drop columns `Min-ISR` and `Int` from the default set (still resolvable by
  key, but not rendered by default).
- Right-align `Partitions`, `Replicas`, `Messages`, `Size`.
- `Messages` renders as compact units (`1.2k`, `42.7M`, `8.9M`) — already
  partly there; ensure consistent width.
- `Retention` renders as `7d`, `14d`, `30d`, `∞` (compact / forever) instead
  of raw `ms`.
- `Title()` returns `Topics(<cluster>)[<count>[, +<n> internal hidden]]`.
- `Breadcrumb()` returns `<topic> ⇢ <N> part.`.

#### Messages list (`screens/messages/`)

- `Title()` returns `Messages(<topic>)[<window>]` plus a `● LIVE` badge when
  `follow` is on.
- Remove the inline `← NEW` indicator from `headerLine()` (which is being
  deleted) and surface "N new messages" via `FlashMsg` instead.
- Right-align `P`, `OFFSET`, `H`. Drop `H` from the default set if it adds
  no value (decision deferred — prototype both).
- `VALUE` becomes a flex column with truncation; binary placeholder
  `(binary, <size>)`, empty `—`.
- `Breadcrumb()` returns `p<part>·offset <off> ⇢ key=<key>`.

#### Messages detail (`screens/messages/detail.go`)

- Render three nested labelled blocks inside the frame body: **Metadata**,
  **Headers (N)** (omit if empty), **Value · <mode>**. Each is a
  `lipgloss.RoundedBorder` with a left-aligned title in the top edge.
- Metadata is a 2×N grid (`label  value  label  value`) for compactness.
- `Title()` returns `Message · <topic> · p<part> · offset <off>`.
- `Breadcrumb()` returns `<n> of <total> ⇢ key=<key>`.

#### Consumer groups list (`screens/groups/list.go`)

- Add status dot colored from group health:
  - green = Stable && totalLag == 0
  - yellow = Stable && lag > threshold  ||  Rebalancing
  - red = Dead || (Empty && lag > 0)
  - gray = Unknown
- `STATE` cell text colored to match the dot.
- Right-align `MEMBERS`, `TOTAL LAG`. `TOTAL LAG` formatted compactly
  (`8 421`, `1.2M`, `—`).
- `COORDINATOR` shows short broker name only.
- `Title()`: `Consumer Groups(<cluster>)[<count>]`.
- `Breadcrumb()`: `<group> ⇢ <state> · lag <total>`.

#### Other screens (Produce, Logs, Topic Configs, Config Sources)

- Adopt `Title()`/`Breadcrumb()` and the frame; no other functional change.
- Produce form continues to render its own dialog body inside the frame.

### Theme palette — k9s-aligned naming

`internal/tui/theme/theme.go` gets a structured `Palette` with these groups
(values map to existing colors where possible — this is mostly renaming and
adding a few new keys):

- `body { fg, bg, logo }`
- `info { label, value }` — header left pane.
- `menu { fg, key, numKey, description }` — header middle pane.
- `frame { border, focus }` — body frame.
- `title { fg, counter, filter, badge }` — frame title.
- `crumbs { fg, active }` — breadcrumb.
- `table.header { fg, bg, sorter }`
- `table.row { fg, bg, cursor, cursorFg }`
- `status { ok, warn, err, info, neutral }` — status dots and severity text.
- `flash { info, ok, warn, err }`

A YAML skin loader is **out of scope** here — only the data structure changes
so the loader can be added later.

## Iterations

Plan ships in four self-contained iterations; each compiles, passes tests,
and is independently reviewable.

### Iteration 1 — Flash bar

- Add `layout/flash.go` and `tui.FlashMsg`.
- Wire flash into `tui.Model.View()` as the always-reserved bottom row.
- Replace `components/toasts.go` usages with `FlashMsg` emissions
  (clusters/topics/messages/groups/produce). Delete `toasts.go` once all
  callsites are migrated.
- Update existing toast tests to assert on `Model.Flash()` accessor.

### Iteration 2 — Frame with title/breadcrumb

- Add `layout/frame.go` with `Frame(opts, body) string` and a custom border
  composer for the top edge.
- Add `Title() string` and `Breadcrumb() string` methods to every screen
  (defaults to empty for screens that don't need them).
- Host (`tui.Model.View()`) calls `Frame(...)` instead of just the body
  string.
- Delete per-screen `headerLine()` / `counterLine()`. Update screen tests to
  compare against the framed output (or assert on the new `Title()` method
  directly to keep golden tests small).

### Iteration 3 — Table flex/truncate + inverted selection

- Extend `components.Column` with `Flex` and `MinWidth`.
- Rewrite `computeWidths` and `padCell` for flex distribution and truncation.
- Switch single-select selection to `Styles.Cursor` reverse-video full-row
  background; multi-select keeps the checkbox prefix behind `WithMultiSelect`.
- Update column definitions in topics / messages / groups for right-aligned
  numerics and flex `VALUE` / `BROKERS`.
- Update table tests for new render output.

### Iteration 4 — Header three panes + prompt-on-top + theme keys

- Rewrite `layout.Header` as a three-pane composition. Add small ASCII logo
  asset.
- Move `layout.CommandLine` rendering from below the body to above the frame.
- Delete the bottom `KeyHints` row (its entries now live in the middle pane).
- Restructure `theme.Palette` per the naming above; refactor every callsite
  to the new keys.
- Update full-app golden tests (`internal/tui/app_test.go` if any) to the
  new layout.

## Out of scope (explicitly)

- YAML skin loader (defer until palette restructure proves itself).
- Optional column tog­glers (`o` to show extra columns) — capture as a
  follow-up if useful after iteration 3.
- Help overlay redesign (`?`) — current modal stays.
- Mouse support.

## Build & test discipline

- After each iteration: `git add` modified files, `make lint`, `make test`.
- No commit unless the user asks.
- Bubble Tea tests for screens use the synchronous cmd-pump helper already
  present (`drive(...)` in topics_test.go) — flash assertions go through it.
