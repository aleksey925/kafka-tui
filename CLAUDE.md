# CLAUDE.md — kafka-tui

Project-specific guidance for Claude Code (and humans). Keep this file in
sync when conventions change.

## Layout

```
cmd/kafka-tui/        # main(); wires CLI → config → TUI program
internal/cli/         # pflag parsing + cross-flag validation
internal/config/      # YAML loader, hierarchical merge, placeholders, watcher
internal/vault/       # KV v2 client with token resolution chain
internal/logging/     # rotating file logger + ResolveFilePath + OpenInPager
internal/clipboard/   # OSC 52 + native (pbcopy / xclip / wl-copy)
internal/state/       # SQLite (modernc.org/sqlite, no CGO) + migrations
internal/kafka/       # franz-go wrapper: Dial, topics, messages, produce, groups
internal/version/     # version.Format(version, commit) → "v1.2.3 (a1b2c3d)"
internal/tui/         # Bubble Tea v2 root model + router + chrome
  layout/             # Header, Status, CommandLine, KeyHints renderers
  theme/              # Color palette + Styles (cluster.color tinting)
  components/         # Table, Form, Confirm, Toasts, Help (reusable)
  screens/            # one subdirectory per screen
    clusters/         # cluster picker
    topics/           # topics list + create/clone/delete + configs
    messages/         # messages list + detail (json/raw/hex)
    produce/          # produce form (history, resend, prefill)
    groups/           # consumer groups list + detail + reset flow
    logs/             # in-app log viewer
    configsrc/        # `:config sources` provenance view
docs/plans/           # plan files (active and completed/)
examples/             # annotated config.yaml + clusters.yaml samples
```

`internal/` is the boundary — nothing under it is part of the public API.

## Bubble Tea conventions

- Bubble Tea v2 (charm.land/bubbletea/v2). Models are pointer receivers and
  expose `Init() / Update() / View()` plus testing helpers (`Render()`,
  `Mode()`, `Action()`).
- Messages flow top-down: `cmd/kafka-tui/main.go` constructs `tui.Model`,
  which owns the `Router` and global chrome. Screens are nested models the
  router dispatches to.
- Screens never call Kafka directly. Each screen package defines a `Service`
  interface with the operations it needs; production wires
  `*kafka.Client`, tests wire a fake. This is what makes screens unit-testable
  without brokers.
- Cross-screen navigation uses an `Action` struct (e.g. `topics.Action{Messages: "foo"}`)
  populated during `Update`. The host reads it after each tick and calls
  `Model.ConsumeAction()` to clear. Avoid imperative routing from inside a
  screen — let the host route.
- Long-running work returns `tea.Cmd`. Tests run the cmd queue synchronously
  via a `drive(t, m, cmd)` helper (see `internal/tui/screens/topics/topics_test.go`).
- Time is injected (`Options.Now`) — never call `time.Now` inline. Same for
  the styles palette (`Options.Styles` defaults to `theme.DefaultStyles()`).

## Adding a new screen

1. Create `internal/tui/screens/<name>/<name>.go` with a `Model`, `Options`,
   `New(Options) *Model`, and an `Action` describing host intents.
2. Define a `Service` interface for any external dependency. Pass it via
   `Options`. Do not import `internal/kafka` directly into the screen body.
3. Register a `ScreenID` in `internal/tui/router.go` and extend
   `ParseCommand` if the screen is reachable via the `:` command bar.
4. Wire the screen into the host (`internal/tui/app.go` or whichever
   bootstrap orchestrates routing) — push it onto the router stack and
   forward `Update`/`View`.
5. Inject `Now`, `Styles`, and a `RefreshInterval` (when applicable) so
   tests can drive the screen deterministically.
6. If the screen has destructive actions, gate them on `Options.ReadOnly`
   and surface a toast with the `read-only` reason.

## Testing patterns

- `pkg_test` package per file (`package topics_test`), imports the screen
  using its public surface only.
- Drive `tea.Cmd`s synchronously with a queue-pumping helper. See
  `drive()` in `internal/tui/screens/topics/topics_test.go` for the
  reference shape.
- Kafka-touching tests use the in-process `kfake` broker
  (`github.com/twmb/franz-go/pkg/kfake`) — never testcontainers, since
  Docker is not always available in the dev environment.
- Vault tests use `httptest.NewServer` to mock the KV v2 API.
- Persistent state tests open SQLite against a tmp file (`t.TempDir()`).
- TUI components have golden-style output assertions on `m.Render()`.
- Comparing collections: assert against the entire slice/map at once
  rather than indexing element-by-element.

## Configuration model

- The `config` package loads two layers (global `~/.kafka-tui/`, project
  `<repo>/.kafka-tui/`) and merges them via rules in `merge.go`.
  Scalars override, lists replace, clusters merge by name.
- Placeholders (`${env:...}`, `${file:...}`, `${vault:...}`) resolve in
  two phases — env+file first, then vault — driven by `placeholders.go`.
  Nesting is rejected.
- Field-origin metadata is recorded during merge and surfaced by the
  `:config sources` screen.
- `--config` (file or directory) disables hierarchy lookup for the matched
  files only.
- The fsnotify-based watcher (`watcher.go`) debounces 500 ms and re-emits a
  full snapshot — never partial updates.

## Style and conventions

- All code, comments, and commit messages in English.
- Comments only for non-obvious WHY. No comment on every line, no
  docstrings on tests.
- Strict typing — Go's type system, no `interface{}` outside boundaries.
- Avoid backwards-compat shims (`_var` rename, re-exports, `// removed`
  notes). If something is unused, delete it.
- Imports always at the top of the file; no inline imports inside funcs.
- Use the existing theme palette for colored output; do not hardcode ANSI
  sequences.

## Build & test

```bash
make deps        # go mod tidy && go mod vendor
make build       # ./dist/kafka-tui with version+commit ldflags
make test        # full suite, 3m timeout
make race        # -race
make cover       # coverage report; ≥80% expected on business-logic packages
make lint        # prek-managed pre-commit hooks (golangci-lint, gofmt)
```

Always `git add` files before `make lint` — hooks check the staged tree.

## What NOT to add

- No new docs files unless the task explicitly asks for them.
- No CGO dependencies (sqlite is `modernc.org/sqlite` for this reason).
- No testcontainers-based tests; use `kfake` / `httptest`.
- No direct calls to `time.Now()` inside screens or services — inject a
  clock so tests stay deterministic.
- No imperative cross-screen navigation; populate an `Action` and let the
  host route.
