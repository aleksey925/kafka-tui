# kafka-tui

A terminal UI client for Apache Kafka. Browse and manage topics, messages, and
consumer groups with k9s-style navigation, hierarchical YAML configuration,
Vault-backed secrets, and a read-only protection mode for production clusters.

Built in Go on top of [Bubble Tea v2][bubbletea] and [franz-go][franz].

[bubbletea]: https://github.com/charmbracelet/bubbletea
[franz]: https://github.com/twmb/franz-go

## Features

- Cluster picker with colored status (`✓ ok` / `✗ failed` / `? unknown` /
  `◐ checking…`) and per-cluster `[RO]` read-only enforcement.
- Topic list with configurable columns, fuzzy search, sorting, internal-topic
  toggle, and create / clone / delete flows.
- Message browser with follow-mode, partition filters, jump-by-offset and
  jump-by-timestamp (`g o`, `g t`, `g p`), JSON / raw / hex value views, and
  copy / save / open-in-`$EDITOR` actions.
- Producer form with compression dropdown, dynamic headers, history
  (`Ctrl+P/N`), prefill-from-last, and resend-from-message.
- Consumer groups list with lazy lag aggregation, per-topic filtering, detail
  view, and a 4-step (or one-shot `Shift+R` express) reset offsets flow.
- Hierarchical config: global (`~/.kafka-tui/`) and project (`.kafka-tui/`
  found by walking up from `cwd`, like `.git`) layers, plus an explicit
  `--config` override.
- Placeholders in any string field: `${env:VAR}`, `${env:VAR:-default}`,
  `${file:/path}`, `${vault:path#key}`, `${vault:path}`.
- Vault KV v2 integration with token resolution
  (`config → $VAULT_TOKEN → ~/.vault-token`).
- File watcher with 500 ms debounce for live config reloads.
- Clipboard with OSC 52 (SSH-friendly), native (`pbcopy` / `xclip` /
  `wl-copy`), or both in parallel.
- Logs viewer in the TUI (`:logs`) with follow-mode and colored levels;
  rotating log files via `lumberjack`-style sizing.

## Installation

Requires Go 1.25+.

```bash
git clone https://github.com/aleksey925/kafka-tui.git
cd kafka-tui
make install              # builds and copies the binary to ~/.local/bin
```

Or build directly from source:

```bash
make build                # produces ./dist/kafka-tui
go install ./cmd/kafka-tui
```

A snapshot release with all platform binaries can be produced via
`make snapshot` (requires [`goreleaser`][goreleaser]).

[goreleaser]: https://goreleaser.com

## Quick start

The fastest way in is to provide brokers inline:

```bash
kafka-tui --brokers localhost:9092
```

For a more realistic setup, drop a `clusters.yaml` into `~/.kafka-tui/`:

```yaml
# ~/.kafka-tui/clusters.yaml
clusters:
  - name: local
    brokers: ["localhost:9092"]
    color: green

  - name: prod
    brokers: ["prod-1:9092", "prod-2:9092"]
    color: red
    read_only: true
    sasl:
      mechanism: SCRAM-SHA-512
      username: ${env:KAFKA_USER}
      password: ${vault:secret/kafka/prod#password}
    tls:
      ca_file: /etc/ssl/certs/kafka-ca.pem
```

Then launch with no flags — the cluster picker opens; `Enter` connects.

```bash
kafka-tui
```

## CLI flags

| Flag                                                       | Purpose                                                 |
| ---------------------------------------------------------- | ------------------------------------------------------- |
| `--version`                                                | Print version and exit.                                 |
| `--logs`                                                   | Open the log file in `$PAGER` and exit.                 |
| `--logs-dir`                                               | Print the log directory and exit.                       |
| `--config <path>`                                          | Use a single file/directory; disables hierarchy lookup. |
| `--cluster <name>`                                         | Pre-select a cluster from `clusters.yaml`.              |
| `--brokers a:9092,b:9092`                                  | Define an inline cluster (skips the picker).            |
| `--color red\|yellow\|green\|gray\|white`                  | Color for the inline cluster.                           |
| `--read-only`                                              | Mark the inline cluster as read-only.                   |
| `--tls`                                                    | Enable TLS on the inline cluster.                       |
| `--tls-ca`, `--tls-cert`, `--tls-key`, `--tls-skip-verify` | TLS sub-options (require `--tls`).                      |
| `--sasl-mechanism`, `--sasl-username`, `--sasl-password`   | SASL options (require `--brokers`).                     |

When `--brokers` is given and a cluster of the same name also exists in
`clusters.yaml`, the CLI cluster wins for the session and a warning toast
notes the collision.

## Configuration

Two files are read from each layer:

- `config.yaml` — UI behavior (logging, refresh intervals, columns,
  produce / clipboard / vault settings).
- `clusters.yaml` — cluster definitions (brokers, color, `read_only`, SASL,
  TLS).

Layers, lowest priority first:

1. **Global** — `~/.kafka-tui/{config,clusters}.yaml`
2. **Project** — `<repo>/.kafka-tui/{config,clusters}.yaml`, located by
   walking up from `cwd` (same lookup rule as `.git`).

Merge rules:

- Scalars in the project layer override the global layer.
- Lists are replaced wholesale (e.g. `topics.columns`).
- Cluster lists are merged by `name`; per-field overrides apply within a
  matching entry.

`:config sources` opens an in-app screen showing which file each field came
from.

### Placeholders

Any string field may contain one or more placeholders. Resolution runs in
two phases: `env` and `file` first, then `vault`.

| Placeholder           | Description                              |
| --------------------- | ---------------------------------------- |
| `${env:VAR}`          | Read environment variable `VAR`.         |
| `${env:VAR:-default}` | Same, with a default when unset/empty.   |
| `${file:/abs/path}`   | Read the file's full contents.           |
| `${vault:path}`       | Read the whole KV v2 secret as a map.    |
| `${vault:path#key}`   | Read a single field from a KV v2 secret. |

Nested placeholders are rejected. A resolution error at startup aborts
launch (lazy exclusion for inactive clusters is deferred work).

### Examples

The `examples/` directory contains annotated `config.yaml` and `clusters.yaml`
samples covering placeholders, TLS, SASL, and read-only flags.

## Hotkeys

A non-exhaustive cheat sheet — `?` opens the in-app help overlay with the
full set, including screen-specific bindings.

| Key                    | Action                                                                                               |
| ---------------------- | ---------------------------------------------------------------------------------------------------- |
| `:`                    | Open command bar (`:topics`, `:groups`, `:clusters`, `:cluster <name>`, `:logs`, `:config sources`). |
| `/`                    | Fuzzy search on the active screen.                                                                   |
| `?`                    | Toggle help overlay.                                                                                 |
| `Ctrl+R`               | Toggle auto-refresh.                                                                                 |
| `Esc` / `q`            | Pop screen / quit at the root.                                                                       |
| `j` / `k` / `Ctrl+u/d` | Navigate tables vim-style.                                                                           |
| `gg` / `G`             | Jump to top / bottom.                                                                                |
| `s` / `S`              | Cycle sort column / direction.                                                                       |
| `Space`                | Multi-select row.                                                                                    |
| `f`                    | Follow mode (messages / logs).                                                                       |
| `[` / `]`              | Load earlier / later messages.                                                                       |
| `g o` / `g t` / `g p`  | Jump by offset / timestamp / partition.                                                              |
| `1` / `2` / `3`        | Value view: JSON / Raw / Hex.                                                                        |
| `y k/v/h/a`            | Copy key / value / headers / all.                                                                    |
| `R` / `Shift+R`        | Reset consumer group offsets (4-step / express).                                                     |

Read-only clusters block destructive actions (`n`, `D`, `y`, `p`, `r`,
`R`, `Shift+R`).

## Development

```bash
make deps       # go mod tidy + vendor
make build      # build to ./dist/kafka-tui
make test       # full test suite
make race       # tests with -race
make cover      # coverage report (HTML via `go tool cover -html=coverage.out`)
make lint       # prek-managed pre-commit hooks (golangci-lint, gofmt, etc.)
```

Tests use the in-process `kfake` broker (`github.com/twmb/franz-go/pkg/kfake`)
rather than testcontainers, so the suite runs without Docker. Persistent
state uses `modernc.org/sqlite` to avoid CGO.

The project layout and screen-development conventions are documented in
`CLAUDE.md`.

## License

MIT.
