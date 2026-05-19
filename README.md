# kafka-tui

Terminal UI client for Apache Kafka. Browse and manage topics, messages, and consumer groups
with k9s-style navigation, hierarchical YAML configuration, Vault-backed secrets, and
read-only protection for production clusters.

Built on [Bubble Tea v2](https://github.com/charmbracelet/bubbletea) and
[franz-go](https://github.com/twmb/franz-go).

> [!WARNING]
> Early project — not yet tested against production Kafka clusters. Use with caution:
> check on a staging cluster before pointing it at critical environments.

- [Why kafka-tui?](#why-kafka-tui)
- [Features](#features)
- [Installation](#installation)
- [How to Use](#how-to-use)
  - [Quick Start](#quick-start)
  - [Configuration](#configuration)
  - [Placeholders](#placeholders)
- [Development](#development)

## Why kafka-tui?

- **Runs anywhere** — a single static binary that works the same on your laptop, in a Docker
  container, or over SSH on a production jump-host. No JVM, no Electron, no browser. When
  you need to inspect a topic on a locked-down server, you copy the binary in and you're done.
- **Lives next to your editor** — modern editors like [Zed](https://zed.dev/) let you bind
  console tools to tasks, so kafka-tui opens in a terminal pane right inside the editor —
  effectively another editor window. Same idea works in tmux, WezTerm, Kitty splits, or any
  terminal multiplexer you already use.
- **Keyboard-first, k9s-style** — fuzzy search, vim-like navigation, follow-mode, copy /
  save / open-in-`$EDITOR` on any value. No clicking through tabs to read a message.

### Example: Zed task

Drop this into `.zed/tasks.json` to launch kafka-tui from the command palette
(`task: spawn` → `kafka-tui`):

```json
[
  {
    "label": "kafka-tui",
    "command": "kafka-tui",
    "use_new_terminal": false,
    "allow_concurrent_runs": false,
    "reveal": "always"
  }
]
```

The task opens in Zed's integrated terminal as a regular pane — split it, move it, close it
like any other editor window.

## Features

- Cluster picker with colored health status and per-cluster `[RO]` read-only enforcement
- Topic list with configurable columns, fuzzy search, sorting, and create / clone / delete flows
- Message browser with follow-mode, partition filters, jump-by-offset / timestamp / partition,
  JSON / raw / hex value views, and copy / save / open-in-`$EDITOR`
- Producer form with compression, dynamic headers, history, prefill-from-last, and resend-from-message
- Consumer groups list with lazy lag aggregation, detail view, and 4-step (or express) reset offsets flow
- Hierarchical YAML config: global (`~/.kafka-tui/`) and project (`<repo>/.kafka-tui/`) layers
- Placeholders in any string field: `${env:...}`, `${file:...}`, `${vault:...}`
- Vault KV v2 integration with token resolution chain
- Live config reload via fsnotify watcher (500 ms debounce)
- Clipboard via OSC 52 (SSH-friendly), native (`pbcopy` / `xclip` / `wl-copy`), or both
- In-app log viewer (`:logs`) with follow-mode and colored levels

## Installation

Download the latest release from [releases](https://github.com/aleksey925/kafka-tui/releases) and install it manually
or you can run the following commands to install the latest version to `~/.local/bin`:

```bash
VERSION=$(curl -sL -o /dev/null -w '%{url_effective}' https://github.com/aleksey925/kafka-tui/releases/latest | sed 's/.*\/v//')
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
curl -#L "https://github.com/aleksey925/kafka-tui/releases/download/v${VERSION}/kafka-tui_${VERSION}_${OS}_${ARCH}.tar.gz" | tar xz -C ~/.local/bin kafka-tui
```

Also, you can build it from source:

```bash
git clone https://github.com/aleksey925/kafka-tui.git
cd kafka-tui
make install  # copies to ~/.local/bin
```

Make sure `~/.local/bin` is in your PATH:

```bash
export PATH="$HOME/.local/bin:$PATH"
```

## How to Use

### Quick Start

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

Then launch with no flags — the cluster picker opens, `Enter` connects:

```bash
kafka-tui
```

Annotated `config.yaml` and `clusters.yaml` samples live in [`examples/`](examples/).

> When `--brokers` is given and a cluster of the same name also exists in `clusters.yaml`, the CLI
> cluster wins for the session and a warning toast notes the collision.

### Configuration

Two files are read from each layer:

| File            | Purpose                                                       |
| --------------- | ------------------------------------------------------------- |
| `config.yaml`   | UI behavior (logging, columns, produce / clipboard / vault).  |
| `clusters.yaml` | Cluster definitions (brokers, color, `read_only`, SASL, TLS). |

Layers, lowest priority first:

1. **Global** — `~/.kafka-tui/{config,clusters}.yaml`
2. **Project** — `<repo>/.kafka-tui/{config,clusters}.yaml`, located by walking up from `cwd`
   (same lookup rule as `.git`)

Merge rules:

- Scalars in the project layer override the global layer
- Lists are replaced wholesale (e.g. `topics.columns`)
- Cluster lists are merged by `name`; per-field overrides apply within a matching entry

`:config sources` opens an in-app screen showing which file each field came from. Annotated
samples covering all fields below live in [`examples/`](examples/).

#### `config.yaml` — UI behavior

| Section     | Field                      | Description                                                                                                                       |
| ----------- | -------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `logging`   | `level`                    | Log level: `debug` / `info` / `warn` / `error`.                                                                                   |
|             | `file`                     | Log file path (supports `${env:...}`, `${file:...}`, and `~`; vault placeholders not allowed here).                               |
|             | `max_size_mb`, `max_files` | Rotation thresholds for the log file.                                                                                             |
| `topics`    | `columns`                  | Visible columns: `name`, `partitions`, `replicas`, `message_count`, `size`, `cleanup_policy`, `retention`, `min_isr`.             |
| `groups`    | `columns`                  | Visible columns: `name`, `state`, `members`, `total_lag`, `coordinator`.                                                          |
| `messages`  | `columns`                  | Visible columns: `timestamp`, `partition`, `offset`, `key`, `value_preview`, `headers`.                                           |
| `produce`   | `history_size`             | How many recent produce events to keep for `Ctrl+P` / `Ctrl+N` recall.                                                            |
|             | `default_compression`      | Default compression in the producer form: `none` / `gzip` / `snappy` / `lz4` / `zstd`.                                            |
| `clipboard` | `method`                   | `auto` (native + OSC 52 in parallel), `native` (`pbcopy` / `xclip` / `wl-copy`), or `osc52`.                                      |
| `vault`     | `address`                  | Vault server URL. Required only when `${vault:...}` placeholders appear anywhere. Overridable via `--vault-addr`.                 |
|             | `token`                    | Vault token. Overridable via `--vault-token`. Empty value falls through the resolution chain (see [Placeholders](#placeholders)). |

Lists (`columns`) replace wholesale across layers; everything else is merged scalar-by-scalar.

#### `clusters.yaml` — cluster definitions

| Field       | Required | Description                                                                                                             |
| ----------- | -------- | ----------------------------------------------------------------------------------------------------------------------- |
| `name`      | yes      | Identifier shown in the picker and used as the merge key across layers.                                                 |
| `brokers`   | yes      | List of `host:port` bootstrap brokers.                                                                                  |
| `color`     | no       | Tint for the picker and chrome: `red` / `yellow` / `green` / `gray` / `white`.                                          |
| `read_only` | no       | When `true`, blocks all destructive actions (produce, delete, reset offsets).                                           |
| `sasl`      | no       | `mechanism` (`PLAIN` / `SCRAM-SHA-256` / `SCRAM-SHA-512`), `username`, `password`.                                      |
| `tls`       | no       | `ca_file` / `ca`, `cert_file` / `cert`, `key_file` / `key`, `skip_verify`. Empty `tls: {}` enables TLS with system CAs. |

TLS is auto-detected: presence of `tls:` (even empty) enables it; SASL without `tls` means
`SASL_PLAINTEXT`. Specifying both inline content (`cert: |`) and a `*_file` path for the same key
is rejected at startup.

### Placeholders

Any string field may contain one or more placeholders. Resolution runs in two phases: `env` and
`file` first, then `vault`. Nested placeholders are rejected.

| Placeholder           | Description                              |
| --------------------- | ---------------------------------------- |
| `${env:VAR}`          | Read environment variable `VAR`.         |
| `${env:VAR:-default}` | Same, with a default when unset/empty.   |
| `${file:/abs/path}`   | Read the file's full contents.           |
| `${vault:path}`       | Read the whole KV v2 secret as a map.    |
| `${vault:path#key}`   | Read a single field from a KV v2 secret. |

Vault address resolution: `--vault-addr CLI → vault.address in config`.

Vault token resolution: `--vault-token CLI → vault.token in config → $VAULT_TOKEN → ~/.vault-token`.

## Development

### Prerequisites

- [mise](https://mise.jdx.dev/getting-started.html#installing-mise-cli) for managing toolchains

### Set up environment

- install toolchains and deps

  ```bash
  mise trust && mise install
  make deps
  ```

- verify the setup by running tests

  ```bash
  make test
  ```

### Build

Two options:

- `make build` — builds the binary into `dist/`.
- `make install` — builds and installs the binary to `~/.local/bin`.
  Ensure this directory is in your `PATH`:

  ```bash
  export PATH="$HOME/.local/bin:$PATH"
  ```

A snapshot release with all platform binaries can be produced via `make snapshot`
(requires [`goreleaser`](https://goreleaser.com)).
