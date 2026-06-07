# kafka-tui

A k9s-style terminal UI for Apache Kafka. Browse and manage topics, produce and inspect
messages, edit topic configuration, and administer consumer groups - the everyday Kafka
workflow, all from the keyboard.

Built on [Bubble Tea v2](https://github.com/charmbracelet/bubbletea) and
[franz-go](https://github.com/twmb/franz-go).

![kafka-tui demo](img/demo.gif)

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

- Browse topics, messages, and consumer groups with fuzzy filter, sort, and auto-refresh; live tail for messages
- Inspect any record as JSON / raw / hex and copy, save, or open it in `$EDITOR`
- Produce messages with compression and custom headers, or resend an existing one
- Manage topics (create / clone / delete) and consumer groups (reset offsets, delete)
- Multi-cluster with colored health status, per-cluster read-only guards, SASL / TLS, and
  Vault-backed secrets
- One static binary with k9s-style keyboard UX

See [Configuration](#configuration) for layered YAML, placeholders, and Vault setup.

## Installation

The easiest way is via [Homebrew](https://brew.sh):

```bash
brew install aleksey925/apps/kafka-tui
```

Alternatively, download the latest release from [releases](https://github.com/aleksey925/kafka-tui/releases) and install
it manually or you can run the following commands to install the latest version to `~/.local/bin`:

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
kafka-tui connect --brokers localhost:9092
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

Connecting at startup goes through the `connect` subcommand, which reaches a cluster two
mutually-exclusive ways:

- `connect <name>` **auto-connects** to a cluster loaded from `clusters.yaml`, skipping the
  picker. An unknown or invalid name lands on the picker with a warning toast — not a hard failure.
- `connect --brokers host:9092,...` defines an **inline cluster** for this session, with
  `--tls*`, `--sasl-*`, `--color`, and `--read-only` supplying its connection details. It always
  gets an auto-generated name with a `-cli` suffix (e.g. `bduzdc7w-cli`), shown in the picker.
  Inline-cluster view state (seek position, partition filter) does not persist across runs because
  the random part of the name changes — for persistent configuration, use `clusters.yaml`.

`connect` takes either a name or `--brokers`, never both. Global flags (`--config`, `--log-level`,
`--vault-addr`, `--vault-token`) live on the root command and work on either side of the
subcommand, e.g. `kafka-tui --log-level debug connect prod`.

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

| Section     | Field                      | Description                                                                                                                           |
| ----------- | -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `logging`   | `level`                    | Log level: `debug` / `info` / `warn` / `error`. Overridable via `--log-level`.                                                        |
|             | `file`                     | Log file path (supports `${env:...}`, `${file:...}`, and `~`; vault placeholders not allowed here).                                   |
|             | `max_size_mb`, `max_files` | Rotation thresholds for the log file.                                                                                                 |
| `topics`    | `columns`                  | Visible columns, in display order: `name`, `partitions`, `replicas`, `messages`, `size`, `cleanup_policy`, `retention_ms`, `min_isr`. |
| `groups`    | `columns`                  | Visible columns, in display order: `state`, `name`, `coordinator`, `protocol`, `members`, `total_lag`.                                |
| `messages`  | `columns`                  | Visible columns, in display order: `timestamp`, `partition`, `offset`, `key`, `value`, `headers`.                                     |
| `produce`   | `default_compression`      | Default compression in the producer form: `none` / `gzip` / `snappy` / `lz4` / `zstd`.                                                |
| `clipboard` | `method`                   | `auto` (native + OSC 52 in parallel), `native` (`pbcopy` / `xclip` / `wl-copy`), `osc52`, or `off`.                                   |
| `vault`     | `address`                  | Vault server URL. Required only when `${vault:...}` placeholders appear anywhere. Overridable via `--vault-addr`.                     |
|             | `token`                    | Vault token. Overridable via `--vault-token`. Empty value falls through the resolution chain (see [Placeholders](#placeholders)).     |

For `columns`, the list order is the on-screen order and any key you omit is hidden; an empty or
absent list falls back to the built-in defaults. Unknown keys are ignored with a warning toast.
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
marks the cluster invalid in the picker.

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

### Secrets on the command line

`--sasl-password` and `--vault-token` accept the same `${env:...}` / `${file:...}` /
`${vault:...}` placeholders as YAML fields — and you should always use them. A literal value
typed on the command line ends up in `ps`, `/proc/<pid>/cmdline`, and your shell history,
where any other user on the host can read it. The app prints a warning toast when it
detects a literal credential, but the leak has already happened by then.

Prefer one of:

```bash
# environment variable (works well with direnv / mise / etc)
kafka-tui connect --brokers prod:9092 --sasl-mechanism SCRAM-SHA-512 \
  --sasl-username svc --sasl-password '${env:KAFKA_PASS}'

# file (works well with mode 0600 secrets)
kafka-tui --vault-token '${file:/run/secrets/vault_token}' connect prod

# vault (chained: --vault-token resolves the placeholder, then SASL pulls from vault)
kafka-tui connect --brokers prod:9092 --sasl-mechanism PLAIN \
  --sasl-username svc --sasl-password '${vault:secret/kafka/prod#password}'
```

Root global flags (`--vault-addr` / `--vault-token`) are resolved once at startup and frozen;
the inline cluster's secrets re-resolve on config reload, like `clusters.yaml`.

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
