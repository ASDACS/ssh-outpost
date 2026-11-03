# ssh-outpost

English | [中文](README.md)

A **minimal remote-execution server**: a client deploys it to a remote host over SSH and starts it; it then stays resident and provides controlled remote-execution capabilities to upper-layer applications across sessions.

- The base provides only **infrastructure** (lifecycle, protocol, plugin management, capability-interface version checking, child-process management, meta-capabilities); it provides no business capabilities.
- All business capabilities come from plugins that are loaded once at startup under `plugins/`, uploaded together with the deployment by **the software that owns them** (see "Plugin ownership").
- Python 3.7+, standard library only, Linux only, and no privileges required throughout its lifecycle.
- The program entry point `server/ssh_server.py` and the `ssh_server_*`, `ssh_server_api`, and `ssh_server_base` names in the protocol/API keep their historical names and do not change with the project rename.
- See [`design.md`](design.md) for the design specification and [`interface-and-deployment-spec.md`](interface-and-deployment-spec.md) for the interface and deployment specification. This README focuses on "how to run it, how to write plugins, and how to test it".

> **Origin**: This project was originally designed as the remote-execution backend for dsh-remote-playground; it is now a standalone deployable project that does not depend on dsh-remote-playground or any specific client. Apart from this note, this repository does not cover that project.

## Directory structure

```
ssh-outpost/
├── design.md                          # Server design specification
├── interface-and-deployment-spec.md   # Interface and deployment specification (client contract)
├── README.md
├── LICENSE                            # MIT
├── run_tests.sh                       # Run all unit tests
├── server/                            # Deployment unit: this directory is the root of the tar.gz
│   ├── version.txt                    # Version number (single line MAJOR.MINOR.PATCH)
│   ├── ssh_server.py                  # Base (single file)
│   └── plugins/
│       └── example.py                 # Example plugin (development template with capability version declarations)
└── tests/                             # Unit tests (standard-library unittest)
    ├── _helpers.py                    # Temp directories, start/stop service, minimal NDJSON client
    ├── test_plugins.py                # Plugin loading, naming checks, capability-interface version checks
    ├── test_truncate.py               # Output truncation buffer
    ├── test_children.py               # Child-process registry and crash-residue reclamation
    ├── test_protocol.py               # Handshake / meta-capabilities / stream / job integration tests
    └── test_lifecycle.py              # flock, server.info, idle exit, shutdown cleanup
```

### Plugin ownership

ssh-outpost is the **remote extension of one software's capabilities**, not a shared public base for multiple software projects:

- **One base per software**: Different components of the same software each own their plugins, and the software's deployment logic uploads them together to one base, accessed through the same SSH port forward—avoiding one base and one port forward per component.
- **Plugins are assumed compatible; no isolation**: Naming and version coordination is a problem the software **must** solve before deployment; the base provides no isolation, no inter-plugin capability dependency resolution, and no multi-source naming arbitration.
- **The base only fails fast**: Plugin/base interface compatibility is enforced at startup by capability-interface version checking; on a mismatch it rejects startup as a whole and never enters a half-usable state.

Therefore this repository keeps only the base and the example plugin; put business plugins in the repository of the software that owns them.

Runtime directory (what it looks like after deployment to the remote host; **`--base-dir` is required**, its location is chosen by the caller, and the base provides no default):

```
<base-dir>/                   # 0700
    server.lock               # flock target, resident and never deleted
    server.info               # Runtime state (pid/port/token/version), atomically overwritten, 0600
    children.json             # Live child-process records, used for crash-residue reclamation
    server.log                # Lifecycle and errors only
    server/                   # Code directory, replaced as a whole on upgrade
        version.txt
        ssh_server.py
        plugins/
```

## Quick start

Local trial run (without a client):

```bash
# --base-dir is required: the base does not decide where it lives; the caller always provides the location
python3 server/ssh_server.py --base-dir /tmp/ssh-outpost-demo
```

On success, it prints a one-line readiness message to stdout:

```json
{"ok": true, "pid": 12345, "port": 41234, "version": "0.1.0", "protocol": 1}
```

Then read `port` and `token` from `/tmp/ssh-outpost-demo/server.info`. To verify one call manually:

```python
import json, socket
info = json.load(open("/tmp/ssh-outpost-demo/server.info"))
sock = socket.create_connection(("127.0.0.1", info["port"]))
sock.sendall((json.dumps({
    "type": "hello", "protocol": 1, "token": info["token"], "client": "manual",
}) + "\n").encode())
print(sock.recv(4096))  # {"type": "hello_ok", ...}

sock.sendall((json.dumps({
    "type": "call", "id": 1, "tool": "ssh_server_list_tools", "params": {},
}) + "\n").encode())
print(sock.recv(65536))
```

Real deployment is done by the client: upload the `server/` package over sftp, start it with `setsid` over ssh, and reach the loopback port through port forwarding.

## Command-line arguments

| Argument | Default | Description |
|---|---|---|
| `--base-dir` | **required** | Runtime directory; its location is chosen by the caller, and the base provides no default |
| `--idle-timeout` | 900 | Idle-exit seconds |
| `--threads` | 4 | Plugin-call thread-pool size |
| `--max-concurrent` | 8 | Maximum concurrent requests per connection |
| `--job-timeout` | 1800 | Per-job timeout in seconds for job mode |
| `--job-cache` | 64 | Number of completed job results to retain (LRU) |
| `--truncate-cap` | 262144 | Per-stream output cap in job mode (bytes) |
| `--log` | `<base-dir>/server.log` | Log path |
| `--debug` | off | Debug logging and error `detail` (traceback) |

## Protocol overview

Transport: TCP, listening only on `127.0.0.1` (kernel-assigned port), reached by the client through SSH port forwarding; UTF-8, **newline-delimited JSON (NDJSON)**, one message per line.

```jsonc
// Handshake (must be sent first after the connection is established)
{"type": "hello", "protocol": 1, "token": "<64hex>", "client": "<client>/<version>"}
{"type": "hello_ok", "protocol": 1, "version": "0.1.0", "pid": 123}
{"type": "hello_err", "error": "bad_token"}

// Call: mode is "stream" (default) or "job"
{"type": "call", "id": 1, "tool": "fs_read", "params": {}, "mode": "stream"}
{"type": "ack", "id": 1}                                              // Stream plugin calls only
{"type": "event", "id": 1, "stream": "stdout", "seq": 0,
 "encoding": "utf-8", "data": "..."}                                  // stream mode only
{"type": "result", "id": 1, "ok": true, "result": {}}
{"type": "result", "id": 1, "ok": false,
 "error": {"code": "tool_error", "message": "...", "detail": "..."}}

// job mode: immediate acknowledgement, then poll with ssh_server_job_get
{"type": "result", "id": 1, "ok": true, "result": {"job_id": "3f2a…", "state": "running"}}
```

Error codes (stable strings clients may rely on): `bad_request`, `bad_token`, `unknown_tool`, `unknown_job`, `bad_params`, `tool_error`, `busy`, `timeout`, `internal`.

### Two execution modes

| | `stream` (default) | `job` |
|---|---|---|
| Output | Immediate `event`; `result` carries metadata only | Accumulated on the server, retrieved by polling |
| Disconnect | **Cancels the request and releases resources** | Does not affect the job |
| Timeout | No hard timeout; the client cancels by disconnecting | Exceeding `--job-timeout` sets it to `timeout` |
| Output cap | Unlimited (handled by the client) | `--truncate-cap`; retains head and tail plus a placeholder |
| Result retention | Not retained | In-memory LRU; lost when the service exits |

### Meta-capabilities (built into the base; the `ssh_server_` prefix is reserved)

| Capability | Parameters | Description |
|---|---|---|
| `ssh_server_ping` | `{}` | Liveness/version probe; also acts as a heartbeat to refresh the idle timer |
| `ssh_server_list_tools` | `{}` | Capability discovery; returns plugin capability descriptions and parameter JSON Schemas |
| `ssh_server_info` | `{}` | Diagnostic information (version, protocol, interpreter, platform, idle timeout, capability-interface version table) |
| `ssh_server_shutdown` | `{reason?}` | Acknowledges first, then shuts down gracefully |
| `ssh_server_job_get` | `{job_id}` | Polls job state and accumulated output |
| `ssh_server_job_cancel` | `{job_id}` | Cancels the job and terminates its child processes |

## Plugin development

### Conventions

- One `.py` file per plugin, directly under `server/plugins/`, with no subdirectories and no package structure.
- Plugin name = file-name stem (`fs.py` → `fs`); capability names must have the form `fs_toolname`, and the base enforces the prefix.
- `ssh_server` is a reserved plugin name, and `ssh_server_` is a reserved prefix; files starting with `_` are treated as helper modules and skipped during loading.
- Plugins are **stateless**, do not import each other, and do not depend on load order.
- Plugins may access child processes/output/logging only through the base-injected `ssh_server_api`; they must not call `subprocess` themselves.
- Plugins declare the base capabilities they use and their **exact versions** with the module-level `REQUIRES` (see "Capability interface versions" below).
- Any load/check failure **fails fast**: the whole service fails to start, avoiding a half-usable state that misleads callers.

### Minimal plugin

```python
# server/plugins/fs.py
REQUIRES = {"ssh_server_base@log": "0.1.0"}     # Declare the base capabilities used and their versions

def read_file(params):
    path = params["path"]
    with open(path, "r", encoding="utf-8") as fh:
        return {"content": fh.read()}

def registry(tools, description):
    tools["fs_read"] = read_file
    description["fs_read"] = {
        "description": "Read the contents of a remote file",
        "parameters": {
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"],
            "additionalProperties": False,
        },
    }
```

### Base API: `ssh_server_api`

```python
from ssh_server_api import spawn, emit, log, config

def run(params):
    emit("stdout", "start\n")                       # Active streaming output
    child = spawn(params["argv"], cwd=params.get("cwd"))
    return {"exit_code": child.wait()}             # Output is forwarded to the current request by default
```

- `spawn(argv, cwd=None, env=None, stdin=None, on_stdout=None, on_stderr=None, timeout=None)`
  The only child-process entry point. Each child process becomes its own process-group leader; the base handles registration, on-demand streaming callbacks, timeouts, and exit cleanup.
  When `stdin` is a `str`/`bytes`, it is written and then closed; without callbacks, output is forwarded to the current request by default.
- `emit(stream, data, encoding="utf-8")`
  `stream` is `"stdout"` or `"stderr"`; `str` is encoded according to `encoding`, and `bytes` is treated as UTF-8 text by default;
  with `encoding="base64"` it is treated as binary. When the request is cancelled, it raises `RequestCancelled`; plugins should let it propagate outward.
- `log(level, message)`: writes to `server.log`, only for errors and lifecycle events, not for per-request auditing.
- `config`: read-only configuration (`idle_timeout`, `version`, `platform`, `base_dir`).
- `capabilities`: read-only capability-interface version table (`{capability name: version}`), for plugins to check themselves.

See [`server/plugins/example.py`](server/plugins/example.py) for a complete runnable example, including `example_echo` (minimal), `example_run` (spawn + automatic streaming), `example_progress` (active emit, useful with job timeouts to observe cancellation), and the `REQUIRES` declaration.

### Capability interface versions

The only interface between plugins and the base is `ssh_server_api`. To expose "signature-level incompatibility" at **startup**, plugins declare the capabilities they use and their versions, and the base compares them one by one at load time.

```python
# Base side (CAPABILITY_VERSIONS, the single source of truth; plugins do not need to know its contents, only to declare what they use)
{"spawn": "0.1.0", "emit": "0.1.0", "log": "0.1.0", "config": "0.1.0", "capabilities": "0.1.0"}

# Plugin side (module-level constant; declare only the capabilities actually used, not the full set)
REQUIRES = {
    "ssh_server_base@spawn": "0.1.0",
    "ssh_server_base@emit": "0.1.0",
}
```

- A capability identifier is `<owner>@<capability name>`; at this stage the only owner is `ssh_server_base` (the base itself). Declarations whose owner is not
  `ssh_server_base` are **neither checked nor rejected** at this stage (reserved for possible future inter-plugin capability dependencies).
- The semantics are **exact matching**, not a minimum version: the declaration must equal the version provided by the base one by one, and the base does not promise
  that "new capabilities can run old plugins". The cost is that breaking changes require the affected plugins to update their declarations in sync. It must be acknowledged that compatibility is a major headache for a component positioned like this; a compatibility mechanism may be introduced later, but no commitment is made here.
- The commitment covers only the **interface surface** (which capabilities exist, parameters, and return structures); it does **not** promise behavioral consistency, nor compatibility when business coupling between plugins causes conflicts (that is for the upper-layer software to resolve before deployment).
- Only changes that **break interface compatibility** bump the version number; backward-compatible additions such as parameters with default values or new fields in return structures do **not**
  bump it—otherwise every optional parameter added to the base would invalidate all plugins that declare old versions.
- A mismatch raises `PluginLoadError`: **the whole service fails to start** and does not enter a half-usable state. The startup error string includes
  "who requires what / what the base provides locally"; a running service can retrieve the same table through `ssh_server_info`.

See [`design.md`](design.md) §8.5 for the complete rules and the "breaking compatibility" decision table.

## Business plugins and examples

`plugins/` contains only the example plugin `example.py`, as a development template and a manual-verification sample.

The two plugins originally shipped with the base—`host` (host files/commands) and `container` (execution inside an Apptainer container)—
**have been moved out of this repository**: under the positioning of "one base per software, plugins maintained by the software that owns them", they belong to the components of the software that uses them
and are uploaded to the base together with that software's deployment.

## Key mechanisms

- **Single instance**: `flock(LOCK_EX | LOCK_NB)` on `server.lock`, held until the process exits; if acquisition fails, it prints `already_running` and exits. The lock file lives outside `server/`, so replacing the code directory during an upgrade does not break single-instance behavior.
- **Readiness and discovery**: after binding `127.0.0.1:0`, atomically write `server.info` (temp file → fsync → `os.replace`); the client reads the file and performs a connection handshake to determine liveness; `server.info` is never deleted and is only overwritten by a new instance.
- **Crash-residue reclamation**: `children.json` records each child's pgid and the `/proc/<pid>/stat` start time; at startup the list should be empty, and if not, each entry is verified and then cleaned up, with **any starttime mismatch (pid reuse) skipped** to avoid killing unrelated processes.
- **Idle exit**: checked every 60 seconds; exit only when there have been no requests, no in-flight requests, no running jobs, and no live child processes for `idle_timeout`. Long jobs and slow clients are never interrupted by the idle logic.
- **Backpressure**: streaming events are written to the socket in blocking mode; if the client reads slowly, the output thread is blocked, the OS pipe fills up, and the child process naturally suspends. **No data is dropped**.
- **Output truncation**: job mode only; each stream keeps half of the head and half of the tail, inserts `...<truncated N bytes>...` in the middle, and marks `truncated`.
- **Security boundary**: loopback-only listening plus a per-instance random token handshake; the base makes no authorization/sandbox decisions—all decisions are made by the client, and on a root remote it is equivalent to full trust, intentionally so.

## Tests

```bash
./run_tests.sh          # quiet mode
./run_tests.sh -v       # verbose mode
# equivalent to: python3 -m unittest discover -s tests -t .
```

Coverage:

- `test_plugins.py`: valid loading; fail-fast paths such as prefix/reserved name/reserved prefix/import error/non-callable/orphan description; and capability-interface version checking (exact match, version mismatch, unknown capability, invalid format, external owner ignored, `registry()` not executed, API module read-only);
- `test_truncate.py`: head/tail retention, placeholder, incremental append, multibyte boundaries;
- `test_children.py`: registration persistence, crash reclamation, **pid-reuse mis-kill protection**, exit cleanup;
- `test_protocol.py`: handshake (including bad token and protocol mismatch), meta-capabilities (including `info.capabilities`), parameter validation, startup failure on capability-version mismatch, stream event ordering, child-process output, disconnect cancellation, `busy` when the thread pool is saturated, and job submission/polling/timeout/cancellation/truncation/graceful shutdown;
- `test_lifecycle.py`: `server.info` fields and permissions, single-instance flock, lock release, idle exit, no idle exit while requests are active, `children.json` cleared after exit, and command-line parsing.

Unit tests for business plugins (file read/write, command execution, etc.) live with the plugin code and are maintained by the software that owns them; they are not in this repository.

Skeleton (not implemented, marked `@unittest.skip`): an end-to-end case that starts the service as a subprocess and sends `SIGTERM`.

## Not included in the project

- Client (deployment, startup, reuse, upgrade decisions, wrapping capabilities as upper-layer interfaces);
- Business plugins (host files/commands, in-container execution, etc.) and their unit tests (maintained by the software that owns them);
- Deployment package (`tar.gz`) build script and remote deployment script (the client side is responsible for these);

## License

This project is licensed under the [MIT License](LICENSE), copyright `Copyright (c) 2026 ASDACS`.
