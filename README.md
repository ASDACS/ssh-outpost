# ssh-outpost

[English](README.en.md) | 中文

一个**极简远程执行服务端**：由客户端通过 SSH 部署到远端并拉起，随后常驻，跨会话为上层应用提供受控的远程执行能力。

- 基座只提供**基础设施**（生命周期、协议、插件管理、能力接口版本校验、子进程管理、元能力），不提供任何业务能力。
- 业务能力全部来自 `plugins/` 下的一次性加载插件，由**持有它的软件**随部署一并上传（见「插件归属」）。
- Python 3.7+、仅标准库、仅支持 Linux、全生命周期不依赖任何特权。
- 程序入口 `server/ssh_server.py` 与协议/API 中的 `ssh_server_*`、`ssh_server_api`、`ssh_server_base` 名称保持历史命名，不随项目更名而改变。
- 设计规范见 [`design.md`](design.md)；接口与部署规范见 [`interface-and-deployment-spec.md`](interface-and-deployment-spec.md)。本 README 侧重「怎么跑、怎么写插件、怎么测」。

> **溯源**：本项目最初是为 dsh-remote-playground 设计的远程执行后端；现在它已经成为一个可以独立部署的项目，不依赖 dsh-remote-playground 或任何特定客户端。除此之外，本仓库不涉及该项目的说明。

## 目录结构

```
ssh-outpost/
├── design.md                          # 服务端设计规范
├── interface-and-deployment-spec.md   # 接口与部署规范（客户端必须遵守的契约）
├── README.md
├── LICENSE                            # MIT
├── run_tests.sh                       # 运行全部单元测试
├── server/                            # 部署单元：打包 tar.gz 时以该目录为根
│   ├── version.txt                    # 版本号（单行 MAJOR.MINOR.PATCH）
│   ├── ssh_server.py                  # 基座（单文件）
│   └── plugins/
│       └── example.py                 # 示例插件（开发模板，含能力版本声明）
└── tests/                             # 单元测试（标准库 unittest）
    ├── _helpers.py                    # 临时目录、起停服务、最小 NDJSON 客户端
    ├── test_plugins.py                # 插件加载、命名校验、能力接口版本校验
    ├── test_truncate.py               # 输出截断缓冲
    ├── test_children.py               # 子进程登记与崩溃残留回收
    ├── test_protocol.py               # 握手 / 元能力 / stream / job 集成测试
    └── test_lifecycle.py              # flock、server.info、空闲退出、退出清理
```

### 插件归属

ssh-outpost 是**某一个软件的能力远程延伸**，不是多个软件共享的公共基座：

- **一个软件一个基座**：同一软件的不同组件各自持有自己的插件，由该软件的部署逻辑一并上传到一个基座、经同一条 SSH 端口转发访问——避免每个组件各部署一个基座、各开一条转发通道。
- **插件之间假定兼容、不做隔离**：命名与版本协调是该软件**部署前**必须解决的问题；基座不做隔离、不做插件间能力依赖解析、不做多来源命名仲裁。
- **基座只做 fail fast**：插件与基座的接口兼容由能力接口版本校验在启动期强制；不匹配就整体拒绝启动，不进入半可用状态。

因此本仓库只保留基座与示例插件，业务插件请放在持有它的软件仓库里。

运行时目录（部署到远端后的样子；**`--base-dir` 必填**，位置由调用方决定，基座不提供默认值）：

```
<base-dir>/                   # 0700
    server.lock               # flock 目标，常驻不删
    server.info               # 运行时状态（pid/port/token/version），原子覆盖，0600
    children.json             # 存活子进程记录，崩溃残留回收用
    server.log                # 仅记录生命周期与错误
    server/                   # 代码目录，升级时整体替换
        version.txt
        ssh_server.py
        plugins/
```

## 快速开始

本地试跑（不经过客户端）：

```bash
# --base-dir 必填：基座不决定自己在哪里，位置始终由调用方给出
python3 server/ssh_server.py --base-dir /tmp/ssh-outpost-demo
```

启动成功会向 stdout 打印一行就绪报文：

```json
{"ok": true, "pid": 12345, "port": 41234, "version": "0.1.0", "protocol": 1}
```

随后可在 `/tmp/ssh-outpost-demo/server.info` 读到 `port` 与 `token`。手工验证一次调用：

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

真实部署由客户端完成：sftp 上传 `server/` 打包，ssh 以 `setsid` 拉起，再通过端口转发访问回环端口。

## 命令行参数

| 参数 | 默认 | 说明 |
|---|---|---|
| `--base-dir` | **必填** | 运行目录；位置由调用方决定，基座不提供默认值 |
| `--idle-timeout` | 900 | 空闲退出秒数 |
| `--threads` | 4 | 插件调用线程池大小 |
| `--max-concurrent` | 8 | 单连接最大并发请求数 |
| `--job-timeout` | 1800 | job 模式单任务超时秒数 |
| `--job-cache` | 64 | 已完成任务结果保留条数（LRU） |
| `--truncate-cap` | 262144 | job 模式单流输出上限（字节） |
| `--log` | `<base-dir>/server.log` | 日志路径 |
| `--debug` | 关 | 调试日志与错误 `detail`（traceback） |

## 协议速览

传输：TCP，仅监听 `127.0.0.1`（内核分配端口），客户端经 SSH 端口转发抵达；UTF-8，**换行分隔 JSON（NDJSON）**，一行一个报文。

```jsonc
// 握手（连接建立后必须先发）
{"type": "hello", "protocol": 1, "token": "<64hex>", "client": "<client>/<version>"}
{"type": "hello_ok", "protocol": 1, "version": "0.1.0", "pid": 123}
{"type": "hello_err", "error": "bad_token"}

// 调用：mode 为 "stream"（默认）或 "job"
{"type": "call", "id": 1, "tool": "fs_read", "params": {}, "mode": "stream"}
{"type": "ack", "id": 1}                                              // 仅 stream 插件调用
{"type": "event", "id": 1, "stream": "stdout", "seq": 0,
 "encoding": "utf-8", "data": "..."}                                  // 仅 stream 模式
{"type": "result", "id": 1, "ok": true, "result": {}}
{"type": "result", "id": 1, "ok": false,
 "error": {"code": "tool_error", "message": "...", "detail": "..."}}

// job 模式：立即回执，随后用 ssh_server_job_get 轮询
{"type": "result", "id": 1, "ok": true, "result": {"job_id": "3f2a…", "state": "running"}}
```

错误码（客户端可依赖的稳定字符串）：`bad_request`、`bad_token`、`unknown_tool`、`unknown_job`、`bad_params`、`tool_error`、`busy`、`timeout`、`internal`。

### 两种执行模式

| | `stream`（默认） | `job` |
|---|---|---|
| 输出 | 即时 `event`，`result` 只带元信息 | 服务端累积，轮询取回 |
| 断连 | **取消请求并释放资源** | 不影响任务 |
| 超时 | 无硬超时，由客户端断连取消 | 超过 `--job-timeout` 置为 `timeout` |
| 输出上限 | 不限制（由客户端处理） | `--truncate-cap`，超出保留头尾 + 占位符 |
| 结果保留 | 不保留 | 内存 LRU，服务退出即丢失 |

### 元能力（基座内建，`ssh_server_` 前缀保留）

| 能力 | 参数 | 说明 |
|---|---|---|
| `ssh_server_ping` | `{}` | 存活/版本探测；同时作为心跳刷新空闲计时 |
| `ssh_server_list_tools` | `{}` | 能力发现，返回插件能力的描述与参数 JSON Schema |
| `ssh_server_info` | `{}` | 诊断信息（版本、协议、解释器、平台、空闲超时、能力接口版本表） |
| `ssh_server_shutdown` | `{reason?}` | 先回执，再优雅退出 |
| `ssh_server_job_get` | `{job_id}` | 轮询任务状态与累计输出 |
| `ssh_server_job_cancel` | `{job_id}` | 取消任务并终止其子进程 |

## 插件开发

### 约定

- 每个插件一个 `.py` 文件，直接位于 `server/plugins/`，无子目录、无包结构。
- 插件名 = 文件名 stem（`fs.py` → `fs`）；能力名必须是 `fs_toolname` 形式，基座强制校验前缀。
- `ssh_server` 是保留插件名，`ssh_server_` 是保留前缀；以 `_` 开头的文件视为辅助模块，跳过加载。
- 插件**无状态**、不互相 import、不依赖加载顺序。
- 插件只能通过基座注入的 `ssh_server_api` 访问子进程/输出/日志，不得自行调用 `subprocess`。
- 插件用模块级 `REQUIRES` 声明它用到的基座能力及其**精确版本**（见下文「能力接口版本」）。
- 任何加载/校验失败都 **fail fast**：整个服务启动失败，避免半可用状态误导调用方。

### 最小插件

```python
# server/plugins/fs.py
REQUIRES = {"ssh_server_base@log": "0.1.0"}     # 声明用到的基座能力及其版本

def read_file(params):
    path = params["path"]
    with open(path, "r", encoding="utf-8") as fh:
        return {"content": fh.read()}

def registry(tools, description):
    tools["fs_read"] = read_file
    description["fs_read"] = {
        "description": "读取远端文件内容",
        "parameters": {
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"],
            "additionalProperties": False,
        },
    }
```

### 基座 API：`ssh_server_api`

```python
from ssh_server_api import spawn, emit, log, config

def run(params):
    emit("stdout", "开始\n")                       # 主动流式输出
    child = spawn(params["argv"], cwd=params.get("cwd"))
    return {"exit_code": child.wait()}             # 输出默认自动转发到当前请求
```

- `spawn(argv, cwd=None, env=None, stdin=None, on_stdout=None, on_stderr=None, timeout=None)`
  唯一的子进程入口。子进程各自成为独立进程组组长，基座负责登记、按需流式回调、超时与退出清理。
  `stdin` 传 `str`/`bytes` 时写入后关闭；不传回调时输出默认转发到当前请求。
- `emit(stream, data, encoding="utf-8")`
  `stream` 为 `"stdout"` 或 `"stderr"`；`str` 按 `encoding` 编码，`bytes` 默认视为 UTF-8 文本，
  `encoding="base64"` 时视为二进制。请求被取消时抛 `RequestCancelled`，插件应让它向外传播。
- `log(level, message)`：写 `server.log`，仅用于错误与生命周期，不要用于逐请求审计。
- `config`：只读配置（`idle_timeout`、`version`、`platform`、`base_dir`）。
- `capabilities`：只读的能力接口版本表（`{能力名: 版本}`），供插件自查。

完整可运行示例见 [`server/plugins/example.py`](server/plugins/example.py)，其中包含 `example_echo`（最小）、`example_run`（spawn + 自动流式）、`example_progress`（主动 emit，可配合 job 超时观察取消），以及 `REQUIRES` 声明。

### 能力接口版本

插件与基座之间唯一的接口就是 `ssh_server_api`。为了让「签名级不兼容」在**启动期**暴露，
插件声明它用到的能力及其版本，基座在加载期逐条比对。

```python
# 基座侧（CAPABILITY_VERSIONS，唯一事实来源；插件无需知道其内容，只需声明自己用到的）
{"spawn": "0.1.0", "emit": "0.1.0", "log": "0.1.0", "config": "0.1.0", "capabilities": "0.1.0"}

# 插件侧（模块级常量；只声明真正用到的能力，不声明全量）
REQUIRES = {
    "ssh_server_base@spawn": "0.1.0",
    "ssh_server_base@emit": "0.1.0",
}
```

- 能力标识是 `<归属>@<能力名>`；本阶段归属只有 `ssh_server_base`（基座自身）。归属不是
  `ssh_server_base` 的声明本阶段**既不校验也不报错**（为将来可能的插件间能力依赖预留）。
- 语义是**精确匹配**，不是最低版本：声明必须与基座提供的版本逐一相等，基座不承诺
  「新版能力能跑旧插件」。代价是破坏性变更必须让相关插件同步更新声明。必须承认在这个定位的组件上，兼容性问题是个大麻烦，后续可能会引入兼容机制，但这里不做承诺。
- 承诺范围仅限**接口表面**（有哪些能力、参数与返回结构），**不承诺**行为一致性，也不承诺
  插件之间业务耦合导致的不兼容（那是上位软件部署前要解决的）。
- 只有**破坏接口兼容**的变更才迭代版本号；增加带默认值的参数、返回结构增加字段等向后兼容
  的新增**不**迭代——否则基座每加一个可选参数都会让所有声明旧版本的插件失效。
- 不匹配即 `PluginLoadError`：**整个服务启动失败**，不进入半可用状态。启动错误串自带
  「谁要求什么 / 基座本地提供什么」；已启动的服务可用 `ssh_server_info` 取回同一张表。

完整规则与「破坏兼容」判定表见 [`design.md`](design.md) §8.5。

## 业务插件与示例

`plugins/` 下只有示例插件 `example.py`，作为开发模板与手工验证样例。

原先随基座提供的 `host`（宿主机文件/命令）与 `container`（Apptainer 容器内执行）两个插件
**已移出本仓库**：按「一个软件一个基座、插件由持有它的软件维护」的定位，它们属于使用它们的
软件的组件，随该软件的部署一并上传到基座。

## 关键机制

- **单实例**：`server.lock` 上 `flock(LOCK_EX | LOCK_NB)`，持有至进程结束；抢占失败输出 `already_running` 并退出。锁文件放在 `server/` 之外，升级替换代码目录不会破坏单实例。
- **就绪与发现**：绑定 `127.0.0.1:0` 后原子写 `server.info`（临时文件 → fsync → `os.replace`），客户端读文件 + 连接握手判定存活；`server.info` 永不删除，只被新实例覆盖。
- **崩溃残留回收**：`children.json` 记录子进程 pgid 与 `/proc/<pid>/stat` 启动时间；启动时列表应为空，非空则逐个校验后清理，**starttime 不一致（pid 复用）一律跳过**，避免误杀。
- **空闲退出**：每 60 秒检查；`idle_timeout` 内无请求、无在途请求、无运行中任务、无存活子进程才退出。长任务与慢客户端都不会被空闲逻辑打断。
- **背压**：流式事件以阻塞方式写 socket；客户端读得慢时输出线程被阻塞，OS 管道填满后子进程自然挂起。**不丢数据**。
- **输出截断**：仅 job 模式；每个流保留头部与尾部各一半，中间插入 `...<truncated N bytes>...` 并标记 `truncated`。
- **安全边界**：仅监听回环 + 每实例随机 token 握手；基座不做授权/沙箱判断——所有决策在客户端，对 root 远端等价于完全信任，这是有意为之。

## 测试

```bash
./run_tests.sh          # 安静模式
./run_tests.sh -v       # 详细模式
# 等价于：python3 -m unittest discover -s tests -t .
```

覆盖范围：

- `test_plugins.py`：合法加载、前缀/保留名/保留前缀/导入错误/不可调用/孤儿描述等 fail fast 路径，以及能力接口版本校验（精确匹配、版本不符、未知能力、格式非法、外部归属忽略、`registry()` 不被执行、API 模块只读）；
- `test_truncate.py`：头尾保留、占位符、增量追加、多字节边界；
- `test_children.py`：登记持久化、崩溃回收、**pid 复用防误杀**、退出清理；
- `test_protocol.py`：握手（含坏 token、协议不匹配）、元能力（含 `info.capabilities`）、参数校验、能力版本不匹配时启动失败、stream 事件顺序、子进程输出、断连取消、线程池饱和返回 `busy`、job 提交/轮询/超时/取消/截断/优雅关闭；
- `test_lifecycle.py`：`server.info` 字段与权限、flock 单实例、锁释放、空闲退出、有请求时不空闲退出、退出后 `children.json` 清空、命令行解析。

业务插件（文件读写、命令执行等）的单测随插件代码一起，由持有它的软件维护，不在本仓库。

骨架（未实现，标 `@unittest.skip`）：以子进程方式拉起服务并发送 `SIGTERM` 的端到端用例。

## 项目不包含

- 客户端（部署、拉起、复用、升级判定、把能力封装成上层接口）；
- 业务插件（宿主机文件/命令、容器内执行等）与其单测（由持有它的软件维护）；
- 部署包（`tar.gz`）构建脚本与远端部署脚本（由客户端侧负责）；

## 许可证

本项目以 [MIT License](LICENSE) 授权，版权归 `Copyright (c) 2026 ASDACS` 所有。
