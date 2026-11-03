#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ssh-outpost 基座（单文件实现）
=============================

本文件是 ssh-outpost 的「基座」：它只提供**基础设施**，不提供任何业务能力。
具体能力（读写文件、执行命令、系统信息……）全部由 ``plugins/`` 下的插件提供。

基座负责的只有五件事：

1. **进程生命周期**：flock 单实例、端口绑定、``server.info`` 原子写、
   ``children.json`` 维护与崩溃残留回收、空闲自退、信号处理、子进程清理。
2. **通信协议**：监听回环端口、token 握手、NDJSON 分帧、请求分发、
   流式事件回传、任务（job）提交与轮询、错误封装。
3. **插件管理**：启动时扫描 ``plugins/*.py``、动态 import、调用 ``registry()``、
   校验能力名并汇总能力表。
4. **能力接口版本校验**：维护基座能力版本表（``CAPABILITY_VERSIONS``），
   校验插件 ``REQUIRES`` 声明的精确版本，不匹配即 fail fast。
5. **基础设施 API**：向插件注入 ``ssh_server_api`` 模块（``spawn`` / ``emit`` /
   ``log`` / ``config`` / ``capabilities``），以及内建元能力 ``ssh_server_*``。

定位与边界：ssh-outpost 是**某一个软件的能力远程延伸**，不是一个多软件共享的公共基座。
因此插件之间假定兼容、不做隔离、不做插件间能力依赖解析；插件与基座的兼容性由上位软件
在部署前解决，基座只负责 fail fast。详见 design.md §2.2、§8.1、§8.5。

设计约束（与 design.md 对应）：

* Python 3.7+，仅标准库，不使用 3.8+ 语法/API。
* 仅支持 Linux（依赖 flock、``/proc``、POSIX 进程组与信号）。
* 全生命周期不依赖任何特权。
* 所有鉴权/门控/决策都在客户端完成，基座无条件执行；
  基座只校验请求确实来自持有 token 的客户端。

协议速览（NDJSON，一行一个 JSON）::

    C→S  {"type":"hello","protocol":1,"token":"<64hex>","client":"..."}
    S→C  {"type":"hello_ok","protocol":1,"version":"0.1.0","pid":123}
    S→C  {"type":"hello_err","error":"bad_token"}

    C→S  {"type":"call","id":1,"tool":"fs_read","params":{},"mode":"stream"}
    S→C  {"type":"ack","id":1}                                  # 仅 stream 模式插件调用
    S→C  {"type":"event","id":1,"stream":"stdout","seq":0,...}  # 仅 stream 模式
    S→C  {"type":"result","id":1,"ok":true,"result":{...}}
    S→C  {"type":"result","id":1,"ok":false,"error":{...}}

    # mode="job" 时立即回执，随后用 ssh_server_job_get 轮询
    S→C  {"type":"result","id":1,"ok":true,"result":{"job_id":"...","state":"running"}}

命令行用法::

    python3 ssh_server.py [--base-dir DIR] [--idle-timeout 900] [--threads 4] ...
"""

import argparse
import base64
import codecs
import collections
import fcntl
import hmac
import importlib.util
import json
import logging
import os
import queue
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
import types

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

#: 协议版本（整数，与代码版本解耦；客户端与此不一致时必须升级服务端）
PROTOCOL_VERSION = 1

#: 目录/文件名（base dir 自身的位置由调用方通过 --base-dir 决定，
#: 基座不持有"自己应该在哪"的默认值）
LOCK_FILENAME = "server.lock"
INFO_FILENAME = "server.info"
CHILDREN_FILENAME = "children.json"
LOG_FILENAME = "server.log"
CODE_DIRNAME = "server"
PLUGINS_DIRNAME = "plugins"
VERSION_FILENAME = "version.txt"

#: 元能力前缀与保留插件名（避免与插件命名冲突）
META_PREFIX = "ssh_server_"
RESERVED_PLUGIN_NAME = "ssh_server"
API_MODULE_NAME = "ssh_server_api"

#: 能力归属标识：本阶段只有基座自身提供能力
CAPABILITY_OWNER = "ssh_server_base"
#: ``owner@capability`` 的分隔符；归属名与能力名都不得包含它
CAPABILITY_SEPARATOR = "@"

#: 基座能力接口版本表：**唯一事实来源**。
#:
#: 键为能力名，与 ``ssh_server_api`` 模块的属性名一一对应；值为该能力的接口版本。
#: 插件用模块级 ``REQUIRES`` 声明它用到的能力及其**精确版本**，基座加载时逐条比对，
#: 任一不匹配即 fail fast（整个服务启动失败）。
#:
#: 维护规则（对应 design.md §8.5）：
#:
#: * 只有**破坏接口兼容**的更新才迭代对应能力的版本号（判定表见 design.md §8.5）；
#: * 向后兼容的新增改动**不**迭代——否则所有声明旧版本的插件都会失效；
#: * 已发布的能力版本号唯一固定，不得回收复用。
#:
#: 已知边界：版本号只覆盖**接口表面**（有哪些能力、签名与返回结构），
#: 不承诺行为一致性；也不覆盖插件之间的业务耦合导致的运行时冲突（另见 design.md §8.1）。
CAPABILITY_VERSIONS = {
    "spawn": "0.1.0",
    "emit": "0.1.0",
    "log": "0.1.0",
    "config": "0.1.0",
    "capabilities": "0.1.0",
}

#: 默认参数（均可由命令行覆盖，见 Config）
DEFAULT_IDLE_TIMEOUT = 900          # 空闲退出秒数（15 分钟）
DEFAULT_THREADS = 4                 # 插件调用线程池大小
DEFAULT_MAX_CONCURRENT = 8          # 单连接最大并发请求数
DEFAULT_JOB_TIMEOUT = 1800          # job 模式单任务处理超时（秒）
DEFAULT_JOB_CACHE = 64              # 已完成任务结果保留条数（LRU）
DEFAULT_TRUNCATE_CAP = 256 * 1024   # job 模式单流输出上限（字节）
DEFAULT_HANDSHAKE_TIMEOUT = 10.0    # 握手超时（秒）
DEFAULT_CONN_IDLE_TIMEOUT = 3600.0  # 连接空闲超时（秒）

IDLE_CHECK_INTERVAL = 60.0          # 空闲检查周期（秒）
TERM_GRACE_SECONDS = 5.0            # 子进程 SIGTERM→SIGKILL 宽限（秒）

MAX_LINE_BYTES = 1024 * 1024        # 单行报文上限
MAX_EVENT_CHUNK = 64 * 1024         # 单次流式分块上限

#: 稳定错误码（客户端依赖这些字符串做分支，不得随意更名）
ERROR_BAD_REQUEST = "bad_request"
ERROR_BAD_TOKEN = "bad_token"
ERROR_UNKNOWN_TOOL = "unknown_tool"
ERROR_UNKNOWN_JOB = "unknown_job"
ERROR_BAD_PARAMS = "bad_params"
ERROR_TOOL_ERROR = "tool_error"
ERROR_BUSY = "busy"
ERROR_TIMEOUT = "timeout"
ERROR_INTERNAL = "internal"


# ---------------------------------------------------------------------------
# 异常
# ---------------------------------------------------------------------------

class SshServerError(Exception):
    """本项目所有自定义异常的基类。"""


class StartupError(SshServerError):
    """启动阶段失败（目录、锁、端口、插件等）。"""


class AlreadyRunning(StartupError):
    """flock 抢占失败：已有实例在运行。"""


class ProtocolError(SshServerError):
    """协议层错误（超长报文、非法握手等）。"""


class PluginLoadError(StartupError):
    """插件加载或校验失败。默认 fail fast，整个服务启动失败。"""


class RequestCancelled(SshServerError):
    """请求被取消（客户端断连、任务超时或显式取消）。

    插件在流式输出过程中触发取消时会收到此异常，应当尽快返回。
    """


class ChildTimeout(SshServerError):
    """spawn 的子进程超过 timeout 被终止。"""


# ---------------------------------------------------------------------------
# 通用工具
# ---------------------------------------------------------------------------

def ensure_private_dir(path, mode=0o700):
    """创建私有目录并强制权限（不受 umask 影响）。"""
    if not os.path.isdir(path):
        os.makedirs(path, mode=mode)
    try:
        os.chmod(path, mode)
    except OSError:
        # 某些文件系统（如挂载的 CIFS）不支持 chmod，忽略即可
        pass


def ensure_private_file(path, mode=0o600):
    """确保文件存在且权限为私有（不存在则以私有权限创建）。"""
    if not os.path.exists(path):
        fd = os.open(path, os.O_CREAT | os.O_WRONLY, mode)
        os.close(fd)
    try:
        os.chmod(path, mode)
    except OSError:
        pass


def write_bytes_atomic(path, data, mode=0o600):
    """原子写文件：同目录临时文件 → fsync → os.replace → fsync 目录。

    这样任何读者要么看到旧内容，要么看到新内容，绝不会读到写一半的文件。
    """
    directory = os.path.dirname(os.path.abspath(path))
    tmp_path = "%s.tmp.%d" % (path, os.getpid())
    fd = os.open(tmp_path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, mode)
    try:
        os.write(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp_path, path)
    # 目录 fsync：保证 rename 本身落盘（部分文件系统上非必需，但代价很小）
    try:
        dir_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError:
        pass


def write_json_atomic(path, payload, mode=0o600):
    """把 dict 序列化为紧凑 JSON 并原子写入。"""
    data = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    write_bytes_atomic(path, data, mode=mode)


def read_json_file(path):
    """读取 JSON 文件；文件不存在或损坏时返回 None（不抛异常）。"""
    try:
        with open(path, "rb") as fh:
            return json.loads(fh.read().decode("utf-8"))
    except (OSError, ValueError):
        return None


def read_version(code_dir):
    """读取 ``version.txt``；缺失时回退为 0.0.0（并允许调用方记录告警）。"""
    try:
        with open(os.path.join(code_dir, VERSION_FILENAME), "r", encoding="utf-8") as fh:
            text = fh.read().strip()
            return text or "0.0.0"
    except OSError:
        return "0.0.0"


def proc_stat(pid):
    """解析 ``/proc/<pid>/stat``，返回关心的字段。

    ``comm`` 字段可能包含空格与括号，因此先截断到最后一个 ``)``，
    之后按空白切分；切分结果下标 0 对应 stat 的第 3 个字段（state）。
    """
    try:
        with open("/proc/%d/stat" % pid, "rb") as fh:
            raw = fh.read().decode("utf-8", "replace")
    except OSError:
        return None
    right = raw.rfind(")")
    if right < 0:
        return None
    parts = raw[right + 2:].split()
    if len(parts) < 20:
        return None
    try:
        return {
            "state": parts[0],
            "ppid": int(parts[1]),
            "pgrp": int(parts[2]),
            "session": int(parts[3]),
            "starttime": int(parts[19]),  # stat 第 22 个字段
        }
    except ValueError:
        return None


def proc_starttime(pid):
    """返回进程启动时间（内核 jiffies）；不可用时返回 None。"""
    stat = proc_stat(pid)
    return None if stat is None else stat["starttime"]


def process_alive(pid):
    """判断进程是否存在（PermissionError 视为存在）。"""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False


def signal_process_group(pgid, sig):
    """向进程组发送信号；返回是否成功发出。"""
    try:
        os.killpg(pgid, sig)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return False


def wait_process_group_gone(pgid, timeout):
    """等待进程组消失，返回是否在 timeout 内消失。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return True
        except PermissionError:
            return False
        time.sleep(0.05)
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


def kill_process_group(pgid, grace=TERM_GRACE_SECONDS, logger=None):
    """先 SIGTERM、宽限后 SIGKILL 地终止一个进程组。"""
    if pgid <= 1:
        return False  # 保护：绝不操作 init 或非法 pgid
    if not signal_process_group(pgid, signal.SIGTERM):
        return False
    if wait_process_group_gone(pgid, grace):
        return True
    signal_process_group(pgid, signal.SIGKILL)
    wait_process_group_gone(pgid, 1.0)
    if logger is not None:
        logger.warning("进程组 %d 未在宽限期内退出，已 SIGKILL", pgid)
    return True


def scan_process_group_members(pgid, exclude_pid=None):
    """扫描 ``/proc`` 找出属于指定进程组的 pid（用于兜底清理）。

    基座自身以独立进程组运行，插件若绕过 ``spawn`` 直接使用 subprocess，
    其子进程会继承基座进程组；退出时通过本函数找到并清理它们。
    """
    members = []
    try:
        names = os.listdir("/proc")
    except OSError:
        return members
    for name in names:
        if not name.isdigit():
            continue
        pid = int(name)
        if exclude_pid is not None and pid == exclude_pid:
            continue
        stat = proc_stat(pid)
        if stat is not None and stat["pgrp"] == pgid:
            members.append(pid)
    return members


# ---------------------------------------------------------------------------
# 截断缓冲：job 模式输出上限，超出后保留头尾并插入占位符
# ---------------------------------------------------------------------------

class TruncatingBuffer(object):
    """有上限的字节缓冲，超出后保留头部与尾部各一半，中间以占位符代替。

    设计要点（对应 design.md §5.4）：

    * 头部一次性写满 ``cap // 2``；
    * 之后进入尾部滑动窗口，只保留最近 ``cap - cap//2`` 字节；
    * 被丢弃的字节数累加，渲染时插入 ``...<truncated N bytes>...``。
    """

    def __init__(self, cap):
        self._cap = max(0, int(cap))
        self._head_limit = self._cap // 2
        self._tail_limit = self._cap - self._head_limit
        self._head = bytearray()
        self._tail = collections.deque()
        self._tail_bytes = 0
        self._dropped = 0

    def append(self, data):
        """追加字节。"""
        if not data:
            return
        data = bytes(data)
        # 1) 先填满头部
        if len(self._head) < self._head_limit:
            take = min(len(data), self._head_limit - len(self._head))
            self._head += data[:take]
            data = data[take:]
        if not data:
            return
        # 2) 余下进入尾部滑动窗口
        self._tail.append(data)
        self._tail_bytes += len(data)
        while self._tail_bytes > self._tail_limit and self._tail:
            first = self._tail[0]
            overflow = self._tail_bytes - self._tail_limit
            if len(first) <= overflow:
                self._tail.popleft()
                self._tail_bytes -= len(first)
                self._dropped += len(first)
            else:
                self._tail[0] = first[overflow:]
                self._tail_bytes -= overflow
                self._dropped += overflow

    @property
    def truncated(self):
        """是否发生过截断。"""
        return self._dropped > 0

    @property
    def dropped(self):
        """被丢弃的字节数。"""
        return self._dropped

    def value(self):
        """渲染为字节串（含占位符）。"""
        out = bytes(self._head)
        if self._dropped:
            out += ("\n...<truncated %d bytes>...\n" % self._dropped).encode("utf-8")
        for chunk in self._tail:
            out += chunk
        return out

    def text(self, encoding="utf-8"):
        """渲染为文本（解码失败以替换字符兜底）。"""
        return self.value().decode(encoding, "replace")


# ---------------------------------------------------------------------------
# children.json：存活子进程记录 + 崩溃残留回收
# ---------------------------------------------------------------------------

class ChildrenRegistry(object):
    """记录当前存活的子进程，并在启动时回收上次崩溃留下的进程。

    文件格式::

        {"children": [{"pid":1,"pgid":1,"starttime":123,"label":"...","created_at":1.0}]}

    为什么要记录 ``starttime``：pid 会被内核复用，回收前必须确认
    「当前占用该 pid 的进程」与「上次记录的进程」是同一个，否则可能误杀无关进程。
    """

    def __init__(self, path, logger=None):
        self._path = path
        self._logger = logger
        self._lock = threading.Lock()
        self._entries = {}  # pid -> entry(dict)

    # -- 内部 ---------------------------------------------------------------
    def _flush_locked(self):
        payload = {"children": [self._entries[k] for k in sorted(self._entries)]}
        try:
            write_json_atomic(self._path, payload)
        except OSError as exc:
            if self._logger is not None:
                self._logger.warning("写入 children.json 失败：%s", exc)

    # -- 对外 ---------------------------------------------------------------
    def record(self, pid, pgid, label=""):
        """登记一个子进程。"""
        with self._lock:
            self._entries[pid] = {
                "pid": pid,
                "pgid": pgid,
                "starttime": proc_starttime(pid),
                "label": label,
                "created_at": time.time(),
            }
            self._flush_locked()

    def forget(self, pid):
        """注销一个子进程（进程已结束）。"""
        with self._lock:
            if self._entries.pop(pid, None) is not None:
                self._flush_locked()

    def clear(self):
        """清空记录（正常退出时调用）。"""
        with self._lock:
            self._entries = {}
            self._flush_locked()

    def entries(self):
        """返回当前记录的副本。"""
        with self._lock:
            return [dict(item) for item in self._entries.values()]

    def live_count(self):
        """当前登记且仍存活的子进程数量。"""
        with self._lock:
            entries = list(self._entries.values())
        return sum(1 for item in entries if process_alive(item["pid"]))

    def recover_stale(self):
        """回收上次崩溃残留的进程组，然后清空记录。

        只清理「pid 存活且 starttime 与记录一致」的进程组；不一致说明
        pid 已被复用，跳过以免误杀。返回被清理的进程组数量。
        """
        payload = read_json_file(self._path) or {}
        entries = payload.get("children") or []
        killed = 0
        own_pgid = os.getpgrp()
        for item in entries:
            try:
                pid = int(item.get("pid"))
                pgid = int(item.get("pgid") or pid)
                starttime = item.get("starttime")
            except (TypeError, ValueError):
                continue
            if pgid <= 1 or pgid == own_pgid:
                continue  # 绝不操作 init 或自身进程组
            if not process_alive(pid):
                continue
            if starttime is not None and proc_starttime(pid) != starttime:
                if self._logger is not None:
                    self._logger.warning("跳过残留 pid=%d：starttime 不匹配（pid 可能已复用）", pid)
                continue
            kill_process_group(pgid, logger=self._logger)
            killed += 1
        if entries and self._logger is not None:
            self._logger.info("启动清理：回收 %d 个残留进程组（共记录 %d 条）", killed, len(entries))
        self.clear()
        return killed

    def terminate_all(self, grace=TERM_GRACE_SECONDS):
        """退出时清理所有登记的子进程组。"""
        entries = self.entries()
        for item in entries:
            kill_process_group(int(item["pgid"]), grace=grace, logger=self._logger)
        self.clear()
        return len(entries)


# ---------------------------------------------------------------------------
# 子进程封装
# ---------------------------------------------------------------------------

class ChildProcess(object):
    """由基座 ``spawn`` 创建的子进程。

    关键约定：

    * 使用 ``start_new_session=True`` 创建，使子进程成为**独立进程组组长**，
      这样才能做到「请求级取消」「任务超时」时只杀该请求的进程组，
      而不会波及基座自身。
    * stdout/stderr 由后台线程读取并回调；回调阻塞（背压）时读取线程随之阻塞，
      OS 管道填满后子进程自然被挂起——这正是设计要求的背压行为。
    """

    def __init__(self, popen, registry, context, on_stdout, on_stderr, label=""):
        self._popen = popen
        self._registry = registry
        self._context = context
        self._label = label
        self._readers = []
        self._closed = False
        self._lock = threading.Lock()
        self.returncode = None
        self.timed_out = False
        self._start_reader(popen.stdout, on_stdout, "stdout")
        self._start_reader(popen.stderr, on_stderr, "stderr")

    # -- 属性 ---------------------------------------------------------------
    @property
    def pid(self):
        return self._popen.pid

    @property
    def pgid(self):
        # 因为以 start_new_session 启动，pgid == pid
        return self._popen.pid

    @property
    def stdin(self):
        """供插件写入的 stdin（仅当 spawn 时传入 subprocess.PIPE）。"""
        return self._popen.stdin

    # -- 内部 ---------------------------------------------------------------
    def _start_reader(self, pipe, callback, stream_name):
        if pipe is None:
            return
        fd = pipe.fileno()

        def loop():
            try:
                while True:
                    try:
                        chunk = os.read(fd, MAX_EVENT_CHUNK)
                    except OSError:
                        break
                    if not chunk:
                        break
                    if callback is not None:
                        try:
                            callback(chunk)
                        except RequestCancelled:
                            break          # 请求已取消，停止排空
                        except Exception:  # noqa: BLE001 - 回调异常不应杀死读取线程
                            if self._context is not None and self._context.logger is not None:
                                self._context.logger.warning("子进程 %s 回调异常", stream_name)
                            break
            finally:
                try:
                    pipe.close()
                except OSError:
                    pass

        thread = threading.Thread(
            target=loop,
            name="child-%s-%d" % (stream_name, self._popen.pid),
            daemon=True,
        )
        thread.start()
        self._readers.append(thread)

    def _close_pipes(self):
        for pipe in (self._popen.stdout, self._popen.stderr, self._popen.stdin):
            if pipe is None:
                continue
            try:
                pipe.close()
            except OSError:
                pass

    def _unregister(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self._registry.forget(self._popen.pid)
        if self._context is not None:
            self._context.unregister_child(self)

    # -- 对外 ---------------------------------------------------------------
    def wait(self, timeout=None):
        """等待子进程结束并返回退出码。

        读取线程会被 join，以保证返回时所有输出都已回调完毕（含背压场景）。
        """
        try:
            code = self._popen.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.terminate()
            raise ChildTimeout("子进程超过 %s 秒未结束" % timeout)
        self.returncode = code
        for thread in self._readers:
            thread.join()
        self._unregister()
        return code

    def poll(self):
        """非阻塞检查退出码。"""
        code = self._popen.poll()
        if code is not None:
            self.returncode = code
        return code

    def terminate(self, grace=TERM_GRACE_SECONDS):
        """终止整个子进程组（SIGTERM → 宽限 → SIGKILL）。"""
        if self._popen.poll() is None:
            kill_process_group(self.pgid, grace=grace,
                               logger=self._context.logger if self._context else None)
        self._close_pipes()
        return self.returncode


# ---------------------------------------------------------------------------
# 请求上下文：把「一次调用」需要的输出通道、子进程、取消信号绑在一起
# ---------------------------------------------------------------------------

class RequestContext(object):
    """单次请求的执行上下文。

    * ``mode == "stream"``：``emit`` 直接向连接推送 ``event``；
    * ``mode == "job"``：``emit`` 写入服务端截断缓冲，由轮询取回。

    上下文通过 ``threading.local`` 暴露给插件（``ssh_server_api.emit`` 等），
    因此插件不需要、也不允许自己管理请求状态。
    """

    def __init__(self, server, conn, request_id, mode, job=None):
        self.server = server
        self.conn = conn
        self.request_id = request_id
        self.mode = mode
        self.job = job
        self.logger = server.logger if server is not None else None
        self._children = []
        self._children_lock = threading.Lock()
        self._cancelled = False
        self._cancel_reason = None
        self._cancel_lock = threading.Lock()
        self._seq = 0
        self._emit_lock = threading.Lock()
        cap = server.config.truncate_cap if server is not None else DEFAULT_TRUNCATE_CAP
        self.stdout_buffer = TruncatingBuffer(cap)
        self.stderr_buffer = TruncatingBuffer(cap)
        #: 文本流的增量解码器，避免 64 KiB 分块切断多字节 UTF-8 字符
        self._decoders = {
            "stdout": codecs.getincrementaldecoder("utf-8")("replace"),
            "stderr": codecs.getincrementaldecoder("utf-8")("replace"),
        }

    # -- 取消 ---------------------------------------------------------------
    @property
    def cancelled(self):
        return self._cancelled

    @property
    def cancel_reason(self):
        return self._cancel_reason

    def cancel(self, reason):
        """取消请求：终止其全部子进程，并让后续 emit 抛 RequestCancelled。"""
        with self._cancel_lock:
            if self._cancelled:
                return
            self._cancelled = True
            self._cancel_reason = reason
        with self._children_lock:
            children = list(self._children)
        for child in children:
            try:
                child.terminate()
            except Exception:  # noqa: BLE001 - 清理路径不应抛异常
                pass
        if self.logger is not None:
            self.logger.info("请求已取消：id=%s mode=%s reason=%s",
                             self.request_id, self.mode, reason)

    # -- 子进程登记 ---------------------------------------------------------
    def register_child(self, child):
        with self._children_lock:
            self._children.append(child)

    def unregister_child(self, child):
        with self._children_lock:
            try:
                self._children.remove(child)
            except ValueError:
                pass

    def live_children(self):
        with self._children_lock:
            return [c for c in self._children if c.poll() is None]

    def cleanup(self):
        """请求结束时的兜底清理：确保没有遗留子进程。"""
        with self._children_lock:
            children = list(self._children)
        for child in children:
            if child.poll() is None:
                child.terminate()

    # -- 输出 ---------------------------------------------------------------
    def _buffer_for(self, stream):
        return self.stdout_buffer if stream == "stdout" else self.stderr_buffer

    def emit(self, stream, data, encoding="utf-8"):
        """向当前请求输出数据。

        参数语义（对应 design.md §5.4）：

        * ``data`` 为 ``str``：文本流，按 ``encoding`` 编码；
        * ``data`` 为 ``bytes`` 且 ``encoding == "utf-8"``：文本流（子进程输出）；
        * ``data`` 为 ``bytes`` 且 ``encoding == "base64"``：二进制流，

        事件会带上 ``"encoding": "base64"``。
        """
        if stream not in ("stdout", "stderr"):
            raise ValueError("未知输出流：%r" % (stream,))
        if self._cancelled:
            raise RequestCancelled("请求已取消（%s）" % self._cancel_reason)
        if isinstance(data, str):
            payload = data.encode(encoding if encoding != "base64" else "utf-8")
            binary = encoding == "base64"
        elif isinstance(data, (bytes, bytearray)):
            payload = bytes(data)
            binary = encoding == "base64"
        else:
            raise TypeError("emit 只接受 str 或 bytes，收到 %r" % type(data).__name__)

        if self.mode == "stream":
            self._emit_stream(stream, payload, binary)
        else:
            self._buffer_for(stream).append(payload)

    def _emit_stream(self, stream, payload, binary):
        """stream 模式：按 MAX_EVENT_CHUNK 分块推送事件，seq 严格递增。"""
        if not payload:
            return
        conn = self.conn
        if conn is None or conn.closed:
            raise RequestCancelled("连接已关闭")
        if binary:
            text = base64.b64encode(payload).decode("ascii")
            chunks = [text[i:i + MAX_EVENT_CHUNK] for i in range(0, len(text), MAX_EVENT_CHUNK)]
            encoding = "base64"
        else:
            # 增量解码，保证跨分块的多字节字符不被破坏
            text = self._decoders[stream].decode(payload)
            if not text:
                return
            chunks = [text[i:i + MAX_EVENT_CHUNK] for i in range(0, len(text), MAX_EVENT_CHUNK)]
            encoding = "utf-8"
        with self._emit_lock:
            for chunk in chunks:
                seq = self._seq
                self._seq += 1
                conn.send_json({
                    "type": "event",
                    "id": self.request_id,
                    "stream": stream,
                    "seq": seq,
                    "encoding": encoding,
                    "data": chunk,
                })

    def flush_decoders(self):
        """请求结束时冲掉增量解码器中残留的字节。"""
        if self.mode != "stream" or self.conn is None or self.conn.closed:
            return
        for stream in ("stdout", "stderr"):
            tail = self._decoders[stream].decode(b"", final=True)
            if tail:
                try:
                    self.emit(stream, tail)
                except RequestCancelled:
                    return

    # -- 渲染（job 轮询用） -------------------------------------------------
    def render_output(self):
        """返回 (stdout, stderr, truncated) 三元组，供 job 轮询使用。"""
        return (
            self.stdout_buffer.text(),
            self.stderr_buffer.text(),
            self.stdout_buffer.truncated or self.stderr_buffer.truncated,
        )


# ---------------------------------------------------------------------------
# 任务（job）
# ---------------------------------------------------------------------------

class Job(object):
    """job 模式下的一个后台任务。

    状态机：``running`` → ``succeeded`` / ``failed`` / ``timeout`` / ``cancelled``。
    结果只存内存（LRU 上限），服务退出即丢失。
    """

    def __init__(self, job_id, tool, cap):
        self.job_id = job_id
        self.tool = tool
        self.state = "running"
        self.result = None
        self.exit_code = None
        self.error = None
        self.created_at = time.time()
        self.finished_at = None
        self.cancel_reason = None
        self.stdout_buffer = TruncatingBuffer(cap)
        self.stderr_buffer = TruncatingBuffer(cap)
        self.context = None
        self._lock = threading.Lock()

    def mark_cancel(self, reason):
        """标记取消/超时；已结束的任务不改变状态。"""
        with self._lock:
            if self.state != "running":
                return False
            self.cancel_reason = reason
            self.state = "timeout" if reason == "timeout" else "cancelled"
            self.finished_at = time.time()
            return True

    def finish(self, state, result=None, error=None):
        """结束任务并写入结果。"""
        with self._lock:
            if self.state == "running":
                self.state = state
                self.result = result
                self.error = error
                self.finished_at = time.time()
            if isinstance(result, dict) and "exit_code" in result:
                self.exit_code = result["exit_code"]

    def snapshot(self):
        """序列化为可 JSON 传输的 dict。"""
        with self._lock:
            state = self.state
            payload = {
                "job_id": self.job_id,
                "tool": self.tool,
                "state": state,
                "exit_code": self.exit_code,
                "result": self.result,
                "error": self.error,
                "stdout": self.stdout_buffer.text(),
                "stderr": self.stderr_buffer.text(),
                "truncated": self.stdout_buffer.truncated or self.stderr_buffer.truncated,
                "created_at": self.created_at,
                "finished_at": self.finished_at,
            }
        return payload


class JobManager(object):
    """任务的创建、查询、取消与 LRU 淘汰。"""

    def __init__(self, server, config):
        self._server = server
        self._config = config
        self._lock = threading.Lock()
        self._jobs = collections.OrderedDict()

    def create(self, tool):
        """创建并登记一个任务。"""
        job_id = os.urandom(8).hex()
        job = Job(job_id, tool, self._config.truncate_cap)
        with self._lock:
            self._jobs[job_id] = job
        return job

    def get(self, job_id):
        with self._lock:
            return self._jobs.get(job_id)

    def running_count(self):
        with self._lock:
            return sum(1 for job in self._jobs.values() if job.state == "running")

    def cancel(self, job_id):
        """取消任务：标记状态并终止其子进程。"""
        job = self.get(job_id)
        if job is None:
            return None
        if job.mark_cancel("cancelled") and job.context is not None:
            job.context.cancel("cancelled")
        return job

    def cancel_all(self):
        """退出时取消所有运行中的任务。"""
        with self._lock:
            jobs = list(self._jobs.values())
        for job in jobs:
            if job.state == "running":
                job.mark_cancel("shutdown")
                if job.context is not None:
                    job.context.cancel("shutdown")

    def _evict_locked(self):
        """按插入顺序淘汰已结束的任务，保证不超过 job_cache。"""
        finished = [jid for jid, job in self._jobs.items() if job.state != "running"]
        while len(finished) > self._config.job_cache:
            victim = finished.pop(0)
            self._jobs.pop(victim, None)

    def finish(self, job, state, result=None, error=None):
        job.finish(state, result=result, error=error)
        with self._lock:
            self._jobs.move_to_end(job.job_id)
            self._evict_locked()


# ---------------------------------------------------------------------------
# 插件管理
# ---------------------------------------------------------------------------

def _render_capabilities(table):
    """把能力版本表渲染成稳定、可读的一行（用于启动失败的诊断消息）。

    fail fast 时客户端连不上、``server.info`` 也不会被写，错误串是唯一的诊断入口，
    因此必须自带「基座本地提供什么」。
    """
    if not table:
        return "(空)"
    return ", ".join("%s=%s" % (name, table[name]) for name in sorted(table))


class ToolEntry(object):
    """一个已加载能力的描述。"""

    __slots__ = ("name", "plugin", "fn", "description")

    def __init__(self, name, plugin, fn, description):
        self.name = name
        self.plugin = plugin
        self.fn = fn
        self.description = description


class PluginManager(object):
    """扫描并加载 ``plugins/*.py``，校验能力表。

    约定（对应 design.md §8）：

    * 插件名 = 文件名 stem；能力名必须是 ``pluginname_toolname``；
    * ``ssh_server`` 为保留插件名，``ssh_server_`` 为保留前缀；
    * 以 ``_`` 开头的文件被视为辅助模块，跳过；
    * 插件用模块级 ``REQUIRES`` 声明它用到的基座能力及其**精确版本**（见 §8.5）；
    * 任何校验失败都 fail fast。
    """

    def __init__(self, config, logger):
        self._config = config
        self._logger = logger

    def _import_module(self, path, stem):
        module_name = "ssh_server_plugin_%s" % stem
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise PluginLoadError("无法为插件 %s 创建 import spec" % path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        except Exception as exc:  # noqa: BLE001 - 任意导入期异常都视为插件错误
            raise PluginLoadError("插件 %s 导入失败：%s\n%s" % (stem, exc, traceback.format_exc()))
        return module

    def _check_capabilities(self, module, stem):
        """校验插件声明的基座能力接口版本（对应 design.md §8.5）。

        语义是**精确匹配**：插件声明的版本必须与基座提供的版本逐一相等，
        基座不承诺「新版能力跑旧插件」。

        只有 ``owner`` 为 ``ssh_server_base`` 的声明会被校验——其余归属（预留给
        插件间能力依赖）本阶段既不校验也不报错，由上位软件在部署前自行解决。
        """
        requires = getattr(module, "REQUIRES", None)
        if requires is None:
            return
        if not isinstance(requires, dict):
            raise PluginLoadError(
                "插件 %s 的 REQUIRES 必须是 dict（{能力ID: 精确版本}），收到 %s"
                % (stem, type(requires).__name__))
        available = self._base_capabilities()
        for capability_id, required in sorted(requires.items()):
            if not isinstance(capability_id, str) or not capability_id:
                raise PluginLoadError(
                    "插件 %s 的 REQUIRES 键必须是非空字符串（形如 %s@能力名），收到 %r"
                    % (stem, CAPABILITY_OWNER, capability_id))
            if not isinstance(required, str) or not required:
                raise PluginLoadError(
                    "插件 %s 声明的能力 %s 版本必须是非空字符串，收到 %r"
                    % (stem, capability_id, required))
            owner, _, name = capability_id.partition(CAPABILITY_SEPARATOR)
            if not name or not owner:
                raise PluginLoadError(
                    "插件 %s 声明的能力 ID %r 格式非法：应为 %s@能力名"
                    % (stem, capability_id, CAPABILITY_OWNER))
            if owner != CAPABILITY_OWNER:
                # 插件间能力依赖本阶段不支持：忽略，由上位软件在部署前解决。
                self._logger.debug("插件 %s 声明了非基座能力 %s，本阶段不校验",
                                   stem, capability_id)
                continue
            provided = available.get(name)
            if provided is None:
                raise PluginLoadError(
                    "插件 %s 能力版本校验失败：能力 %s 不存在于基座能力版本表；"
                    "基座本地提供 %s" % (stem, capability_id, _render_capabilities(available)))
            if provided != required:
                raise PluginLoadError(
                    "插件 %s 能力版本校验失败：能力 %s 要求版本 %s，基座提供 %s；"
                    "基座本地提供 %s"
                    % (stem, capability_id, required, provided,
                       _render_capabilities(available)))

    @staticmethod
    def _base_capabilities():
        """返回注入给插件的 ``ssh_server_api`` 模块所声明的能力版本表。"""
        module = sys.modules.get(API_MODULE_NAME)
        table = getattr(module, "capabilities", None) if module is not None else None
        if isinstance(table, dict) and table:
            return dict(table)
        return dict(CAPABILITY_VERSIONS)

    def load(self):
        """加载全部插件并返回 ``{name: ToolEntry}``。"""
        plugins_dir = self._config.plugins_dir
        entries = {}
        if not os.path.isdir(plugins_dir):
            self._logger.info("插件目录不存在，跳过加载：%s", plugins_dir)
            return entries
        for filename in sorted(os.listdir(plugins_dir)):
            if not filename.endswith(".py") or filename.startswith("_"):
                continue
            stem = filename[:-3]
            path = os.path.join(plugins_dir, filename)
            if not stem.isidentifier():
                raise PluginLoadError("插件文件名不是合法标识符：%s" % filename)
            if stem == RESERVED_PLUGIN_NAME:
                raise PluginLoadError("插件名 %r 为基座保留，禁止使用" % stem)
            module = self._import_module(path, stem)
            registry = getattr(module, "registry", None)
            if not callable(registry):
                raise PluginLoadError("插件 %s 未提供可调用的 registry(tools, description)" % stem)
            # 能力版本校验先于 registry()：版本不匹配时没有必要再执行插件代码。
            self._check_capabilities(module, stem)

            local_tools = {}
            local_desc = {}
            try:
                registry(local_tools, local_desc)
            except Exception as exc:  # noqa: BLE001
                raise PluginLoadError("插件 %s 的 registry() 执行失败：%s\n%s"
                                      % (stem, exc, traceback.format_exc()))
            if not isinstance(local_tools, dict) or not isinstance(local_desc, dict):
                # registry 只能填充传入的 dict，不得替换
                raise PluginLoadError("插件 %s 的 registry() 不应替换传入的 dict" % stem)

            for name, fn in local_tools.items():
                if not isinstance(name, str) or not name.startswith(stem + "_"):
                    raise PluginLoadError(
                        "插件 %s 的能力名 %r 非法：必须为 %s_toolname 形式" % (stem, name, stem))
                if name.startswith(META_PREFIX):
                    raise PluginLoadError("能力名 %r 使用了基座保留前缀 %r" % (name, META_PREFIX))
                if name in entries:
                    raise PluginLoadError("能力名重复：%s" % name)
                if not callable(fn):
                    raise PluginLoadError("能力 %s 对应的对象不可调用" % name)
                desc = local_desc.get(name) or {}
                if not isinstance(desc, dict):
                    raise PluginLoadError("能力 %s 的 description 必须是 dict" % name)
                parameters = desc.get("parameters") or {"type": "object"}
                if not isinstance(parameters, dict):
                    raise PluginLoadError("能力 %s 的 parameters 必须是 JSON Schema dict" % name)
                entries[name] = ToolEntry(
                    name=name,
                    plugin=stem,
                    fn=fn,
                    description={
                        "description": str(desc.get("description", "")),
                        "parameters": parameters,
                    },
                )
            orphans = [key for key in local_desc if key not in local_tools]
            if orphans:
                raise PluginLoadError("插件 %s 提供了未注册能力的能力描述：%s" % (stem, sorted(orphans)))
            self._logger.info("已加载插件 %s（%d 个能力）", stem,
                              sum(1 for n in entries if entries[n].plugin == stem))
        return entries


def build_api_module(server):
    """构造并注册注入给插件的 ``ssh_server_api`` 模块。

    插件函数只接收一个参数 dict，因此需要子进程/日志/流式输出时，
    统一通过该模块访问基座基础设施。模块是运行时构造的，不新增文件。

    模块同时承载**能力接口版本表**（``capabilities``）：插件的 ``REQUIRES``
    与它比对（见 ``PluginManager._check_capabilities``），插件也可用它做自查。
    """
    module = types.ModuleType(API_MODULE_NAME)
    module.__doc__ = "ssh-outpost 基座注入给插件的基础设施 API（非基座实现文件）。"

    def spawn(argv, cwd=None, env=None, stdin=None,
              on_stdout=None, on_stderr=None, timeout=None):
        """创建子进程；唯一受支持的子进程入口。"""
        context = server.current_context()
        if context is None:
            raise RuntimeError("spawn 只能在插件工具执行期间调用")
        return server.spawn_child(context, argv, cwd=cwd, env=env, stdin=stdin,
                                  on_stdout=on_stdout, on_stderr=on_stderr,
                                  timeout=timeout)

    def emit(stream, data, encoding="utf-8"):
        """向当前请求输出数据（stream 模式即时推送，job 模式累积）。"""
        context = server.current_context()
        if context is None:
            raise RuntimeError("emit 只能在插件工具执行期间调用")
        return context.emit(stream, data, encoding=encoding)

    def log(level, message):
        """写日志。仅用于错误与生命周期，不要用于逐请求审计。"""
        server.plugin_log(level, message)

    module.spawn = spawn
    module.emit = emit
    module.log = log
    module.config = types.SimpleNamespace(
        idle_timeout=server.config.idle_timeout,
        version=server.version,
        platform=sys.platform,
        base_dir=server.config.base_dir,
    )
    # 只读视图：插件不得篡改基座的能力版本表。
    module.capabilities = types.MappingProxyType(dict(CAPABILITY_VERSIONS))
    sys.modules[API_MODULE_NAME] = module
    return module


# ---------------------------------------------------------------------------
# 协议：行读取器
# ---------------------------------------------------------------------------

class LineReader(object):
    """从 socket 上按 ``\\n`` 读取一行（NDJSON），带单行长度上限。"""

    def __init__(self, sock, max_bytes=MAX_LINE_BYTES):
        self._sock = sock
        self._buf = bytearray()
        self._max = max_bytes

    def readline(self):
        """返回不含换行符的一行字节；EOF 返回 None。"""
        while True:
            index = self._buf.find(b"\n")
            if index >= 0:
                line = bytes(self._buf[:index])
                del self._buf[:index + 1]
                return line
            if len(self._buf) > self._max:
                raise ProtocolError("单行报文超过上限 %d 字节" % self._max)
            chunk = self._sock.recv(65536)
            if not chunk:
                if self._buf:
                    line = bytes(self._buf)
                    self._buf = bytearray()
                    return line
                return None
            self._buf += chunk


# ---------------------------------------------------------------------------
# 连接处理
# ---------------------------------------------------------------------------

class Connection(threading.Thread):
    """一个客户端连接：握手、读循环、请求分发。

    并发模型（对应 design.md §5.1）：

    * 每连接一个读循环线程（本线程）；
    * 插件调用交给基座线程池；池满返回 ``busy``；
    * 单连接同时未完成的 stream 请求数受 ``max_concurrent`` 限制；
    * job 请求提交后立即回执，不占用连接并发额度。
    """

    def __init__(self, server, sock, addr):
        threading.Thread.__init__(self, name="conn-%s:%s" % addr, daemon=True)
        self.server = server
        self.sock = sock
        self.addr = addr
        self.reader = LineReader(sock)
        self.closed = False
        self.authenticated = False
        self._send_lock = threading.Lock()
        self._contexts = {}
        self._contexts_lock = threading.Lock()
        self._active = 0
        self._active_lock = threading.Lock()

    # -- 发送 ---------------------------------------------------------------
    def send_json(self, payload):
        """线程安全地发送一行 JSON；发送缓冲满时阻塞（背压）。"""
        if self.closed:
            raise RequestCancelled("连接已关闭")
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8") + b"\n"
        with self._send_lock:
            try:
                self.sock.sendall(data)
            except OSError:
                self.closed = True
                raise RequestCancelled("连接写入失败")

    def send_error(self, request_id, code, message, detail=None):
        error = {"code": code, "message": message}
        if detail:
            error["detail"] = detail
        self.send_json({"type": "result", "id": request_id, "ok": False, "error": error})

    # -- 生命周期 -----------------------------------------------------------
    def close(self):
        if self.closed:
            return
        self.closed = True
        # 取消本连接发起的所有 stream 请求（job 不受断连影响）
        with self._contexts_lock:
            contexts = list(self._contexts.values())
            self._contexts.clear()
        for context in contexts:
            context.cancel("disconnect")
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass
        self.server.unregister_connection(self)

    def run(self):
        try:
            self.sock.settimeout(self.server.config.handshake_timeout)
            if not self._handshake():
                return
            self.sock.settimeout(self.server.config.conn_idle_timeout)
            while not self.server.stop_requested:
                try:
                    line = self.reader.readline()
                except socket.timeout:
                    # 连接空闲超时：仅当本连接确实没有在途 stream 请求时才断开。
                    # 否则长时间运行的任务会被这条超时误杀，与「有在途请求绝不打断执行」冲突。
                    if self._active > 0:
                        continue
                    break
                if line is None:
                    break
                if not line.strip():
                    continue
                try:
                    message = json.loads(line.decode("utf-8"))
                except ValueError:
                    self.send_error(None, ERROR_BAD_REQUEST, "报文不是合法 JSON")
                    break
                self._handle_message(message)
        except socket.timeout:
            pass
        except ProtocolError:
            pass
        except (OSError, RequestCancelled):
            pass
        finally:
            self.close()

    # -- 握手 ---------------------------------------------------------------
    def _handshake(self):
        line = self.reader.readline()
        if line is None:
            return False
        try:
            message = json.loads(line.decode("utf-8"))
        except ValueError:
            self.send_json({"type": "hello_err", "error": ERROR_BAD_REQUEST})
            return False
        if not isinstance(message, dict) or message.get("type") != "hello":
            self.send_json({"type": "hello_err", "error": ERROR_BAD_REQUEST})
            return False
        if message.get("protocol") != PROTOCOL_VERSION:
            self.send_json({"type": "hello_err", "error": "protocol_mismatch",
                            "expected": PROTOCOL_VERSION})
            return False
        token = message.get("token")
        if not isinstance(token, str) or not hmac.compare_digest(token, self.server.token):
            self.send_json({"type": "hello_err", "error": ERROR_BAD_TOKEN})
            self.server.logger.warning("来自 %s 的握手 token 校验失败", self.addr)
            return False
        self.authenticated = True
        self.send_json({
            "type": "hello_ok",
            "protocol": PROTOCOL_VERSION,
            "version": self.server.version,
            "pid": os.getpid(),
        })
        return True

    # -- 分发 ---------------------------------------------------------------
    def _handle_message(self, message):
        self.server.touch()
        if not isinstance(message, dict) or message.get("type") != "call":
            self.send_error(None, ERROR_BAD_REQUEST, "只接受 type=call 的报文")
            return
        request_id = message.get("id")
        if request_id is None or not isinstance(request_id, (int, str)):
            self.send_error(None, ERROR_BAD_REQUEST, "call 缺少合法的 id")
            return
        tool = message.get("tool")
        if not isinstance(tool, str) or not tool:
            self.send_error(request_id, ERROR_BAD_REQUEST, "call 缺少 tool")
            return
        params = message.get("params", {})
        if params is None:
            params = {}
        if not isinstance(params, dict):
            self.send_error(request_id, ERROR_BAD_PARAMS, "params 必须是 dict")
            return
        mode = message.get("mode", "stream")
        if mode not in ("stream", "job"):
            self.send_error(request_id, ERROR_BAD_REQUEST, "mode 只能是 stream 或 job")
            return

        # 元能力：始终在连接线程内联执行（快速、不受线程池饱和影响）
        handler = META_HANDLERS.get(tool)
        if handler is not None:
            try:
                result = handler(self, request_id, params)
            except SshServerError as exc:
                self.send_error(request_id, getattr(exc, "code", ERROR_INTERNAL), str(exc))
            except Exception as exc:  # noqa: BLE001
                self.send_error(request_id, ERROR_INTERNAL, str(exc),
                                traceback.format_exc() if self.server.config.debug else None)
            else:
                if result is not None:
                    self.send_json({"type": "result", "id": request_id, "ok": True, "result": result})
            return

        # 插件能力
        entry = self.server.plugins.get(tool)
        if entry is None:
            self.send_error(request_id, ERROR_UNKNOWN_TOOL, "未知能力：%s" % tool)
            return
        if mode == "job":
            self._dispatch_job(request_id, entry, params)
        else:
            self._dispatch_stream(request_id, entry, params)

    # -- stream 模式 --------------------------------------------------------
    def _dispatch_stream(self, request_id, entry, params):
        with self._active_lock:
            if self._active >= self.server.config.max_concurrent:
                self.send_error(request_id, ERROR_BUSY, "单连接并发请求已达上限")
                return
            self._active += 1
        if not self.server.begin_request():
            with self._active_lock:
                self._active -= 1
            self.send_error(request_id, ERROR_BUSY, "服务端线程池已满")
            return
        context = RequestContext(self.server, self, request_id, "stream")
        with self._contexts_lock:
            self._contexts[request_id] = context
        try:
            self.send_json({"type": "ack", "id": request_id})
        except RequestCancelled:
            self._finish_stream(request_id, context)
            self.server.end_request()
            return
        self.server.submit(self._execute_stream, context, entry, params)

    def _execute_stream(self, context, entry, params):
        """线程池中执行一次 stream 调用。"""
        request_id = context.request_id
        try:
            with self.server.api_context(context):
                result = entry.fn(params)
            context.flush_decoders()
            if not context.cancelled and not self.closed:
                self.send_json({"type": "result", "id": request_id, "ok": True, "result": result})
        except RequestCancelled:
            pass  # 客户端断连或被取消，不再回包
        except Exception as exc:  # noqa: BLE001 - 插件异常统一封装
            if not self.closed:
                payload = self.server.error_payload(exc)
                try:
                    self.send_json({"type": "result", "id": request_id, "ok": False, "error": payload})
                except RequestCancelled:
                    pass
        finally:
            context.cleanup()
            self._finish_stream(request_id, context)
            self.server.end_request()

    def _finish_stream(self, request_id, context):
        with self._contexts_lock:
            self._contexts.pop(request_id, None)
        with self._active_lock:
            if self._active > 0:
                self._active -= 1

    # -- job 模式 -----------------------------------------------------------
    def _dispatch_job(self, request_id, entry, params):
        if not self.server.begin_request():
            self.send_error(request_id, ERROR_BUSY, "服务端线程池已满")
            return
        job = self.server.jobs.create(entry.name)
        self.server.submit(self.server.run_job, job, entry.fn, params)
        try:
            self.send_json({
                "type": "result", "id": request_id, "ok": True,
                "result": {"job_id": job.job_id, "state": "running"},
            })
        except RequestCancelled:
            self.server.jobs.cancel(job.job_id)
            self.server.end_request()


# ---------------------------------------------------------------------------
# 元能力实现（注意：均返回可 JSON 序列化的结果）
# ---------------------------------------------------------------------------

def _meta_ping(conn, request_id, params):
    """存活/版本探测；同时刷新 last_active_time（心跳）。"""
    server = conn.server
    return {
        "ok": True,
        "version": server.version,
        "protocol": PROTOCOL_VERSION,
        "pid": os.getpid(),
        "uptime": round(time.monotonic() - server.started_monotonic, 3),
        "active_requests": server.active_count(),
    }


def _meta_info(conn, request_id, params):
    """诊断信息。

    额外返回基座的能力接口版本表：fail fast 时客户端连不上、只能看启动日志；
    服务起来了则可以用这里对照插件 ``REQUIRES``，定位版本不匹配的插件。
    """
    server = conn.server
    return {
        "version": server.version,
        "protocol": PROTOCOL_VERSION,
        "python": sys.executable,
        "platform": sys.platform,
        "started_at": server.started_at,
        "idle_timeout": server.config.idle_timeout,
        "capabilities": dict(CAPABILITY_VERSIONS),
    }


def _meta_list_tools(conn, request_id, params):
    """能力发现：供客户端生成 Agent 工具定义。"""
    server = conn.server
    tools = []
    for name in sorted(server.plugins):
        entry = server.plugins[name]
        tools.append({
            "name": entry.name,
            "description": entry.description["description"],
            "parameters": entry.description["parameters"],
        })
    return {"tools": tools}


def _meta_shutdown(conn, request_id, params):
    """优雅关闭；先回执，再异步触发停止。"""
    server = conn.server
    reason = params.get("reason") if isinstance(params, dict) else None

    def delayed():
        time.sleep(0.05)  # 给回执留出 flush 时间
        server.request_stop("shutdown:%s" % (reason or "-"))

    threading.Thread(target=delayed, name="shutdown-trigger", daemon=True).start()
    return {"ok": True, "reason": reason}


def _meta_job_get(conn, request_id, params):
    """轮询任务状态与累计输出。"""
    job_id = params.get("job_id")
    if not isinstance(job_id, str) or not job_id:
        raise _BadParams("job_id 必须是非空字符串")
    job = conn.server.jobs.get(job_id)
    if job is None:
        raise _UnknownJob(job_id)
    return job.snapshot()


def _meta_job_cancel(conn, request_id, params):
    """取消任务并终止其子进程。"""
    job_id = params.get("job_id")
    if not isinstance(job_id, str) or not job_id:
        raise _BadParams("job_id 必须是非空字符串")
    job = conn.server.jobs.cancel(job_id)
    if job is None:
        raise _UnknownJob(job_id)
    return {"ok": True, "job_id": job_id, "state": job.state}


class _BadParams(SshServerError):
    """元能力参数错误（内部使用，携带稳定错误码）。"""

    code = ERROR_BAD_PARAMS


class _UnknownJob(SshServerError):
    """任务不存在（内部使用，携带稳定错误码）。"""

    code = ERROR_UNKNOWN_JOB


META_HANDLERS = {
    META_PREFIX + "ping": _meta_ping,
    META_PREFIX + "info": _meta_info,
    META_PREFIX + "list_tools": _meta_list_tools,
    META_PREFIX + "shutdown": _meta_shutdown,
    META_PREFIX + "job_get": _meta_job_get,
    META_PREFIX + "job_cancel": _meta_job_cancel,
}


# ---------------------------------------------------------------------------
# 简易守护线程池（相比 ThreadPoolExecutor，线程均为 daemon，不会拖住进程退出）
# ---------------------------------------------------------------------------

class WorkerPool(object):
    """固定大小的守护线程池。"""

    def __init__(self, size, logger=None, name="worker"):
        self._logger = logger
        self._queue = queue.Queue()
        self._threads = []
        self._closed = False
        for index in range(max(1, int(size))):
            thread = threading.Thread(target=self._loop, name="%s-%d" % (name, index), daemon=True)
            thread.start()
            self._threads.append(thread)

    def _loop(self):
        while True:
            item = self._queue.get()
            if item is None:
                return
            func, args, kwargs = item
            try:
                func(*args, **kwargs)
            except Exception:  # noqa: BLE001 - 工作线程绝不能因异常退出
                if self._logger is not None:
                    self._logger.error("工作线程执行异常：\n%s", traceback.format_exc())

    def submit(self, func, *args, **kwargs):
        if self._closed:
            raise RuntimeError("线程池已关闭")
        self._queue.put((func, args, kwargs))

    def shutdown(self):
        if self._closed:
            return
        self._closed = True
        for _ in self._threads:
            self._queue.put(None)


# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------

class Config(object):
    """运行配置。所有默认值对应 design.md §4.9 的启动参数表。"""

    def __init__(self, base_dir=None, idle_timeout=DEFAULT_IDLE_TIMEOUT,
                 threads=DEFAULT_THREADS, max_concurrent=DEFAULT_MAX_CONCURRENT,
                 job_timeout=DEFAULT_JOB_TIMEOUT, job_cache=DEFAULT_JOB_CACHE,
                 truncate_cap=DEFAULT_TRUNCATE_CAP, log_path=None, debug=False,
                 handshake_timeout=DEFAULT_HANDSHAKE_TIMEOUT,
                 conn_idle_timeout=DEFAULT_CONN_IDLE_TIMEOUT,
                 idle_check_interval=IDLE_CHECK_INTERVAL,
                 enable_log=True, announce=True):
        if not base_dir:
            raise ValueError("base_dir 必填：运行目录的位置由调用方决定，基座不提供默认值")
        self.base_dir = os.path.abspath(os.path.expanduser(base_dir))
        self.code_dir = os.path.join(self.base_dir, CODE_DIRNAME)
        self.plugins_dir = os.path.join(self.code_dir, PLUGINS_DIRNAME)
        self.lock_path = os.path.join(self.base_dir, LOCK_FILENAME)
        self.info_path = os.path.join(self.base_dir, INFO_FILENAME)
        self.children_path = os.path.join(self.base_dir, CHILDREN_FILENAME)
        self.log_path = log_path or os.path.join(self.base_dir, LOG_FILENAME)
        self.enable_log = enable_log
        self.announce = announce
        self.idle_timeout = idle_timeout
        self.threads = threads
        self.max_concurrent = max_concurrent
        self.job_timeout = job_timeout
        self.job_cache = job_cache
        self.truncate_cap = truncate_cap
        self.debug = debug
        self.handshake_timeout = handshake_timeout
        self.conn_idle_timeout = conn_idle_timeout
        self.idle_check_interval = idle_check_interval

    @classmethod
    def from_args(cls, argv):
        """解析命令行参数。"""
        parser = argparse.ArgumentParser(
            prog="ssh_server.py",
            description="ssh-outpost 基座：极简远程执行服务端（仅基础设施，能力由插件提供）")
        parser.add_argument("--base-dir", required=True,
                            help="运行目录（必填；位置由调用方决定，基座不提供默认值）")
        parser.add_argument("--idle-timeout", type=float, default=DEFAULT_IDLE_TIMEOUT,
                            help="空闲退出秒数，默认 %d" % DEFAULT_IDLE_TIMEOUT)
        parser.add_argument("--threads", type=int, default=DEFAULT_THREADS,
                            help="插件调用线程池大小，默认 %d" % DEFAULT_THREADS)
        parser.add_argument("--max-concurrent", type=int, default=DEFAULT_MAX_CONCURRENT,
                            help="单连接最大并发请求数，默认 %d" % DEFAULT_MAX_CONCURRENT)
        parser.add_argument("--job-timeout", type=float, default=DEFAULT_JOB_TIMEOUT,
                            help="job 模式单任务超时秒数，默认 %d" % DEFAULT_JOB_TIMEOUT)
        parser.add_argument("--job-cache", type=int, default=DEFAULT_JOB_CACHE,
                            help="已完成任务保留条数，默认 %d" % DEFAULT_JOB_CACHE)
        parser.add_argument("--truncate-cap", type=int, default=DEFAULT_TRUNCATE_CAP,
                            help="job 模式单流输出上限（字节），默认 %d" % DEFAULT_TRUNCATE_CAP)
        parser.add_argument("--log", dest="log_path", default=None,
                            help="日志路径，默认 <base-dir>/%s" % LOG_FILENAME)
        parser.add_argument("--debug", action="store_true", help="输出调试信息与错误 detail")
        args = parser.parse_args(argv)
        return cls(
            base_dir=args.base_dir,
            idle_timeout=args.idle_timeout,
            threads=args.threads,
            max_concurrent=args.max_concurrent,
            job_timeout=args.job_timeout,
            job_cache=args.job_cache,
            truncate_cap=args.truncate_cap,
            log_path=args.log_path,
            debug=args.debug,
        )


# ---------------------------------------------------------------------------
# 服务端主体
# ---------------------------------------------------------------------------

class Server(object):
    """ssh-outpost 基座。

    典型的嵌入式用法（测试即如此）::

        server = Server(Config(base_dir=tmpdir))
        server.start()
        ...                       # 期间可自行连接或检查内部状态
        server.shutdown()

    CLI 用法见 :func:`main`。
    """

    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger("ssh_server")
        self.token = None
        self.version = "0.0.0"
        self.port = None
        self.started_at = None
        self.started_monotonic = time.monotonic()
        self.plugins = {}
        self.children = ChildrenRegistry(config.children_path, logger=self.logger)
        self.jobs = JobManager(self, config)

        self._lock_fd = None
        self._listener = None
        self._stop_event = threading.Event()
        self._stop_reason = None
        self._tls = threading.local()
        self._active = 0
        self._active_lock = threading.Lock()
        self._last_active = time.monotonic()
        self._connections = set()
        self._connection_lock = threading.Lock()
        self._pool = None
        self._slots = None
        self._accept_thread = None
        self._idle_thread = None
        self._started = False
        self._shutdown_done = False

    # -- 基础状态 -----------------------------------------------------------
    @property
    def stop_requested(self):
        return self._stop_event.is_set()

    @property
    def stop_reason(self):
        return self._stop_reason

    def touch(self):
        """刷新活跃时间（每收到一个请求调用一次）。"""
        self._last_active = time.monotonic()

    def active_count(self):
        with self._active_lock:
            return self._active

    def mark_request_started(self):
        with self._active_lock:
            self._active += 1

    def mark_request_finished(self):
        with self._active_lock:
            if self._active > 0:
                self._active -= 1

    # -- 线程池 -------------------------------------------------------------
    def try_acquire_slot(self):
        return self._slots.acquire(blocking=False)

    def release_slot(self):
        try:
            self._slots.release()
        except ValueError:
            pass

    def submit(self, func, *args, **kwargs):
        self._pool.submit(func, *args, **kwargs)

    def begin_request(self):
        """占用一个执行额度并计入「在途请求」；额度不足返回 False。

        「在途请求」参与空闲判定（见 ``_is_idle``），因此 stream 与 job
        两种模式都必须走这里，否则长请求可能在空闲超时时被误杀。
        """
        if not self.try_acquire_slot():
            return False
        self.mark_request_started()
        return True

    def end_request(self):
        """释放在途计数与执行额度（必须与 ``begin_request`` 配对）。"""
        self.mark_request_finished()
        self.release_slot()

    # -- 插件 API 上下文 ----------------------------------------------------
    def current_context(self):
        return getattr(self._tls, "context", None)

    class _ApiContext(object):
        """with server.api_context(ctx): ... 期间 ctx 对插件 API 可见。"""

        def __init__(self, server, context):
            self._server = server
            self._context = context
            self._previous = None

        def __enter__(self):
            self._previous = getattr(self._server._tls, "context", None)
            self._server._tls.context = self._context
            return self._context

        def __exit__(self, exc_type, exc, tb):
            self._server._tls.context = self._previous
            return False

    def api_context(self, context):
        return Server._ApiContext(self, context)

    def plugin_log(self, level, message):
        """插件日志入口：仅记录错误级别，避免污染为审计日志。"""
        text = "[plugin] %s" % message
        if str(level).lower() in ("error", "critical"):
            self.logger.error(text)
        elif str(level).lower() == "warning":
            self.logger.warning(text)
        else:
            self.logger.debug(text)

    # -- 子进程 -------------------------------------------------------------
    def spawn_child(self, context, argv, cwd=None, env=None, stdin=None,
                    on_stdout=None, on_stderr=None, timeout=None, label=None):
        """创建子进程并纳入基座统一管理。"""
        if isinstance(argv, (str, bytes)):
            raise TypeError("argv 必须是参数列表，不接受整串命令")
        argv = list(argv)
        if not argv:
            raise ValueError("argv 不能为空")
        if stdin is None:
            popen_stdin = subprocess.DEVNULL
            input_data = None
        elif isinstance(stdin, (bytes, bytearray, str)):
            popen_stdin = subprocess.PIPE
            input_data = stdin.encode("utf-8") if isinstance(stdin, str) else bytes(stdin)
        else:
            popen_stdin = stdin
            input_data = None
        try:
            popen = subprocess.Popen(
                argv,
                cwd=cwd,
                env=env,
                stdin=popen_stdin,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,   # 独立进程组：可精确取消
                close_fds=True,
            )
        except OSError as exc:
            raise SshServerError("无法启动子进程 %r：%s" % (argv[0], exc))

        default_stdout = (lambda chunk: context.emit("stdout", chunk)) if on_stdout is None else on_stdout
        default_stderr = (lambda chunk: context.emit("stderr", chunk)) if on_stderr is None else on_stderr
        child = ChildProcess(popen, self.children, context,
                             default_stdout, default_stderr, label=label or argv[0])
        self.children.record(popen.pid, popen.pid, label=label or argv[0])
        context.register_child(child)

        if input_data is not None:
            def write_stdin():
                try:
                    popen.stdin.write(input_data)
                    popen.stdin.close()
                except OSError:
                    pass
            threading.Thread(target=write_stdin, name="child-stdin-%d" % popen.pid,
                             daemon=True).start()

        if timeout is not None:
            def on_timeout():
                child.timed_out = True
                child.terminate()
                if self.config.debug:
                    self.logger.warning("子进程 %r 超过 %s 秒，已终止", argv[0], timeout)
            timer = threading.Timer(timeout, on_timeout)
            timer.daemon = True
            timer.start()
            child._timeout_timer = timer  # 便于测试观察；wait() 后无需显式取消

        return child

    # -- job ----------------------------------------------------------------
    def run_job(self, job, fn, params):
        """在线程池中执行一个 job。"""
        context = RequestContext(self, None, None, "job", job=job)
        job.context = context
        # 让 job 的快照直接读取上下文缓冲区，避免两份缓冲不一致。
        job.stdout_buffer = context.stdout_buffer
        job.stderr_buffer = context.stderr_buffer
        timer = threading.Timer(self.config.job_timeout, self._on_job_timeout, args=(job,))
        timer.daemon = True
        timer.start()
        result = None
        error = None
        try:
            with self.api_context(context):
                result = fn(params)
        except RequestCancelled as exc:
            error = {"code": ERROR_TIMEOUT if job.cancel_reason == "timeout" else "cancelled",
                     "message": str(exc)}
        except Exception as exc:  # noqa: BLE001
            error = self.error_payload(exc)
        finally:
            timer.cancel()
            context.cleanup()
            if job.cancel_reason:
                state = "timeout" if job.cancel_reason == "timeout" else "cancelled"
            elif error is not None:
                state = "failed"
            else:
                state = "succeeded"
            self.jobs.finish(job, state, result=result, error=error)
            self.end_request()
        return result

    def _on_job_timeout(self, job):
        """job 超时：终止子进程并把任务置为 timeout。"""
        if job.mark_cancel("timeout") and job.context is not None:
            job.context.cancel("timeout")
        self.logger.warning("任务 %s 超过 %.0f 秒未完成，已终止", job.job_id, self.config.job_timeout)

    def error_payload(self, exc):
        """把异常映射为稳定错误码 + 人可读信息。"""
        if isinstance(exc, ChildTimeout):
            code = ERROR_TIMEOUT
        elif isinstance(exc, ValueError):
            code = ERROR_BAD_PARAMS
        else:
            code = ERROR_TOOL_ERROR
        payload = {"code": code, "message": str(exc) or exc.__class__.__name__}
        if self.config.debug:
            payload["detail"] = traceback.format_exc()
        return payload

    # -- 连接登记 -----------------------------------------------------------
    def register_connection(self, conn):
        with self._connection_lock:
            self._connections.add(conn)

    def unregister_connection(self, conn):
        with self._connection_lock:
            self._connections.discard(conn)

    # -- 启动 ---------------------------------------------------------------
    def _ensure_layout(self):
        ensure_private_dir(self.config.base_dir)
        ensure_private_dir(self.config.code_dir)
        ensure_private_dir(self.config.plugins_dir)
        if self.config.enable_log:
            ensure_private_file(self.config.log_path)

    def _setup_logging(self):
        logger = logging.getLogger("ssh_server")
        logger.setLevel(logging.DEBUG if self.config.debug else logging.INFO)
        logger.handlers = []
        if self.config.enable_log:
            handler = logging.FileHandler(self.config.log_path, encoding="utf-8")
            handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
            logger.addHandler(handler)
        else:
            logger.addHandler(logging.NullHandler())
        self.logger = logger
        return logger

    def _acquire_lock(self):
        """flock 抢占；失败抛 AlreadyRunning。"""
        fd = os.open(self.config.lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            raise AlreadyRunning("已有实例持有 %s" % self.config.lock_path)
        self._lock_fd = fd

    def start(self):
        """完成启动并开始接受连接（非阻塞）。"""
        if self._started:
            raise StartupError("服务已启动")
        self._ensure_layout()
        self._setup_logging()
        self._acquire_lock()
        try:
            # 崩溃残留回收：列表应当为空，非空说明上次异常退出
            self.children.recover_stale()
            self.version = read_version(self.config.code_dir)
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", 0))
            listener.listen(64)
            listener.settimeout(0.2)
            self._listener = listener
            self.port = listener.getsockname()[1]
            self.token = os.urandom(32).hex()
            self.started_at = time.time()
            self.started_monotonic = time.monotonic()
            self._last_active = time.monotonic()
            self._pool = WorkerPool(self.config.threads, logger=self.logger)
            self._slots = threading.BoundedSemaphore(max(1, int(self.config.threads)))
            build_api_module(self)
            self.plugins = PluginManager(self.config, self.logger).load()
            write_json_atomic(self.config.info_path, {
                "pid": os.getpid(),
                "port": self.port,
                "token": self.token,
                "protocol": PROTOCOL_VERSION,
                "version": self.version,
                "python": sys.executable,
                "started_at": self.started_at,
            })
        except Exception:
            self.shutdown()
            raise
        self._started = True
        self._accept_thread = threading.Thread(target=self._accept_loop, name="accept", daemon=True)
        self._accept_thread.start()
        self._idle_thread = threading.Thread(target=self._idle_loop, name="idle-check", daemon=True)
        self._idle_thread.start()
        self.logger.info("服务已就绪：pid=%d port=%d version=%s plugins=%d",
                         os.getpid(), self.port, self.version, len(self.plugins))
        if self.config.announce:
            payload = {"ok": True, "pid": os.getpid(), "port": self.port,
                       "version": self.version, "protocol": PROTOCOL_VERSION}
            sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
            sys.stdout.flush()
        return self

    # -- 主循环 -------------------------------------------------------------
    def _accept_loop(self):
        while not self._stop_event.is_set():
            try:
                client, addr = self._listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass
            conn = Connection(self, client, addr)
            self.register_connection(conn)
            conn.start()
        self.logger.debug("accept 循环退出")

    def _idle_loop(self):
        interval = self.config.idle_check_interval
        while not self._stop_event.wait(interval):
            if self._is_idle():
                self.logger.info("空闲 %.0f 秒且无在途请求，自动退出", self.config.idle_timeout)
                self.request_stop("idle")

    def _is_idle(self):
        if self.active_count() > 0 or self.jobs.running_count() > 0:
            return False
        if self.children.live_count() > 0:
            return False
        return (time.monotonic() - self._last_active) >= self.config.idle_timeout

    # -- 停止 ---------------------------------------------------------------
    def request_stop(self, reason):
        """请求停止：置标志并关闭监听套接字唤醒 accept 循环。"""
        if self._stop_event.is_set():
            return
        self._stop_reason = reason
        self._stop_event.set()
        listener = self._listener
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass

    def wait(self):
        """阻塞直到收到停止请求。"""
        self._stop_event.wait()

    def shutdown(self):
        """执行清理（幂等）。"""
        if self._shutdown_done:
            return
        self._shutdown_done = True
        if self._stop_reason is None:
            self._stop_reason = "shutdown"
        if not self._stop_event.is_set():
            self._stop_event.set()
        self.logger.info("开始关闭：reason=%s", self._stop_reason)
        listener, self._listener = self._listener, None
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass
        with self._connection_lock:
            connections = list(self._connections)
        for conn in connections:
            conn.close()
        self.jobs.cancel_all()
        self.children.terminate_all()
        # 兜底：清理继承了基座进程组、但未经 spawn 登记的进程（插件直接 subprocess 的情况）。
        #
        # 安全前提：只有当本进程是**会话首进程**时，自身进程组内的其他进程才必然由
        # 本服务派生。设计上服务由客户端以 setsid 拉起，因此正常运行时成立；在单元
        # 测试等「同进程内嵌启动」的场景下不成立，此时跳过，避免误杀同组的其他进程。
        try:
            is_session_leader = os.getsid(0) == os.getpid()
        except OSError:
            is_session_leader = False
        if is_session_leader:
            leftovers = scan_process_group_members(os.getpgrp(), exclude_pid=os.getpid())
            for pid in leftovers:
                try:
                    os.kill(pid, signal.SIGTERM)
                except OSError:
                    pass
            if leftovers:
                time.sleep(0.2)
                for pid in leftovers:
                    if process_alive(pid):
                        try:
                            os.kill(pid, signal.SIGKILL)
                        except OSError:
                            pass
        if self._pool is not None:
            self._pool.shutdown()
            self._pool = None
        # 关闭 flock（进程退出也会释放，这里显式关闭便于测试与嵌入式使用）
        if self._lock_fd is not None:
            try:
                os.close(self._lock_fd)
            except OSError:
                pass
            self._lock_fd = None
        self._started = False
        self.logger.info("已关闭")

    def install_signal_handlers(self):
        """安装 SIGTERM/SIGINT 处理器（只在主线程且仅 CLI 场景需要）。"""

        def handler(signum, frame):  # noqa: ARG001
            self.request_stop("signal:%d" % signum)

        signal.signal(signal.SIGTERM, handler)
        signal.signal(signal.SIGINT, handler)

    def run(self):
        """CLI 入口：启动、等待停止、清理。返回进程退出码。"""
        try:
            self.start()
        except AlreadyRunning:
            sys.stdout.write(json.dumps({"ok": False, "error": "already_running"}) + "\n")
            sys.stdout.flush()
            return 3
        except Exception as exc:  # noqa: BLE001 - 任何启动期异常都统一以退出码 4 上报
            sys.stdout.write(json.dumps({"ok": False, "error": "startup_failed",
                                         "message": str(exc)}, ensure_ascii=False) + "\n")
            sys.stdout.flush()
            return 4
        self.install_signal_handlers()
        try:
            self.wait()
        finally:
            self.shutdown()
        return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv=None):
    """命令行入口。"""
    config = Config.from_args(argv)
    return Server(config).run()


if __name__ == "__main__":
    sys.exit(main())
