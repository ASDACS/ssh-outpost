# -*- coding: utf-8 -*-
"""测试辅助工具：导入基座、构造临时目录、起停服务、模拟客户端。

设计目标：

* 测试**不需要**真实安装目录；每个用例使用独立临时目录（``--base-dir`` 必填）；
* 服务在同进程内以 ``start()/shutdown()`` 方式起停，便于断言内部状态；
* 客户端是一个最小的 NDJSON 客户端，足够覆盖协议用例。
"""

import contextlib
import json
import os
import shutil
import socket
import sys
import tempfile
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVER_DIR = os.path.join(REPO_ROOT, "server")
if SERVER_DIR not in sys.path:
    sys.path.insert(0, SERVER_DIR)

import ssh_server  # noqa: E402  （需要先设置 sys.path）


# ---------------------------------------------------------------------------
# 目录与配置
# ---------------------------------------------------------------------------

def make_config(base_dir, **overrides):
    """构造测试用 Config：默认关闭日志、不向 stdout 播报就绪报文。"""
    kwargs = {"base_dir": base_dir, "announce": False, "enable_log": False}
    kwargs.update(overrides)
    return ssh_server.Config(**kwargs)


def make_layout(base_dir):
    """创建 ``server/`` 与 ``server/plugins/`` 目录并写入 version.txt。"""
    code_dir = os.path.join(base_dir, "server")
    plugins_dir = os.path.join(code_dir, "plugins")
    os.makedirs(plugins_dir, exist_ok=True)
    with open(os.path.join(code_dir, "version.txt"), "w", encoding="utf-8") as fh:
        fh.write("9.9.9\n")
    return code_dir, plugins_dir


def write_plugin(plugins_dir, stem, source):
    """写入一个插件文件。"""
    os.makedirs(plugins_dir, exist_ok=True)
    path = os.path.join(plugins_dir, stem + ".py")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(source)
    return path


def write_file(path, content):
    """写普通文本文件。"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)
    return path


# ---------------------------------------------------------------------------
# 默认演示插件源码（用于协议集成测试）
# ---------------------------------------------------------------------------

DEMO_PLUGIN = r'''
import time
from ssh_server_api import emit, spawn

#: 演示能力接口版本声明：与基座能力版本表精确匹配（design.md §8.5）。
REQUIRES = {
    "ssh_server_base@emit": "0.1.0",
    "ssh_server_base@spawn": "0.1.0",
}


def demo_echo(params):
    return {"echo": params.get("text", "")}


def demo_boom(params):
    raise RuntimeError("boom")


def demo_progress(params):
    n = int(params.get("n", 3))
    for i in range(n):
        emit("stdout", "tick %d\n" % i)
        time.sleep(0.05)
    return {"ticks": n}


def demo_run(params):
    child = spawn(params["argv"])
    return {"exit_code": child.wait()}


def demo_big(params):
    # 产生超过截断上限的输出（每行 64 字节 * 行数）
    n = int(params.get("n", 10))
    for i in range(n):
        emit("stdout", ("line-%06d " % i) + "x" * 50 + "\n")
    return {"lines": n}


def registry(tools, description):
    tools["demo_echo"] = demo_echo
    tools["demo_boom"] = demo_boom
    tools["demo_progress"] = demo_progress
    tools["demo_run"] = demo_run
    tools["demo_big"] = demo_big
    for name in list(tools):
        description[name] = {"description": name, "parameters": {"type": "object"}}
'''


# ---------------------------------------------------------------------------
# 客户端
# ---------------------------------------------------------------------------

class Client(object):
    """最小 NDJSON 客户端。"""

    def __init__(self, port, token, timeout=5.0, do_handshake=True):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.reader = ssh_server.LineReader(self.sock)
        self.hello_reply = None
        if do_handshake:
            self.hello(token)

    def send(self, payload):
        self.sock.sendall(json.dumps(payload, ensure_ascii=False).encode("utf-8") + b"\n")

    def recv(self):
        line = self.reader.readline()
        if line is None:
            raise EOFError("连接已关闭")
        return json.loads(line.decode("utf-8"))

    def hello(self, token, protocol=None):
        self.send({
            "type": "hello",
            "protocol": ssh_server.PROTOCOL_VERSION if protocol is None else protocol,
            "token": token,
            "client": "test/0.0",
        })
        self.hello_reply = self.recv()
        return self.hello_reply

    def call(self, tool, params=None, mode="stream", request_id=1, timeout=None):
        """发起调用，返回 ``(result_报文, events 列表)``。"""
        if timeout is not None:
            self.sock.settimeout(timeout)
        self.send({"type": "call", "id": request_id, "tool": tool,
                   "params": params or {}, "mode": mode})
        events = []
        while True:
            message = self.recv()
            kind = message.get("type")
            if kind == "event":
                events.append(message)
                continue
            if kind == "ack":
                continue
            if kind == "result":
                return message, events
            raise AssertionError("非预期的报文：%r" % (message,))

    def wait_job(self, job_id, timeout=5.0, terminal=("succeeded", "failed", "timeout", "cancelled")):
        """轮询任务直至终态，返回最后一次快照。"""
        deadline = time.time() + timeout
        snapshot = None
        while time.time() < deadline:
            result, _ = self.call(ssh_server.META_PREFIX + "job_get", {"job_id": job_id})
            self.assert_success(result)
            snapshot = result["result"]
            if snapshot["state"] in terminal:
                return snapshot
            time.sleep(0.05)
        raise AssertionError("任务在 %.1f 秒内未到达终态：%r" % (timeout, snapshot))

    @staticmethod
    def assert_success(result):
        if not result.get("ok"):
            raise AssertionError("调用失败：%r" % (result.get("error"),))
        return result["result"]

    def close(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


def connect(server, timeout=5.0):
    """连接一个已启动的 Server。"""
    return Client(server.port, server.token, timeout=timeout)


# ---------------------------------------------------------------------------
# 起停服务的上下文管理器
# ---------------------------------------------------------------------------

@contextlib.contextmanager
def running_server(plugins=None, config_overrides=None, base_dir=None):
    """启动一个临时服务，yield 出 Server 实例，退出时清理。

    :param plugins: ``{stem: source}``，会写入临时 plugins 目录
    :param config_overrides: 覆盖 Config 默认值（如 idle_timeout/threads）
    """
    tmp_dir = base_dir or tempfile.mkdtemp(prefix="ssh-outpost-test-")
    created_tmp = base_dir is None
    try:
        _, plugins_dir = make_layout(tmp_dir)
        for stem, source in (plugins or {}).items():
            write_plugin(plugins_dir, stem, source)
        config = make_config(tmp_dir, **(config_overrides or {}))
        server = ssh_server.Server(config)
        server.start()
        try:
            yield server
        finally:
            server.shutdown()
    finally:
        if created_tmp:
            shutil.rmtree(tmp_dir, ignore_errors=True)
