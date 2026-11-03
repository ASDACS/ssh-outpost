# -*- coding: utf-8 -*-
"""示例插件：演示 registry 约定、能力版本声明与 ssh_server_api 用法。

注意：插件是**无状态**的、单文件的；能力名必须为 ``<文件名>_<工具名>``。
本文件不是基座的一部分，仅作为插件开发模板与手工验证样例。

它也演示了 ``REQUIRES``：插件声明自己用到的基座能力及其**精确版本**，
基座在加载期比对自身的能力版本表，任一不匹配即整个服务启动失败（fail fast）。
规则与判定表见 design.md §8.5；能力名与 ``ssh_server_api`` 的属性名一一对应。
"""

import time

# 基座在加载插件前会把 ssh_server_api 注入 sys.modules，
# 插件通过它访问子进程 / 流式输出 / 日志等基础设施。
from ssh_server_api import emit, log, spawn

#: 本插件用到的基座能力接口版本。只声明**真正用到**的能力，不声明全量。
REQUIRES = {
    "ssh_server_base@emit": "0.1.0",
    "ssh_server_base@log": "0.1.0",
    "ssh_server_base@spawn": "0.1.0",
}


def example_echo(params):
    """最小示例：把传入的 text 原样回显。"""
    text = params.get("text", "")
    if not isinstance(text, str):
        raise ValueError("text 必须是字符串")
    return {"echo": text}


def example_run(params):
    """演示 spawn + 流式输出：执行 argv 并返回退出码。

    params:
        argv (list[str]): 要执行的命令与参数（必须为列表，不接受整串命令）
        cwd  (str, 可选): 工作目录
    """
    argv = params.get("argv")
    if not isinstance(argv, list) or not argv:
        raise ValueError("argv 必须是非空列表")
    cwd = params.get("cwd")

    # spawn 不传回调时，基座默认把子进程输出转发到当前请求的输出流：
    # stream 模式即时推送 event，job 模式累积到截断缓冲。
    child = spawn(argv, cwd=cwd)
    code = child.wait()
    log("info", "example_run 结束：%r -> %s" % (argv, code))
    return {"exit_code": code, "argv": argv}


def example_progress(params):
    """演示主动 emit：每秒输出一个点，便于观察流式回传与取消/超时。"""
    seconds = float(params.get("seconds", 3))
    deadline = time.time() + seconds
    count = 0
    while time.time() < deadline:
        # 请求被取消（断连 / 任务超时）时，emit 会抛 RequestCancelled，
        # 插件应当让它向外传播，尽快结束执行。
        emit("stdout", ".%d\n" % count)
        count += 1
        time.sleep(min(0.2, max(0.0, deadline - time.time())))
    return {"ticks": count}


def registry(tools, description):
    """基座调用入口：把插件能力登记进传入的两个 dict。"""
    tools["example_echo"] = example_echo
    description["example_echo"] = {
        "description": "回显传入的文本（最小示例）",
        "parameters": {
            "type": "object",
            "properties": {"text": {"type": "string"}},
            "required": ["text"],
            "additionalProperties": False,
        },
    }

    tools["example_run"] = example_run
    description["example_run"] = {
        "description": "执行一条命令并返回退出码，输出以流式事件回传",
        "parameters": {
            "type": "object",
            "properties": {
                "argv": {"type": "array", "items": {"type": "string"}},
                "cwd": {"type": "string"},
            },
            "required": ["argv"],
            "additionalProperties": False,
        },
    }

    tools["example_progress"] = example_progress
    description["example_progress"] = {
        "description": "演示主动流式输出，可配合 job 模式观察超时取消",
        "parameters": {
            "type": "object",
            "properties": {"seconds": {"type": "number"}},
            "additionalProperties": False,
        },
    }
