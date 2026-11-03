# -*- coding: utf-8 -*-
"""协议集成测试：握手、元能力、stream/job 两种执行模式、背压相关行为。

这些用例会在同进程内真实启动服务并走 TCP + SSH 端口转发等价的回环连接，
因此同时覆盖了 ``Server`` / ``Connection`` / ``WorkerPool`` / 插件 的协作。
"""

import os
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tests._helpers import (  # noqa: E402
    DEMO_PLUGIN,
    Client,
    connect,
    running_server,
    ssh_server,
)

SERVERS = {"demo": DEMO_PLUGIN}


class HandshakeTest(unittest.TestCase):

    def test_handshake_ok(self):
        with running_server(plugins=SERVERS) as srv:
            client = connect(srv)
            self.assertEqual(client.hello_reply["type"], "hello_ok")
            self.assertEqual(client.hello_reply["protocol"], ssh_server.PROTOCOL_VERSION)
            self.assertEqual(client.hello_reply["version"], "9.9.9")
            client.close()

    def test_bad_token(self):
        with running_server(plugins=SERVERS) as srv:
            client = Client(srv.port, srv.token, do_handshake=False)
            reply = client.hello("wrong-token")
            self.assertEqual(reply["type"], "hello_err")
            self.assertEqual(reply["error"], ssh_server.ERROR_BAD_TOKEN)
            client.close()

    def test_protocol_mismatch(self):
        with running_server(plugins=SERVERS) as srv:
            client = Client(srv.port, srv.token, do_handshake=False)
            reply = client.hello(srv.token, protocol=999)
            self.assertEqual(reply["type"], "hello_err")
            self.assertEqual(reply["error"], "protocol_mismatch")
            client.close()


class MetaCapabilityTest(unittest.TestCase):

    def test_ping(self):
        with running_server(plugins=SERVERS) as srv:
            client = connect(srv)
            result, events = client.call(ssh_server.META_PREFIX + "ping")
            payload = Client.assert_success(result)
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["version"], "9.9.9")
            self.assertEqual(payload["protocol"], ssh_server.PROTOCOL_VERSION)
            self.assertEqual(events, [])
            client.close()

    def test_info(self):
        with running_server(plugins=SERVERS) as srv:
            client = connect(srv)
            result, _ = client.call(ssh_server.META_PREFIX + "info")
            payload = Client.assert_success(result)
            self.assertEqual(payload["platform"], "linux")
            self.assertEqual(payload["idle_timeout"], ssh_server.DEFAULT_IDLE_TIMEOUT)
            # 诊断信息里带基座的能力接口版本表（design.md §8.5）
            self.assertEqual(payload["capabilities"], ssh_server.CAPABILITY_VERSIONS)
            self.assertIn("spawn", payload["capabilities"])
            client.close()

    def test_list_tools(self):
        with running_server(plugins=SERVERS) as srv:
            client = connect(srv)
            result, _ = client.call(ssh_server.META_PREFIX + "list_tools")
            tools = {item["name"]: item for item in Client.assert_success(result)["tools"]}
            self.assertIn("demo_echo", tools)
            self.assertIn("parameters", tools["demo_echo"])
            client.close()

    def test_startup_fails_fast_on_capability_mismatch(self):
        """声明了不匹配的能力版本时，整个服务启动失败（design.md §8.5）。"""
        bad = ('REQUIRES = {"ssh_server_base@spawn": "9.9.9"}\n'
               'def registry(tools, description):\n'
               '    tools["bad_x"] = lambda params: {}\n'
               '    description["bad_x"] = {}\n')
        with self.assertRaises(ssh_server.PluginLoadError):
            with running_server(plugins={"bad": bad}):
                pass


class CallValidationTest(unittest.TestCase):

    def test_unknown_tool(self):
        with running_server(plugins=SERVERS) as srv:
            client = connect(srv)
            result, _ = client.call("nope_tool")
            self.assertFalse(result["ok"])
            self.assertEqual(result["error"]["code"], ssh_server.ERROR_UNKNOWN_TOOL)
            client.close()

    def test_not_a_call_message(self):
        with running_server(plugins=SERVERS) as srv:
            client = connect(srv)
            client.send({"type": "wat"})
            message = client.recv()
            self.assertFalse(message["ok"])
            self.assertEqual(message["error"]["code"], ssh_server.ERROR_BAD_REQUEST)
            client.close()

    def test_params_must_be_dict(self):
        with running_server(plugins=SERVERS) as srv:
            client = connect(srv)
            client.send({"type": "call", "id": 7, "tool": "demo_echo", "params": [1, 2]})
            message = client.recv()
            self.assertFalse(message["ok"])
            self.assertEqual(message["error"]["code"], ssh_server.ERROR_BAD_PARAMS)
            self.assertEqual(message["id"], 7)
            client.close()

    def test_bad_mode(self):
        with running_server(plugins=SERVERS) as srv:
            client = connect(srv)
            client.send({"type": "call", "id": 8, "tool": "demo_echo",
                         "params": {}, "mode": "parallel"})
            message = client.recv()
            self.assertFalse(message["ok"])
            self.assertEqual(message["error"]["code"], ssh_server.ERROR_BAD_REQUEST)
            client.close()

    def test_plugin_exception_is_tool_error(self):
        with running_server(plugins=SERVERS) as srv:
            client = connect(srv)
            result, _ = client.call("demo_boom")
            self.assertFalse(result["ok"])
            self.assertEqual(result["error"]["code"], ssh_server.ERROR_TOOL_ERROR)
            self.assertIn("boom", result["error"]["message"])
            client.close()


class StreamModeTest(unittest.TestCase):

    def test_simple_result(self):
        with running_server(plugins=SERVERS) as srv:
            client = connect(srv)
            result, events = client.call("demo_echo", {"text": "hi"})
            self.assertEqual(Client.assert_success(result), {"echo": "hi"})
            self.assertEqual(events, [])
            client.close()

    def test_progress_events_are_ordered(self):
        with running_server(plugins=SERVERS) as srv:
            client = connect(srv)
            result, events = client.call("demo_progress", {"n": 3})
            self.assertEqual(Client.assert_success(result), {"ticks": 3})
            self.assertEqual(len(events), 3)
            self.assertEqual([e["seq"] for e in events], [0, 1, 2])
            self.assertTrue(all(e["stream"] == "stdout" for e in events))
            self.assertTrue(all(e["encoding"] == "utf-8" for e in events))
            self.assertEqual("".join(e["data"] for e in events), "tick 0\ntick 1\ntick 2\n")
            client.close()

    def test_child_process_output_streamed(self):
        with running_server(plugins=SERVERS) as srv:
            client = connect(srv)
            result, events = client.call("demo_run", {"argv": ["sh", "-c", "echo child-hi"]})
            payload = Client.assert_success(result)
            self.assertEqual(payload["exit_code"], 0)
            text = "".join(e["data"] for e in events)
            self.assertIn("child-hi", text)
            client.close()

    def test_disconnect_cancels_request(self):
        """流式请求在客户端断连后被取消，活动计数与子进程归零。"""
        with running_server(plugins=SERVERS) as srv:
            client = connect(srv)
            box = {}

            def run():
                try:
                    client.call("demo_progress", {"n": 40})
                except Exception as exc:  # noqa: BLE001 - 断连后预期失败
                    box["error"] = exc

            thread = threading.Thread(target=run, daemon=True)
            thread.start()
            time.sleep(0.3)
            self.assertGreater(srv.active_count(), 0, "请求应处于执行中")
            client.close()

            deadline = time.time() + 5
            while time.time() < deadline and srv.active_count() > 0:
                time.sleep(0.05)
            self.assertEqual(srv.active_count(), 0, "断连后请求未被取消")
            self.assertEqual(srv.children.live_count(), 0)
            thread.join(timeout=5)

    def test_long_stream_survives_connection_idle_timeout(self):
        """有在途 stream 请求时，连接空闲超时不得断开连接。"""
        with running_server(plugins=SERVERS,
                            config_overrides={"conn_idle_timeout": 0.3}) as srv:
            client = connect(srv)
            # 约 1 秒，明显超过 0.3 秒的连接空闲超时
            result, _ = client.call("demo_progress", {"n": 20})
            self.assertEqual(Client.assert_success(result), {"ticks": 20})
            client.close()

    def test_idle_connection_is_closed(self):
        """没有在途请求的连接，空闲超时后由服务端关闭。"""
        with running_server(plugins=SERVERS,
                            config_overrides={"conn_idle_timeout": 0.3}) as srv:
            client = connect(srv)
            time.sleep(0.8)
            with self.assertRaises((EOFError, OSError)):
                client.call("demo_echo", {"text": "x"})
            client.close()

    def test_pool_full_returns_busy(self):
        """线程池饱和时，新请求立即收到 busy，而不是排队等待。"""
        with running_server(plugins=SERVERS, config_overrides={"threads": 1}) as srv:
            slow = connect(srv)
            fast = connect(srv)
            thread = threading.Thread(
                target=lambda: self._swallow(slow.call, "demo_progress", {"n": 40}),
                daemon=True,
            )
            thread.start()
            time.sleep(0.3)
            result, _ = fast.call("demo_echo", {"text": "x"})
            self.assertFalse(result["ok"])
            self.assertEqual(result["error"]["code"], ssh_server.ERROR_BUSY)
            slow.close()
            fast.close()
            thread.join(timeout=5)

    @staticmethod
    def _swallow(func, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception:  # noqa: BLE001
            pass


class JobModeTest(unittest.TestCase):

    def test_job_returns_id_and_succeeds(self):
        with running_server(plugins=SERVERS) as srv:
            client = connect(srv)
            result, _ = client.call("demo_echo", {"text": "job"}, mode="job")
            payload = Client.assert_success(result)
            self.assertEqual(payload["state"], "running")
            snapshot = client.wait_job(payload["job_id"])
            self.assertEqual(snapshot["state"], "succeeded")
            self.assertEqual(snapshot["result"], {"echo": "job"})
            client.close()

    def test_job_accumulates_output(self):
        with running_server(plugins=SERVERS) as srv:
            client = connect(srv)
            result, _ = client.call("demo_run", {"argv": ["sh", "-c", "printf hi"]}, mode="job")
            job_id = Client.assert_success(result)["job_id"]
            snapshot = client.wait_job(job_id)
            self.assertEqual(snapshot["state"], "succeeded")
            self.assertIn("hi", snapshot["stdout"])
            self.assertEqual(snapshot["exit_code"], 0)
            client.close()

    def test_job_timeout(self):
        with running_server(plugins=SERVERS, config_overrides={"job_timeout": 0.3}) as srv:
            client = connect(srv)
            result, _ = client.call("demo_progress", {"n": 40}, mode="job")
            job_id = Client.assert_success(result)["job_id"]
            snapshot = client.wait_job(job_id)
            self.assertEqual(snapshot["state"], "timeout")
            client.close()

    def test_job_cancel(self):
        with running_server(plugins=SERVERS) as srv:
            client = connect(srv)
            result, _ = client.call("demo_progress", {"n": 40}, mode="job")
            job_id = Client.assert_success(result)["job_id"]
            cancel_result, _ = client.call(ssh_server.META_PREFIX + "job_cancel", {"job_id": job_id})
            self.assertEqual(Client.assert_success(cancel_result)["state"], "cancelled")
            snapshot = client.wait_job(job_id)
            self.assertEqual(snapshot["state"], "cancelled")
            client.close()

    def test_job_get_unknown(self):
        with running_server(plugins=SERVERS) as srv:
            client = connect(srv)
            result, _ = client.call(ssh_server.META_PREFIX + "job_get", {"job_id": "deadbeef"})
            self.assertFalse(result["ok"])
            self.assertEqual(result["error"]["code"], ssh_server.ERROR_UNKNOWN_JOB)
            client.close()

    def test_job_get_requires_job_id(self):
        with running_server(plugins=SERVERS) as srv:
            client = connect(srv)
            result, _ = client.call(ssh_server.META_PREFIX + "job_get", {})
            self.assertFalse(result["ok"])
            self.assertEqual(result["error"]["code"], ssh_server.ERROR_BAD_PARAMS)
            client.close()

    def test_job_truncation_applies(self):
        """job 模式输出超过上限时保留头尾并标记 truncated。"""
        with running_server(plugins=SERVERS, config_overrides={"truncate_cap": 64}) as srv:
            client = connect(srv)
            result, _ = client.call("demo_big", {"n": 20}, mode="job")
            job_id = Client.assert_success(result)["job_id"]
            snapshot = client.wait_job(job_id)
            self.assertEqual(snapshot["state"], "succeeded")
            self.assertTrue(snapshot["truncated"])
            self.assertIn("<truncated", snapshot["stdout"])
            client.close()

    def test_shutdown_meta_stops_server(self):
        with running_server(plugins=SERVERS) as srv:
            client = connect(srv)
            result, _ = client.call(ssh_server.META_PREFIX + "shutdown", {"reason": "test"})
            self.assertTrue(Client.assert_success(result)["ok"])
            self.assertTrue(srv._stop_event.wait(3), "shutdown 未触发停止")
            client.close()


if __name__ == "__main__":
    unittest.main()
