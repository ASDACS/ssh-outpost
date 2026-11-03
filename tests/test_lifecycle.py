# -*- coding: utf-8 -*-
"""生命周期测试：flock 单实例、server.info、空闲自退、退出清理。

SIGTERM 的真实信号测试需要独立进程，这里以 ``@unittest.skip`` 标注为骨架，
后续可补充一个 subprocess 端到端用例。
"""

import contextlib
import io
import os
import shutil
import stat
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tests._helpers import (  # noqa: E402
    DEMO_PLUGIN,
    make_config,
    make_layout,
    running_server,
    ssh_server,
    write_plugin,
)

SERVERS = {"demo": DEMO_PLUGIN}


class ServerInfoTest(unittest.TestCase):

    def test_server_info_written(self):
        """启动后 server.info 字段完整且权限为 0600。"""
        with running_server(plugins=SERVERS) as srv:
            info = ssh_server.read_json_file(srv.config.info_path)
            self.assertIsNotNone(info)
            self.assertEqual(info["pid"], os.getpid())
            self.assertEqual(info["port"], srv.port)
            self.assertEqual(info["token"], srv.token)
            self.assertEqual(info["protocol"], ssh_server.PROTOCOL_VERSION)
            self.assertEqual(info["version"], "9.9.9")
            self.assertTrue(info["python"])
            mode = stat.S_IMODE(os.stat(srv.config.info_path).st_mode)
            self.assertEqual(mode, 0o600)

    def test_server_info_survives_shutdown(self):
        """退出时不删除 server.info（只会被下一个实例原子覆盖）。"""
        tmp_dir = tempfile.mkdtemp(prefix="ssh-outpost-info-")
        try:
            with running_server(plugins=SERVERS, base_dir=tmp_dir) as srv:
                path = srv.config.info_path
                port = srv.port
            self.assertTrue(os.path.exists(path))
            self.assertEqual(ssh_server.read_json_file(path)["port"], port)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)


class SingleInstanceTest(unittest.TestCase):

    def test_second_instance_rejected(self):
        """同一 base_dir 上的第二个实例启动失败（flock）。"""
        with tempfile.TemporaryDirectory() as tmp:
            make_layout(tmp)
            first = ssh_server.Server(make_config(tmp))
            first.start()
            try:
                second = ssh_server.Server(make_config(tmp))
                self.assertRaises(ssh_server.AlreadyRunning, second.start)
                second.shutdown()
            finally:
                first.shutdown()

    def test_lock_released_after_shutdown(self):
        """退出后锁释放，新实例可以启动。"""
        with tempfile.TemporaryDirectory() as tmp:
            make_layout(tmp)
            first = ssh_server.Server(make_config(tmp))
            first.start()
            first.shutdown()
            second = ssh_server.Server(make_config(tmp))
            second.start()
            try:
                self.assertTrue(second.port)
            finally:
                second.shutdown()

    def test_children_file_cleared_on_shutdown(self):
        """正常退出后 children.json 为空列表。"""
        tmp_dir = tempfile.mkdtemp(prefix="ssh-outpost-children-")
        try:
            with running_server(plugins=SERVERS, base_dir=tmp_dir) as srv:
                path = srv.config.children_path
            self.assertEqual(ssh_server.read_json_file(path)["children"], [])
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)


class IdleExitTest(unittest.TestCase):

    def test_idle_triggers_stop(self):
        """空闲超过阈值时自动请求停止，原因为 idle。"""
        overrides = {"idle_timeout": 0.3, "idle_check_interval": 0.05}
        with running_server(plugins=SERVERS, config_overrides=overrides) as srv:
            self.assertTrue(srv._stop_event.wait(5), "空闲退出未触发")
            self.assertEqual(srv.stop_reason, "idle")

    def test_activity_prevents_idle_exit(self):
        """持续有请求时不触发空闲退出。"""
        from tests._helpers import connect
        overrides = {"idle_timeout": 0.4, "idle_check_interval": 0.05}
        with running_server(plugins=SERVERS, config_overrides=overrides) as srv:
            client = connect(srv)
            deadline = time.time() + 1.2
            while time.time() < deadline:
                client.call(ssh_server.META_PREFIX + "ping")
            self.assertFalse(srv._stop_event.is_set(), "有请求时不应空闲退出")
            client.close()


class CliExitCodeTest(unittest.TestCase):
    """``Server.run()`` 的退出码契约，客户端据此分支处理。"""

    def test_run_reports_already_running(self):
        """已有实例时 run() 输出 already_running 并返回 3。"""
        with running_server(plugins=SERVERS) as srv:
            other = ssh_server.Server(make_config(srv.config.base_dir))
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                code = other.run()
            self.assertEqual(code, 3)
            self.assertIn("already_running", buffer.getvalue())

    def test_run_reports_startup_failure(self):
        """插件加载失败等启动错误：输出 startup_failed 并返回 4。"""
        tmp_dir = tempfile.mkdtemp(prefix="ssh-outpost-badplugin-")
        try:
            _, plugins_dir = make_layout(tmp_dir)
            write_plugin(plugins_dir, "demo", "import definitely_not_a_module_xyz\n")
            server = ssh_server.Server(make_config(tmp_dir))
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                code = server.run()
            self.assertEqual(code, 4)
            self.assertIn("startup_failed", buffer.getvalue())
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)


class ConfigTest(unittest.TestCase):

    def test_from_args(self):
        """命令行参数解析。"""
        config = ssh_server.Config.from_args(
            ["--base-dir", "/tmp/ssh-outpost-x", "--threads", "2",
             "--idle-timeout", "5", "--debug"])
        self.assertEqual(config.threads, 2)
        self.assertEqual(config.idle_timeout, 5)
        self.assertTrue(config.debug)
        self.assertTrue(config.base_dir.endswith("ssh-outpost-x"))
        self.assertEqual(config.lock_path, os.path.join(config.base_dir, "server.lock"))

    def test_base_dir_is_required(self):
        """base dir 的位置由调用方决定：不给就明确失败，而不是回退到某个默认家目录。"""
        with self.assertRaises(SystemExit):
            ssh_server.Config.from_args([])
        with self.assertRaises(ValueError):
            ssh_server.Config()

    def test_base_dir_expands_user(self):
        """基座自己展开 ~；调用方因此可以安全地下发 ~/... 形式。"""
        config = ssh_server.Config(base_dir="~/ssh-outpost-test")
        self.assertTrue(os.path.isabs(config.base_dir))
        self.assertNotIn("~", config.base_dir)


class SignalSkeletonTest(unittest.TestCase):
    """信号处理的端到端用例骨架（需要独立进程，暂未实现）。"""

    @unittest.skip("骨架：需要以子进程方式拉起服务并发送 SIGTERM，后续补充")
    def test_sigterm_triggers_cleanup(self):
        self.fail("TODO: 子进程 + os.kill(SIGTERM) 验证子进程被回收、children.json 清空")


if __name__ == "__main__":
    unittest.main()
