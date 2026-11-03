# -*- coding: utf-8 -*-
"""``ChildrenRegistry`` 单元测试：记录持久化、崩溃残留回收、pid 复用防误杀。"""

import os
import subprocess
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tests._helpers import ssh_server  # noqa: E402


def spawn_sleeper():
    """启动一个独立进程组的 sleep，用于模拟残留子进程。"""
    return subprocess.Popen(
        ["sleep", "30"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,  # pgid == pid，便于精确清理
    )


def wait_dead(proc, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            return True
        time.sleep(0.05)
    return False


class ChildrenRegistryTest(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp(prefix="ssh-outpost-children-")
        self.path = os.path.join(self.tmp_dir, "children.json")

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_record_and_forget_persist(self):
        """登记与注销都会实时落盘。"""
        registry = ssh_server.ChildrenRegistry(self.path)
        registry.record(1234, 1234, "fake")
        data = ssh_server.read_json_file(self.path)
        self.assertEqual(len(data["children"]), 1)
        self.assertEqual(data["children"][0]["pid"], 1234)
        registry.forget(1234)
        self.assertEqual(ssh_server.read_json_file(self.path)["children"], [])

    def test_recover_stale_kills_leftover(self):
        """模拟上一次崩溃：启动时应回收记录中的存活进程组。"""
        proc = spawn_sleeper()
        try:
            ssh_server.ChildrenRegistry(self.path).record(proc.pid, proc.pid, "sleep")
            # 新实例模拟服务重启
            killed = ssh_server.ChildrenRegistry(self.path).recover_stale()
            self.assertEqual(killed, 1)
            self.assertTrue(wait_dead(proc), "残留进程未被回收")
            self.assertEqual(ssh_server.read_json_file(self.path)["children"], [])
        finally:
            if proc.poll() is None:
                proc.kill()

    def test_recover_skips_pid_reuse(self):
        """starttime 不匹配说明 pid 已被复用，绝不能误杀。"""
        proc = spawn_sleeper()
        try:
            real_start = ssh_server.proc_starttime(proc.pid)
            ssh_server.write_json_atomic(self.path, {"children": [{
                "pid": proc.pid,
                "pgid": proc.pid,
                "starttime": (real_start or 0) + 1,  # 伪造为不同进程
                "label": "reused",
            }]})
            killed = ssh_server.ChildrenRegistry(self.path).recover_stale()
            self.assertEqual(killed, 0)
            self.assertIsNone(proc.poll(), "存活进程被误杀")
        finally:
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=5)

    def test_terminate_all(self):
        """退出清理会终止全部登记进程并清空记录。"""
        proc = spawn_sleeper()
        try:
            registry = ssh_server.ChildrenRegistry(self.path)
            registry.record(proc.pid, proc.pid, "sleep")
            self.assertEqual(registry.terminate_all(grace=2.0), 1)
            self.assertTrue(wait_dead(proc))
            self.assertEqual(ssh_server.read_json_file(self.path)["children"], [])
        finally:
            if proc.poll() is None:
                proc.kill()

    def test_live_count(self):
        """live_count 只统计仍存活的登记项。"""
        proc = spawn_sleeper()
        try:
            registry = ssh_server.ChildrenRegistry(self.path)
            registry.record(proc.pid, proc.pid, "sleep")
            proc.kill()
            proc.wait(timeout=5)
            self.assertEqual(registry.live_count(), 0)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
