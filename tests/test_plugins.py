# -*- coding: utf-8 -*-
"""``PluginManager`` 单元测试：加载、命名校验、fail fast 行为。"""

import logging
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tests._helpers import make_config, make_layout, ssh_server, write_plugin  # noqa: E402

VALID_PLUGIN = '''
def do_echo(params):
    return {"echo": params.get("text", "")}

def registry(tools, description):
    tools["demo_echo"] = do_echo
    description["demo_echo"] = {
        "description": "回显",
        "parameters": {"type": "object", "properties": {"text": {"type": "string"}}},
    }
'''


def plugin_with_requires(requires_literal):
    """生成一个声明了 ``REQUIRES`` 的合法插件源码。"""
    return (
        "REQUIRES = %s\n\n"
        "def do_echo(params):\n"
        "    return {}\n\n"
        "def registry(tools, description):\n"
        "    tools[\"demo_echo\"] = do_echo\n"
        "    description[\"demo_echo\"] = {}\n" % (requires_literal,)
    )


class PluginManagerTest(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp(prefix="ssh-outpost-plugins-")
        _, self.plugins_dir = make_layout(self.tmp_dir)
        self.config = make_config(self.tmp_dir)
        self.manager = ssh_server.PluginManager(self.config, logging.getLogger("test-plugins"))

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def _load(self):
        return self.manager.load()

    def test_valid_plugin(self):
        """合法插件被加载，能力名与描述正确。"""
        write_plugin(self.plugins_dir, "demo", VALID_PLUGIN)
        entries = self._load()
        self.assertIn("demo_echo", entries)
        entry = entries["demo_echo"]
        self.assertEqual(entry.plugin, "demo")
        self.assertEqual(entry.description["description"], "回显")
        self.assertEqual(entry.description["parameters"]["type"], "object")
        self.assertEqual(entry.fn({"text": "hi"}), {"echo": "hi"})

    def test_wrong_prefix_rejected(self):
        """能力名必须带插件名前缀。"""
        write_plugin(self.plugins_dir, "demo", '''
def registry(tools, description):
    tools["other_echo"] = lambda params: {}
    description["other_echo"] = {}
''')
        self.assertRaises(ssh_server.PluginLoadError, self._load)

    def test_meta_prefix_rejected(self):
        """能力名不得占用基座保留前缀。"""
        write_plugin(self.plugins_dir, "ssh", '''
def registry(tools, description):
    tools["ssh_server_ping"] = lambda params: {}
    description["ssh_server_ping"] = {}
''')
        self.assertRaises(ssh_server.PluginLoadError, self._load)

    def test_reserved_plugin_name_rejected(self):
        """保留插件名 ssh_server 禁止使用。"""
        write_plugin(self.plugins_dir, "ssh_server", VALID_PLUGIN)
        self.assertRaises(ssh_server.PluginLoadError, self._load)

    def test_missing_registry_rejected(self):
        """没有 registry() 的插件 fail fast。"""
        write_plugin(self.plugins_dir, "demo", "VALUE = 1\n")
        self.assertRaises(ssh_server.PluginLoadError, self._load)

    def test_import_error_rejected(self):
        """导入期异常 fail fast。"""
        write_plugin(self.plugins_dir, "demo", "import definitely_not_a_module_xyz\n")
        self.assertRaises(ssh_server.PluginLoadError, self._load)

    def test_non_callable_rejected(self):
        """能力值必须可调用。"""
        write_plugin(self.plugins_dir, "demo", '''
def registry(tools, description):
    tools["demo_x"] = 42
    description["demo_x"] = {}
''')
        self.assertRaises(ssh_server.PluginLoadError, self._load)

    def test_orphan_description_rejected(self):
        """description 中出现未注册的能力名视为错误。"""
        write_plugin(self.plugins_dir, "demo", '''
def registry(tools, description):
    tools["demo_x"] = lambda params: {}
    description["demo_x"] = {}
    description["demo_ghost"] = {}
''')
        self.assertRaises(ssh_server.PluginLoadError, self._load)

    def test_underscore_files_skipped(self):
        """以下划线开头的文件视为辅助模块，跳过加载。"""
        write_plugin(self.plugins_dir, "_helper", "this is not valid python !!!\n")
        self.assertEqual(self._load(), {})

    def test_missing_plugins_dir_is_ok(self):
        """插件目录不存在时返回空表，不算错误。"""
        missing = make_config(os.path.join(self.tmp_dir, "nowhere"))
        manager = ssh_server.PluginManager(missing, logging.getLogger("test-plugins"))
        self.assertEqual(manager.load(), {})


class CapabilityVersionTest(unittest.TestCase):
    """能力接口版本声明与校验（design.md §8.5）。"""

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp(prefix="ssh-outpost-caps-")
        _, self.plugins_dir = make_layout(self.tmp_dir)
        self.config = make_config(self.tmp_dir)
        # 与 Server.start() 一致：先注入 ssh_server_api，再加载插件。
        self.server = ssh_server.Server(self.config)
        ssh_server.build_api_module(self.server)
        self.manager = ssh_server.PluginManager(self.config, logging.getLogger("test-caps"))

    def tearDown(self):
        sys.modules.pop(ssh_server.API_MODULE_NAME, None)
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def _load(self):
        return self.manager.load()

    def test_no_requires_declaration_is_allowed(self):
        """未声明 REQUIRES 的插件照常加载（缺能力会在 import/调用期暴露）。"""
        write_plugin(self.plugins_dir, "demo", VALID_PLUGIN)
        self.assertIn("demo_echo", self._load())

    def test_exact_versions_accepted(self):
        """声明与基座提供完全一致时加载成功。"""
        source = plugin_with_requires(
            '{"ssh_server_base@spawn": "0.1.0", "ssh_server_base@emit": "0.1.0"}')
        write_plugin(self.plugins_dir, "demo", source)
        self.assertIn("demo_echo", self._load())

    def test_wrong_version_rejected_with_diagnostic(self):
        """版本不匹配 fail fast，且错误消息自带「要求什么 / 提供什么」。"""
        source = plugin_with_requires('{"ssh_server_base@spawn": "9.9.9"}')
        write_plugin(self.plugins_dir, "demo", source)
        with self.assertRaises(ssh_server.PluginLoadError) as ctx:
            self._load()
        message = str(ctx.exception)
        self.assertIn("demo", message)
        self.assertIn("ssh_server_base@spawn", message)
        self.assertIn("9.9.9", message)
        self.assertIn(ssh_server.CAPABILITY_VERSIONS["spawn"], message)

    def test_unknown_capability_rejected(self):
        """声明了基座不存在的能力：fail fast。"""
        source = plugin_with_requires('{"ssh_server_base@teleport": "0.1.0"}')
        write_plugin(self.plugins_dir, "demo", source)
        with self.assertRaises(ssh_server.PluginLoadError):
            self._load()

    def test_requires_must_be_dict(self):
        """REQUIRES 不是 dict：fail fast。"""
        write_plugin(self.plugins_dir, "demo",
                     "REQUIRES = ['ssh_server_base@spawn']\n\n" + VALID_PLUGIN)
        with self.assertRaises(ssh_server.PluginLoadError):
            self._load()

    def test_requires_version_must_be_string(self):
        """版本值不是字符串：fail fast。"""
        write_plugin(self.plugins_dir, "demo",
                     'REQUIRES = {"ssh_server_base@spawn": 1}\n\n' + VALID_PLUGIN)
        with self.assertRaises(ssh_server.PluginLoadError):
            self._load()

    def test_malformed_capability_id_rejected(self):
        """缺少 owner@ 前缀的能力 ID：fail fast。"""
        write_plugin(self.plugins_dir, "demo",
                     'REQUIRES = {"spawn": "0.1.0"}\n\n' + VALID_PLUGIN)
        with self.assertRaises(ssh_server.PluginLoadError):
            self._load()

    def test_foreign_capability_owner_is_ignored(self):
        """非基座归属的声明（为插件间依赖预留）本阶段既不校验也不报错。"""
        source = plugin_with_requires(
            '{"ssh_server_base@spawn": "0.1.0", "other_plugin@thing": "9.9.9"}')
        write_plugin(self.plugins_dir, "demo", source)
        self.assertIn("demo_echo", self._load())

    def test_api_module_exposes_capability_table(self):
        """注入的 ssh_server_api.capabilities 与基座常量同源。"""
        api = sys.modules[ssh_server.API_MODULE_NAME]
        self.assertEqual(dict(api.capabilities), ssh_server.CAPABILITY_VERSIONS)
        for name in ("spawn", "emit", "log", "config"):
            self.assertIn(name, api.capabilities)

    def test_api_capability_table_is_read_only(self):
        """插件不得篡改基座的能力版本表（mappingproxy 对赋值抛 TypeError）。"""
        api = sys.modules[ssh_server.API_MODULE_NAME]
        with self.assertRaises(TypeError):
            api.capabilities["spawn"] = "9.9.9"
        self.assertEqual(api.capabilities["spawn"], ssh_server.CAPABILITY_VERSIONS["spawn"])

    def test_registry_not_called_when_versions_mismatch(self):
        """版本校验先于 registry()：不匹配时插件代码不应被执行。"""
        source = (
            'REQUIRES = {"ssh_server_base@spawn": "9.9.9"}\n\n'
            'def registry(tools, description):\n'
            '    raise AssertionError("registry 不应被调用")\n')
        write_plugin(self.plugins_dir, "demo", source)
        with self.assertRaises(ssh_server.PluginLoadError):
            self._load()


if __name__ == "__main__":
    unittest.main()
