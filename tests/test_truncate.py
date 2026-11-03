# -*- coding: utf-8 -*-
"""``TruncatingBuffer`` 单元测试：头尾保留、占位符、增量追加。"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tests._helpers import ssh_server  # noqa: E402


class TruncatingBufferTest(unittest.TestCase):

    def test_small_payload_untouched(self):
        """未超过上限时不截断。"""
        buf = ssh_server.TruncatingBuffer(100)
        buf.append(b"hello")
        self.assertEqual(buf.value(), b"hello")
        self.assertFalse(buf.truncated)
        self.assertEqual(buf.dropped, 0)

    def test_head_and_tail_reserved(self):
        """一次性写入超限数据：保留头尾各一半，中间插入占位符。"""
        buf = ssh_server.TruncatingBuffer(40)  # head 20 / tail 20
        buf.append(b"A" * 100)
        value = buf.value()
        self.assertTrue(buf.truncated)
        self.assertEqual(buf.dropped, 60)
        self.assertTrue(value.startswith(b"A" * 20))
        self.assertTrue(value.endswith(b"A" * 20))
        self.assertIn(b"<truncated 60 bytes>", value)

    def test_incremental_append_drops_from_middle(self):
        """增量追加时，被丢弃的是中间部分，头部与最新尾部始终保留。"""
        buf = ssh_server.TruncatingBuffer(10)  # head 5 / tail 5
        for code in b"ABCDEFGHIJK":
            buf.append(bytes([code]))
        # 头 5 字节为 ABCDE；尾部窗口只保留 GHIJK（F 被丢弃）
        self.assertTrue(buf.truncated)
        self.assertEqual(buf.dropped, 1)
        value = buf.value()
        self.assertTrue(value.startswith(b"ABCDE"))
        self.assertTrue(value.endswith(b"GHIJK"))
        self.assertIn(b"<truncated 1 bytes>", value)

    def test_zero_cap_drops_everything(self):
        """上限为 0 时全部丢弃，只留占位符。"""
        buf = ssh_server.TruncatingBuffer(0)
        buf.append(b"abc")
        self.assertEqual(buf.dropped, 3)
        self.assertIn(b"<truncated 3 bytes>", buf.value())

    def test_text_decoding(self):
        """文本渲染按 UTF-8 解码。"""
        buf = ssh_server.TruncatingBuffer(64)
        buf.append("中文输出\n".encode("utf-8"))
        self.assertEqual(buf.text(), "中文输出\n")

    def test_multibyte_not_broken_at_boundary(self):
        """占位符插入位置为字节边界，渲染结果不抛异常。"""
        buf = ssh_server.TruncatingBuffer(9)  # head 4 / tail 5
        buf.append("中".encode("utf-8") * 10)  # 每字 3 字节
        text = buf.text()  # 不应抛 UnicodeDecodeError
        self.assertIn("truncated", text)


if __name__ == "__main__":
    unittest.main()
