#!/usr/bin/env bash
# 运行全部单元测试。
#
# 用法：
#   ./run_tests.sh            # 安静模式（只显示结果）
#   ./run_tests.sh -v         # 详细模式
set -euo pipefail

cd "$(dirname "$0")"

if [ "${1:-}" = "-v" ]; then
    exec python3 -m unittest discover -s tests -t . -v
fi

exec python3 -m unittest discover -s tests -t .
