#!/usr/bin/env bash
# 自增 docs/meta.json 的 taskorder，并把任务号输出到 stdout。
# 由 UserPromptSubmit hook 调用：该事件的 hook stdout 会注入 agent 上下文，
# 让 agent 全程知道「当前任务号」，并在任务开始时就去查看 docs/ 下各 index.md、
# 结合本次任务内容判断登记文档是否已过时（taskorder 落后 = 可能过时的常驻提示）。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
META="$ROOT/docs/meta.json"

python3 - "$META" <<'PY'
import json, sys

path = sys.argv[1]
try:
    with open(path, encoding="utf-8") as f:
        meta = json.load(f)
    order = int(meta.get("taskorder", 0))
except Exception:
    order = 0

order += 1
with open(path, "w", encoding="utf-8") as f:
    json.dump({"taskorder": order}, f, ensure_ascii=False, indent=2)
    f.write("\n")

print(f"【任务号】本次任务 taskorder = {order}（已自增并写入 docs/meta.json）。")
print("【文档保鲜】请查看 docs/ 下各 index.md：结合本次任务内容，判断登记文档（尤其 taskorder 落后的）描述是否已过时——过时则更新文档内容，并把该文档登记的 taskorder 拨到当前任务号；确认仍准确则不动。任务结束时的检查会再次提醒落后的文档。")
PY
