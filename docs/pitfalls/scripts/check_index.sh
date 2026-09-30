#!/usr/bin/env bash
# 任务收尾时检查【本域】(docs/pitfalls/) 索引的一致性。由 Stop hook 调用，stdout 会呈现给 agent。
# 域 = 本脚本所在目录的上一级（docs/pitfalls/），由脚本路径推导而非硬编码。
#
# 与 docs/{design,logs}/scripts/check_index.sh 的关键差异：【本域不校验 taskorder】。
#   踩坑记录不是每轮任务都有——大多数任务不踩新坑，硬性要求"每轮更新"会逼出凑数的垃圾条目。
#   因此这里只做一类检查：
#     登记↔磁盘双向对账：本域下的 .md 与 index.md 的登记项一一对应——
#     磁盘有而未登记（缺少索引）、登记而无文件（多余索引）均提醒。
#   index.md 里仍保留 taskorder 列，用于追溯"这个坑是在哪一轮踩到的"，但它【纯粹是记录】，
#   本脚本既不校验其数值，也不要求它等于当前任务号。
#
# 表格为机器可解析格式：按表头列名定位「文件地址」列，不依赖列顺序。
# 注意：本脚本与另外两个域的 check_index.sh 的表格解析代码是【有意各自内联】的，
#      不抽公共库——域之间彻底解耦，新增/删除一个域不影响另一个。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
DOMAIN="$(cd "$(dirname "$0")/.." && pwd)"

python3 - "$ROOT" "$DOMAIN" <<'PY'
import json, os, re, sys

root, domain = sys.argv[1], sys.argv[2]
tag = os.path.basename(domain)
domain_rel = os.path.relpath(domain, root)
idx = os.path.join(domain, "index.md")

idx_rel = os.path.relpath(idx, root)
if not os.path.exists(idx):
    print(f"[{tag}] 未找到 {idx_rel}，跳过检查。")
    sys.exit(0)

lines = open(idx, encoding="utf-8").read().splitlines()

# 按表头定位列，不依赖列顺序
header_i = sep_i = None
cols = []
for i, line in enumerate(lines):
    if re.match(r"^\s*\|.*\|\s*$", line):
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if all(re.fullmatch(r":?-{2,}:?", c) for c in cells):
            sep_i = i
            break
        if "文件地址" in cells:
            header_i, cols = i, cells

if header_i is None or sep_i is None or sep_i != header_i + 1:
    print(f"[{tag}] {idx_rel} 未找到可解析的表头/分隔行，跳过检查。")
    sys.exit(0)

def col(*names):
    for n in names:
        if n in cols:
            return cols.index(n)
    return None

c_path = col("文件地址", "路径")
if c_path is None:
    print(f"[{tag}] {idx_rel} 缺少「文件地址」列，跳过检查。")
    sys.exit(0)

# ---- 收集登记项（不读 taskorder：本域不做任何 taskorder 校验）----
registered, dangling = set(), []
for line in lines[sep_i + 1:]:
    m = re.match(r"^\s*\|(.+)\|\s*$", line)
    if not m:
        continue
    cells = [c.strip() for c in m.group(1).split("|")]
    if len(cells) <= c_path:
        continue
    path_rel = cells[c_path].strip()
    if not path_rel or set(path_rel) <= {"-", " "}:
        continue
    path = os.path.normpath(os.path.join(root, path_rel))
    name = os.path.relpath(path, root)

    if not os.path.exists(path):
        dangling.append(name)
        continue
    registered.add(path)

# ---- 登记↔磁盘双向对账（本域下的 .md，不含 index.md 自身）----
actual = {
    os.path.normpath(os.path.join(domain, f))
    for f in os.listdir(domain)
    if f.endswith(".md") and f != "index.md"
}
unindexed = sorted(os.path.relpath(p, root) for p in actual - registered)

if dangling or unindexed:
    print(f"[{tag}] {idx_rel} 登记与磁盘不一致，请维护索引：")
    for d in dangling:
        print(f"    - 多余索引（登记了但文件不存在）：{d}")
    for u in unindexed:
        print(f"    - 缺少索引（磁盘存在但未登记）：{u}")
else:
    print(f"[{tag}] {idx_rel} 登记与磁盘一致（共 {len(registered)} 条；本域不校验 taskorder）。")
PY
