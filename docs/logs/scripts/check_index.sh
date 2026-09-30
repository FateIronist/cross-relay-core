#!/usr/bin/env bash
# 任务收尾时检查【本域】日志索引的一致性。由 Stop hook 调用，stdout 会呈现给 agent。
# 域 = 本脚本所在目录的上一级（docs/logs/），由脚本路径推导而非硬编码。
# 与 docs/design/scripts/check_index.sh 的关键差异：日志是【追加型存档】，不做逐行保鲜校验。
#   1. 只校验【倒排首行】= 最新一篇日志：其 taskorder 必须等于 docs/meta.json 的当前任务号，
#      即「最新日志必须由当前任务产生」——这是逼出「每轮任务都落日志」的强制项。
#      相似任务合并进同一日志文件时，agent 需把被复用那一行的 taskorder 拨到当前任务号，
#      并保持该行仍在表格最前（它被多个任务共用，最新共用的任务即当前任务）。
#      其余历史行的 taskorder 【不校验】（写于任务 N 的日志在任务 N+1 不该被判定为过时）。
#   2. 登记↔磁盘双向对账：本域 changelogs/ 下的 .md 与登记项一一对应——
#      磁盘有而未登记（缺少索引）、登记而无文件（多余索引）均提醒。
# 表格为机器可解析格式：按表头列名定位「文件地址 / taskorder」列，不依赖列顺序。
# 注意：本脚本与 docs/design/scripts/check_index.sh 的表格解析代码是【有意各自内联】的，
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
logdir = os.path.join(domain, "changelogs")

try:
    with open(os.path.join(root, "docs", "meta.json"), encoding="utf-8") as f:
        current = int(json.load(f)["taskorder"])
except Exception as e:
    print(f"[{tag}] 无法读取 docs/meta.json，跳过检查：{e}")
    sys.exit(0)

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
        if "文件地址" in cells or "taskorder" in cells:
            header_i, cols = i, cells

if header_i is None or sep_i is None or sep_i != header_i + 1:
    print(f"[{tag}] {idx_rel} 未找到可解析的表头/分隔行，跳过检查。")
    sys.exit(0)

def col(*names):
    for n in names:
        if n in cols:
            return cols.index(n)
    return None

c_path, c_order = col("文件地址", "路径"), col("taskorder")
if c_path is None or c_order is None:
    print(f"[{tag}] {idx_rel} 缺少「文件地址」或「taskorder」列，跳过检查。")
    sys.exit(0)

# ---- 收集登记项（保持表格行序，倒排 = 首行最新）----
rows, registered, dangling, malformed = [], set(), [], []
for line in lines[sep_i + 1:]:
    m = re.match(r"^\s*\|(.+)\|\s*$", line)
    if not m:
        continue
    cells = [c.strip() for c in m.group(1).split("|")]
    if len(cells) <= max(c_path, c_order):
        continue
    path_rel = cells[c_path].strip()
    if not path_rel or set(path_rel) <= {"-", " "}:
        continue
    path = os.path.normpath(os.path.join(root, path_rel))
    name = os.path.relpath(path, root)

    rows.append((name, cells[c_order]))
    if not os.path.exists(path):
        dangling.append(name)
        continue
    registered.add(path)
    try:
        int(cells[c_order])
    except (ValueError, TypeError):
        malformed.append(f"{name}（taskorder 列不是数字：{cells[c_order]!r}）")

# ---- 检查 1：只校验倒排首行 ----
if not rows:
    print(f"[{tag}] {idx_rel} 表格为空，本任务尚未登记日志。")
    print(f"[{tag}] 请于任务收尾时在 {domain_rel}/changelogs/ 落一篇日志，并在表格【最上方】登记，taskorder 填 {current}。")
else:
    first_name, first_order = rows[0]
    try:
        first = int(first_order)
    except (ValueError, TypeError):
        print(f"[{tag}] 首行日志 {first_name} 的 taskorder 列无法解析：{first_order!r}")
    else:
        if first != current:
            print(f"[{tag}] 最新日志 taskorder 未跟上：{idx_rel} 首行「{first_name}」登记 {first}，当前任务号为 {current}。")
            print(f"[{tag}] 倒排首行应是最新日志且由当前任务产生——请新写一篇日志，或把本轮任务合并进该文件（追加小节），")
            print(f"[{tag}] 二者都需把首行 taskorder 拨到 {current}，并保持该行在表格最前。")

# ---- 检查 2：登记↔磁盘双向对账 ----
actual = set()
if os.path.isdir(logdir):
    actual = {
        os.path.normpath(os.path.join(logdir, f))
        for f in os.listdir(logdir)
        if f.endswith(".md") and f != "index.md"
    }
unindexed = sorted(os.path.relpath(p, root) for p in actual - registered)
if dangling or unindexed:
    print(f"[{tag}] {idx_rel} 登记与磁盘不一致，请维护索引：")
    for d in dangling:
        print(f"    - 多余索引（登记了但文件不存在）：{d}")
    for u in unindexed:
        print(f"    - 缺少索引（磁盘存在但未登记）：{u}")

if malformed:
    print(f"[{tag}] 已跳过（taskorder 列无法解析）：" + "；".join(malformed))

if rows and not dangling and not unindexed and not malformed:
    first_name, first_order = rows[0]
    try:
        if int(first_order) == current:
            print(f"[{tag}] 最新日志 {first_name} 的 taskorder 已同步至 {current}，登记与磁盘一致。")
    except (ValueError, TypeError):
        pass
PY
