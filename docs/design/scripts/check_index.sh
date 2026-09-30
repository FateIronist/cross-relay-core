#!/usr/bin/env bash
# 任务收尾时检查【本域】文档索引的一致性。由 Stop hook 调用，stdout 会呈现给 agent。
# 域 = 本脚本所在目录的上一级（docs/design/），由脚本路径推导而非硬编码。
# 因此本脚本只扫自己域下的 index.md —— docs/logs/ 有自己的
# docs/logs/scripts/check_index.sh，两者互不干扰，也就不需要任何「排除某目录」的补丁。
# 对本域下每个 index.md 做两类检查：
#   1. 文档保鲜提醒：登记的 taskorder 落后于 docs/meta.json 当前值 = 该文档自那之后
#      未随任务更新过。提醒 agent 结合本次任务内容复核文档描述是否已过时：
#      过时 → 更新文档内容并把登记值拨到当前任务号；确认无需更新 → 保持不动
#      （持续提醒属预期，它就是「可能过时」的常驻提示）。
#      「只读」列标记为「是/true/read-only」的文件、无写权限的文件跳过。
#   2. 登记↔磁盘双向对账：index.md 所在目录下的真实 .md 文件（不含 index.md 自身）
#      与登记项一一对应——磁盘有而未登记（缺少索引）、登记而无文件（多余索引）均提醒。
# 表格为机器可解析格式：按表头列名定位「文件地址 / taskorder / 只读」列，不依赖列顺序。
# 注意：本脚本与 docs/logs/scripts/check_index.sh 的表格解析代码是【有意各自内联】的，
#      不抽公共库——域之间彻底解耦，新增/删除一个域不影响另一个。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
DOMAIN="$(cd "$(dirname "$0")/.." && pwd)"

python3 - "$ROOT" "$DOMAIN" <<'PY'
import glob, json, os, re, sys

root, domain = sys.argv[1], sys.argv[2]
tag = os.path.basename(domain)
domain_rel = os.path.relpath(domain, root)

try:
    with open(os.path.join(root, "docs", "meta.json"), encoding="utf-8") as f:
        current = int(json.load(f)["taskorder"])
except Exception as e:
    print(f"[{tag}] 无法读取 docs/meta.json，跳过检查：{e}")
    sys.exit(0)

READONLY = {"是", "true", "yes", "y", "read-only", "readonly"}
stale, readonly, unwritable, malformed = [], [], [], []

indexes = sorted(glob.glob(os.path.join(domain, "**", "index.md"), recursive=True))
print(f"[{tag}] 当前任务号 {current}，检查 {domain_rel}/ 下 {len(indexes)} 个 index.md：")

for idx in indexes:
    idx_dir = os.path.dirname(idx)
    idx_rel = os.path.relpath(idx, root)
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
        print(f"  [索引] {idx_rel}：未找到可解析的表头/分隔行，跳过该索引")
        continue

    def col(*names):
        for n in names:
            if n in cols:
                return cols.index(n)
        return None

    c_path, c_order, c_ro = col("文件地址", "路径"), col("taskorder"), col("只读")
    if c_path is None or c_order is None:
        print(f"  [索引] {idx_rel}：缺少「文件地址」或「taskorder」列，跳过该索引")
        continue

    # ---- 收集登记项 ----
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

        if c_ro is not None and cells[c_ro].lower() in READONLY:
            readonly.append(name)
            continue
        if not os.access(path, os.W_OK):
            unwritable.append(name)
            continue
        try:
            order = int(cells[c_order])
        except (ValueError, TypeError):
            malformed.append(f"{name}（taskorder 列不是数字：{cells[c_order]!r}）")
            continue
        if order != current:
            stale.append(f"{name}（登记 {order}，自任务 {order} 后未更新，当前 {current}）")

    # ---- 登记↔磁盘双向对账（index.md 所在目录下的 .md，不含 index.md 自身）----
    actual = {
        os.path.normpath(os.path.join(idx_dir, f))
        for f in os.listdir(idx_dir)
        if f.endswith(".md") and f != "index.md"
    }
    unindexed = sorted(os.path.relpath(p, root) for p in actual - registered)
    if dangling or unindexed:
        print(f"  [索引] {idx_rel}：登记与磁盘不一致，请维护 index.md：")
        for d in dangling:
            print(f"    - 多余索引（登记了但文件不存在）：{d}")
        for u in unindexed:
            print(f"    - 缺少索引（磁盘存在但未登记）：{u}")

if stale:
    print(f"【文档保鲜提醒】以下条目的 taskorder 落后于当前任务号 {current}。请逐条复核：")
    print(f"  · 描述已过时 → 更新文档内容；")
    print(f"  · 无论是否改内容 → 复核后都必须把登记值拨到 {current}（登记「只读=是」的条目除外）。")
    print(f"  docs/design/ 的 taskorder 是「本轮已复核该文档、并确认与现有架构对齐」的凭证，")
    print(f"  不是「内容最后一次被修改」的记录——持续落后即视为该轮任务未完成。")
    print(f"  拨号方式：直接在本表格中逐条手改（本仓刻意不提供拨号脚本——工具会把该断言稀释成橡皮图章）。")
    for s in stale:
        print(f"  - {s}")
for items, label in ((malformed, "taskorder 列无法解析"), (readonly, "只读标记"),
                     (unwritable, "无写权限")):
    if items:
        print(f"[{tag}] 已跳过（{label}）：" + "；".join(items))

if not any((stale, malformed)):
    print(f"[{tag}] taskorder 全部同步。")
PY
