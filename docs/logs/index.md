# 日志索引

> 本目录存放 agent 的任务日志，正文统一放在 `docs/logs/changelogs/` 下，本文件是它们的唯一索引。
> **本索引采用倒排**：最新完成的日志排在表格最前，越往下越旧。
> 与 `docs/` 其它 `index.md` 的差异：日志是**追加型存档**，**只校验表格第一行**——倒排首行即最新一篇日志，其 `taskorder` 必须等于 `docs/meta.json` 的当前任务号（意为「最新日志须由当前任务产生」）；**其余历史行的 `taskorder` 不校验**（写于任务 N 的日志在任务 N+1 不该被判定为过时）。相似任务合并进同一日志文件时，把被复用那一行的 `taskorder` 拨到当前任务号，并保持该行在表格最前。
> 本检查由 `docs/logs/scripts/check_index.sh` 在 Stop hook 中执行，另做 `changelogs/` 的索引↔磁盘双向对账。该脚本只扫 `docs/logs/`，与 `docs/design/scripts/check_index.sh` 各管一域、互不干扰。
> 表格为机器可解析格式：每行一个文件，列以 `|` 分隔，请按列名取值，不要依赖列顺序以外的格式。

| 文件地址 | 文件名 | 简要内容 | taskorder | 完成时间 |
|---|---|---|---|---|
| docs/logs/changelogs/2026-09-30_15-27-45_日志体系搭建.md | 2026-09-30_15-27-45_日志体系搭建 | 搭建日志体系：确立 changelogs 目录、倒排索引、命名规范与 CLAUDE.md 日志维护条款；目录由仓库根迁至 docs/logs/；落地 check_index.sh（只校验倒排首行 + 索引↔磁盘对账）并接入 Stop hook；检查脚本按域分离至 docs/{design,logs,pitfalls}/scripts/；新增《Agent 约束体系设计》文档；新增 pitfalls 踩坑域与种子坑记录；修正 design 域 taskorder 的「复核凭证」语义（原规则「确认仍准确则不动」自相矛盾）；曾引入 bump_taskorder.sh 拨号脚本，因首次使用即产生 4 条假断言（橡皮图章）已于任务 21 移除，改回纯文本约束 + 手改；任务 22 把三域 taskorder 对照表升为 CLAUDE.md 独立小节并补齐「填什么值」列；任务 23 给 CLAUDE.md 瘦身——只留可执行条文（何时/做什么/禁止什么），原理全部移交 docs/design/agent约束体系.md | 23 | 2026-09-30 16:22:28 |
