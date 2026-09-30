# design 文档索引

> 本目录设计文档的统一索引。`taskorder` 为该条目**最后一次被 agent 复核并对齐的任务号**——它是「**本轮 agent 已复核该文档、并确认其与现有架构对齐**」的**凭证**，**不是**「内容最后一次被修改」的记录。因此每个非只读条目在**每轮任务结束时都必须等于** `docs/meta.json` 的当前任务号，**即使内容一字未改**；持续落后即视为该轮任务未完成。校验由本目录的 `docs/design/scripts/check_index.sh` 在 Stop hook 中自动执行；拨号**不提供脚本**，复核后直接在本表格里逐条手改（理由是拨号工具会把「已复核」这个断言稀释成橡皮图章）。`只读` 为是表示内容稳定、仅作参考，保鲜校验与拨号均跳过；为否表示会随开发演进持续更新。
> 表格为机器可解析格式：每行一个文件，列以 `|` 分隔，请按列名取值，不要依赖列顺序以外的格式。

| 文件地址 | 文件名 | 简要内容 | taskorder | 只读 |
|---|---|---|---|---|
| docs/design/prime-design.md | prime-design | 双层中继架构的设计动机：最严格 NAT 下的可达性约束推导、设计取舍与未来宽松 NAT 演进方向 | 0 | 是 |
| docs/design/项目架构介绍.md | 项目架构介绍 | 静态架构总览：模块树、Control/Proxy 角色定位、事件体系、钩子表、Context 体系与依赖关系；动态流程引至三大通道文档；引用一律符号名 | 23 | 否 |
| docs/design/control建立流程.md | control建立流程 | control 加密控制通道建立：UDP 元数据发现（备选）、握手时序（RSA/AES、认证、心跳）、Pipeline、钩子挂载时机、permit 后代理注册的业务层边界 | 23 | 否 |
| docs/design/TCP通道建立流程.md | TCP通道建立流程 | TCP 隧道建立：requester 触发 → REQUIRE_CHANNEL 挂起 → 客户端双连接 → 注册包配对 → OPEN 四段转发；含控制面/数据面闭环说明与已知问题 | 23 | 否 |
| docs/design/UDP通道建立流程.md | UDP通道建立流程 | UDP 通道建立：首包惰性建 tunnel、地址 map 分发、duplex channel、writeToOpposite 转发、双层超时；与 TCP 的差异对照表与已知问题 | 23 | 否 |
| docs/design/agent约束体系.md | agent约束体系 | 本文是**原理与设计取舍**的唯一归属地（CLAUDE.md 只留可执行条文、不写设计）：三层结构、任务号主线、三种文档生命周期的差异化校验口径、脚本按域分离、hook 挂载表、拨号工具的反面教训与已知边界 | 23 | 否 |
