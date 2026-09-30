package top.fateironist.cross_relay_core.model.proxy.tunnel;

/**
 * 隧道状态机：由 TunnelContext 独占维护，是"半开隧道不漏数据"这一约束的判定依据。
 * 状态单向推进，转发方法仅在 OPEN 时生效，关闭路径会依次经过 CLOSING 到 CLOSED。
 */
public enum TunnelStatus {
    INIT, // 已创建，但两端资源（channel/address）尚未齐备，此时不转发任何数据
    OPEN, // 两端资源齐备，隧道可用，转发方法仅在此状态生效
    CLOSING, // 已进入关闭流程，正在做本地清理与通知对端，转发已停止
    CLOSED // 本地清理完成、隧道已出各注册表，不再被引用
}
