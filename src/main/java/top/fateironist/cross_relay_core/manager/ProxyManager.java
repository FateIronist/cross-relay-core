package top.fateironist.cross_relay_core.manager;

import java.util.concurrent.Future;

/**
 * manager 层的 proxy 侧接口骨架：供上层业务在不直接接触 TunnelContext 体系的前提下管理隧道。
 * 当前仅有接口声明，尚无实现类与调用点。
 */
public interface ProxyManager {
    /**
     * 关闭指定隧道，对应 control 通道上的 TUNNEL_CLOSE 事件语义（隧道 graceful close 会经 closeRemoteTunnelHook 通知对端）。
     * 返回的 Future 用于获知关闭是否成功，而非单纯表示调用已受理。
     */
    Future<Boolean> closeTunnel(String tunnelId);
}
