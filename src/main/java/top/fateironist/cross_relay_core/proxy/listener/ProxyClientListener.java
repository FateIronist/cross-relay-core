package top.fateironist.cross_relay_core.proxy.listener;

import io.netty.util.concurrent.Future;
import top.fateironist.cross_relay_core.Listener;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

/**
 * Proxy 客户端侧的生命周期监听钩子：只承担「通知」角色，不承担任何核心编排逻辑——
 * 隧道建立、双通道配对、级联关闭全部内聚在核心库的 TunnelContext / ProxyContext 体系，业务层仅在此观测。
 * 默认空实现，由上层按需覆写。
 */
public class ProxyClientListener implements Listener {

    /**
     * 一条隧道建立完成（两端所需的 channel / address 齐备，tryOpen 判定为 OPEN）时回调。
     * TCP 在两条连接均 sync 成功、UDP 在 duplex channel bind 成功时触发；实现应快速返回，避免阻塞 Netty 事件循环。
     */
    public void onTunnelEstablished(TunnelContext context) {

    }

    /**
     * 隧道本端关闭（channelInactive 等触发点）时回调，此时上下文可能已开始释放，不应当再发起写操作。
     */
    public void onTunnelClose(TunnelContext context) {

    }

    /**
     * 隧道所在 channel 抛出异常时回调，不应当在此消费掉异常语义，仅作观测；context 可能为 null，实现需自行判空。
     */
    public void caughtTunnelException(TunnelContext context, Throwable cause) {

    }

}
