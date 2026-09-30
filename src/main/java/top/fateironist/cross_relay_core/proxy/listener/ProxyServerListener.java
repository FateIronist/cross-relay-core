package top.fateironist.cross_relay_core.proxy.listener;

import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import top.fateironist.cross_relay_core.Listener;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

/**
 * Proxy 服务端数据面的生命周期与准入钩子，供上层观测与准入控制。
 * 只承担"通知"（onTunnelEstablished / onTunnelClose / caughtTunnelException）与"准入"（beforeXxxAccept）两种角色，
 * 不承担任何核心编排逻辑——通道建立、配对与隧道状态机全部内聚在核心库的 ProxyContext / TunnelContext 体系中；默认空实现/放行。
 */
public class ProxyServerListener implements Listener {

    /**
     * 内网客户端的回连到达 client-proxy 端口时触发（仅 TCP 有调用点；UDP 无连接事件，不触发）。
     * 返回 false 表示拒绝，实现方不需要自行关闭连接，调用方已对 false 的情况关闭该连接。
     */
    public boolean beforeClientToServerConnectionAccept(TransportLayerProtocol protocol, Channel serverChannel, Object remoteConnection) {
        return true;
    }

    /**
     * 外部 requester 到达时触发，先于 tunnel 建立。remoteConnection 的类型随协议不同：TCP 为新建的子 Channel，UDP 为数据包的来源地址 msg.sender()。
     * 返回 false 表示拒绝，实现方不需要自行清理：TCP 会关闭该连接，UDP 因 channel 多路复用而只是静默丢弃该数据包。
     */
    public boolean beforeRequesterToServerConnectionAccept(TransportLayerProtocol protocol, Channel serverChannel, Object remoteConnection) {
        return true;
    }

    /**
     * 隧道建立时触发。服务端侧的触发点是 requester 子连接的 channelActive：此时只表示外部连接已就绪，
     * 客户端回连的注册包配对可能尚未完成甚至已经失败，该回调不做区分，仅作观测点。
     */
    public void onTunnelEstablished(TunnelContext context) {

    }

    /**
     * 隧道关闭时触发，与 resource 回收同时进行。服务端侧的触发点是 requester 子连接断开（级联关闭的两个触发点之一）；
     * 注意 client-proxy 中继段断开目前不触发本回调（已知问题）。
     */
    public void onTunnelClose(TunnelContext context) {

    }

    /** 各端 exceptionCaught 时触发；回调返回后核心仍会执行收尾（已配对只关本地，未配对直接关连接） */
    public void caughtTunnelException(TunnelContext context, Throwable cause) {

    }


}
