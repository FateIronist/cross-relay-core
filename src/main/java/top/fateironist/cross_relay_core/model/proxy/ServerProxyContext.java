package top.fateironist.cross_relay_core.model.proxy;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Promise;
import top.fateironist.constack.Container;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.control.ProxyControlEventEnum;
import top.fateironist.cross_relay_core.model.control.event.CommonInfo;
import top.fateironist.cross_relay_core.model.control.event.ControlEvent;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 服务端代理容器基类：持有 requester 注册等待表与注册超时看护。
 * requester 到达后需等待 client-proxy 端注册完成隧道才算建立，超时则隧道销毁、requester 侧关闭。
 */
public abstract class ServerProxyContext extends ProxyContext {
    /** 等待 client-proxy 注册的 requester 登记：tunnelId -> 注册完成信号 */
    protected final Map<String, Promise<Object>> requesterWaitMap = new ConcurrentHashMap<>();

    /** requester 注册超时（ms），超时未等到对端注册则隧道销毁 */
    protected volatile int requesterTimeout = 30000;

    public ServerProxyContext(Container parent, String proxyId, TransportLayerProtocol protocol, List<ChannelHandlerContext> handlerContexts, ControlContext controlContext) {
        super(parent, proxyId, protocol, handlerContexts, controlContext);
    }

    public void requireChannel(String tunnelId, TransportLayerProtocol protocol) {
        ControlEvent<CommonInfo> controlEvent = new ControlEvent<>(ProxyControlEventEnum.REQUIRE_CHANNEL.getType(), new CommonInfo(tunnelId, proxyId, protocol));
        controlClient(controlEvent);
    }

    /** 受控清理：隧道销毁时注销等待表项，并以失败完成尚在等待的注册方 */
    @Override
    public void unregisterTunnel(String tunnelId) {
        Promise<Object> pending = requesterWaitMap.remove(tunnelId);
        if (pending != null) {
            pending.setFailure(new IllegalStateException("tunnel disposed before requester registration completed"));
        }
        super.unregisterTunnel(tunnelId);
    }

    /**
     * requester 注册超时看护：超时未注册成功则销毁隧道并按协议回收 requester 侧资源。
     * channelToClose 仅 TCP 传入（每条隧道独立的连接）；UDP 传 null（共享数据报 channel 不归单个隧道所有）。
     */
    protected void watchRegisterTimeout(String tunnelId, Promise<Object> promise, Channel channelToClose, TunnelContext tunnel) {
        DefaultEventLoopGroup.GROUP.schedule(() -> {
            if (!requesterWaitMap.remove(tunnelId, promise)) {
                // 已注册成功或隧道已销毁（等待方已被 unregisterTunnel 完成）
                return;
            }
            promise.setFailure(new TimeoutException("requester registration timeout: " + tunnelId));
            if (tunnel != null) {
                tunnel.disposal();
            }
            if (channelToClose != null) {
                channelToClose.close();
            }
        }, requesterTimeout, TimeUnit.MILLISECONDS);
    }
}
