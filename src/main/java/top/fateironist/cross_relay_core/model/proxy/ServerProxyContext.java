package top.fateironist.cross_relay_core.model.proxy;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Promise;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.control.ProxyControlEventEnum;
import top.fateironist.cross_relay_core.model.control.event.CommonInfo;
import top.fateironist.cross_relay_core.model.control.event.ControlEvent;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * 服务端侧 ProxyContext 的公共基类：在基类之上补充"服务端发起、数据面闭环"的挂起-唤醒机制。
 * 服务端收到外部 requester 后无法直接命令客户端开通道，只能经 control 通道发 REQUIRE_CHANNEL，
 * 并用 requesterWaitMap 里的 Promise 把这次请求挂起，等客户端在数据面通道上回的注册包来唤醒。
 * 该机制 TCP 与 UDP 完全同构。
 */
public abstract class ServerProxyContext extends ProxyContext{
    // tunnelId -> 挂起的 Promise：requireChannel 发出后等待客户端回连注册；唤醒者恒为数据面的 registerClientProxy（setSuccess 后出表）
    protected final Map<String, Promise<Object>> requesterWaitMap = new ConcurrentHashMap<>();

    /** 协议由更下层的 TCP/UDP 实现固定传入 */
    public ServerProxyContext(String proxyId, TransportLayerProtocol protocol, List<ChannelHandlerContext> handlerContexts, ControlContext controlContext) {
        super(proxyId, protocol, handlerContexts, controlContext);
    }


    /**
     * 覆写出表钩子：除基类的"移出 tunnelRegisterMap"外，同时清掉该 tunnelId 上仍在等待的 Promise，
     * 防止隧道已经关闭而等待方（引导类的 future.get()）永久挂起。
     */
    @Override
    protected Consumer<TunnelContext> tunnelCloseHook() {
        return new Consumer<TunnelContext>() {
            @Override
            public void accept(TunnelContext context) {
                ServerProxyContext.super.tunnelCloseHook().accept(context);
                requesterWaitMap.remove(context.getTunnelId());
            }
        };
    }

    /**
     * 经 control 通道向客户端下发 REQUIRE_CHANNEL，要求它为 tunnelId 建立数据面通道。
     * 事件体为 CommonInfo{tunnelId, proxyId, protocol}，客户端凭 proxyId 找到本地 proxy 上下文、凭 tunnelId 对齐隧道。
     * 注意：客户端侧的拦截驱动逻辑尚未实现（ProxyControlEventEnum 已预留枚举），即"发出"这一半是完整的，"客户端响应"仍是目标设计。
     */
    public void requireChannel(String tunnelId, TransportLayerProtocol protocol) {
        ControlEvent<CommonInfo> controlEvent = new ControlEvent<>(ProxyControlEventEnum.REQUIRE_CHANNEL.getType(), new CommonInfo(tunnelId, proxyId, protocol));
        controlClient(controlEvent);
    }

}
