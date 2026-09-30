package top.fateironist.cross_relay_core.model.proxy;

import io.netty.channel.ChannelHandlerContext;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ClientUdpTunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * 客户端 UDP 代理上下文：比 TCP 版多维护一张地址路由表。
 * UDP 是包协议、无连接归属，客户端全局只有一条 duplex channel，收包后只能按来源地址判断这是内网服务的响应还是服务端的数据，
 * 故地址 map 在此充当"分发路由表"。
 */
public class ClientUdpProxyContext extends ClientProxyContext {
    // 地址 -> 隧道 的路由表；键涵盖服务端代理地址与内网服务地址两个方向，查表即代表"这个地址属于哪条隧道"
    private final Map<InetSocketAddress, ClientUdpTunnelContext> addressContextMap = new ConcurrentHashMap<>();

    /** 协议固定为 UDP */
    public ClientUdpProxyContext(String proxyId, List<ChannelHandlerContext> handlerContexts, ControlContext controlContext) {
        super(proxyId, TransportLayerProtocol.UDP, handlerContexts, controlContext);
    }

    /** 客户端 UDP 隧道以"单条 duplex channel + 内网服务地址 + 服务端地址"三要素齐备为 OPEN 条件 */
    @Override
    public TunnelContext createNewTunnelContext(String tunnelId) {
        return new ClientUdpTunnelContext(tunnelId);
    }

    /**
     * 带初始地址的建隧道重载：在基类的"挂出表钩子并入 tunnelRegisterMap"之外，额外把 address 预先登记进 addressContextMap。
     * 与基类版本有一处差异需注意：这里只挂了 tunnelCloseHook，没有挂 closeRemoteTunnelHook，
     * 若该隧道后续走 closeGracefully()，通知对端一步会因钩子为 null 而空指针。
     * 另：当前 main 源码中并无调用点（ProxyUdpClient 用的是基类的单参版本），故 addressContextMap 实际始终为空。
     */
    public TunnelContext newTunnelContext(String tunnelId, InetSocketAddress address) {
        TunnelContext tunnelContext = createNewTunnelContext(tunnelId);
        tunnelContext.setTunnelCloseHook(tunnelCloseHook());
        // 优雅关闭（closeGracefully）会无条件回调该钩子，缺失会导致 NPE
        tunnelContext.setCloseRemoteTunnelHook(closeRemoteTunnelHook());

        tunnelRegisterMap.put(tunnelContext.getTunnelId(), tunnelContext);
        addressContextMap.put(address, (ClientUdpTunnelContext) tunnelContext);

        return tunnelContext;
    }

    /**
     * 覆写出表钩子：除基类的"移出 tunnelRegisterMap"外，再按隧道记录的两条地址清 addressContextMap，
     * 避免隧道已关而地址映射残留，导致同一地址再也建不起新隧道。
     */
    @Override
    protected Consumer<TunnelContext> tunnelCloseHook() {
        return new Consumer<TunnelContext>() {
            @Override
            public void accept(TunnelContext context) {
                ClientUdpTunnelContext tunnelContext = (ClientUdpTunnelContext) context;
                ClientUdpProxyContext.super.tunnelCloseHook().accept(context);
                addressContextMap.remove(tunnelContext.getServerProxyAddress());
                addressContextMap.remove(tunnelContext.getServiceAddress());
            }
        };
    }

    /** 按包的对端地址反查隧道，供 ProxyUdpClient 收包时判向；未登记过的地址返回 null（地址表需先由地址版 newTunnelContext 填充） */
    public ClientUdpTunnelContext getTunnelContext(InetSocketAddress address) {
        return addressContextMap.get(address);
    }
}
