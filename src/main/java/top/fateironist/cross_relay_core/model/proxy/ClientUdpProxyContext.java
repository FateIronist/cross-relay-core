package top.fateironist.cross_relay_core.model.proxy.tunnel.client;

import io.netty.channel.ChannelHandlerContext;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.proxy.ClientProxyContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.server.ServerUdpProxyContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.server.ServerUdpTunnelContext;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class ClientUdpProxyContext extends ClientProxyContext {
    private final Map<InetSocketAddress, ClientUdpTunnelContext> addressContextMap = new ConcurrentHashMap<>();

    public ClientUdpProxyContext(String proxyId, List<ChannelHandlerContext> handlerContexts) {
        super(proxyId, TransportLayerProtocol.UDP, handlerContexts);
    }

    @Override
    public TunnelContext createNewTunnelContext(String tunnelId) {
        return new ClientUdpTunnelContext(tunnelId);
    }

    public TunnelContext newTunnelContext(String tunnelId, InetSocketAddress address) {
        TunnelContext tunnelContext = createNewTunnelContext(tunnelId);
        tunnelContext.setTunnelCloseHook(tunnelCloseHook());

        tunnelRegisterMap.put(tunnelContext.getTunnelId(), tunnelContext);
        addressContextMap.put(address, (ClientUdpTunnelContext) tunnelContext);

        return tunnelContext;
    }

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

    public ClientUdpTunnelContext getTunnelContext(InetSocketAddress address) {
        return addressContextMap.get(address);
    }
}
