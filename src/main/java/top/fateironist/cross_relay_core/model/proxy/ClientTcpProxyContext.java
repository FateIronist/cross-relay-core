package top.fateironist.cross_relay_core.model.proxy.tunnel.client;

import io.netty.channel.ChannelHandlerContext;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.proxy.ClientProxyContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

import java.util.List;

public class ClientTcpProxyContext extends ClientProxyContext {
    public ClientTcpProxyContext(String proxyId, List<ChannelHandlerContext> handlerContexts) {
        super(proxyId, TransportLayerProtocol.TCP, handlerContexts);
    }

    @Override
    public TunnelContext createNewTunnelContext(String tunnelId) {
        return new ClientTcpTunnelContext(tunnelId);
    }
}
