package top.fateironist.cross_relay_core.model.proxy;

import io.netty.channel.ChannelHandlerContext;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ClientTcpTunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

import java.util.List;

public class ClientTcpProxyContext extends ClientProxyContext {
    public ClientTcpProxyContext(String proxyId, List<ChannelHandlerContext> handlerContexts, ControlContext controlContext) {
        super(proxyId, TransportLayerProtocol.TCP, handlerContexts, controlContext);
    }

    @Override
    public TunnelContext createNewTunnelContext(String tunnelId) {
        return new ClientTcpTunnelContext(tunnelId);
    }
}
