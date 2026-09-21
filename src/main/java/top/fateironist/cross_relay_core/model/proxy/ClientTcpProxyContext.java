package top.fateironist.cross_relay_core.model.proxy;

import io.netty.channel.ChannelHandlerContext;
import top.fateironist.constack.Container;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ClientTcpTunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

import java.util.List;

public class ClientTcpProxyContext extends ClientProxyContext {
    public ClientTcpProxyContext(Container parent, String proxyId, List<ChannelHandlerContext> handlerContexts, ControlContext controlContext) {
        super(parent, proxyId, TransportLayerProtocol.TCP, handlerContexts, controlContext);
    }

    @Override
    public TunnelContext createNewTunnelContext(Container parent, String tunnelId) {
        return new ClientTcpTunnelContext(parent, tunnelId);
    }
}
