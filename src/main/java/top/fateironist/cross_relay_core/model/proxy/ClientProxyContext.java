package top.fateironist.cross_relay_core.model.proxy;

import io.netty.channel.ChannelHandlerContext;
import top.fateironist.constack.Container;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.control.ControlContext;

import java.util.List;

public abstract class ClientProxyContext extends ProxyContext {
    public ClientProxyContext(Container parent, String proxyId, TransportLayerProtocol protocol, List<ChannelHandlerContext> handlerContexts, ControlContext controlContext) {
        super(parent, proxyId, protocol, handlerContexts, controlContext);
    }
}
