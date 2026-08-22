package top.fateironist.cross_relay_core.model.proxy;

import io.netty.channel.ChannelHandlerContext;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;

import java.util.List;

public abstract class ClientProxyContext extends ProxyContext{
    public ClientProxyContext(String proxyId, TransportLayerProtocol protocol, List<ChannelHandlerContext> handlerContexts) {
        super(proxyId, protocol, handlerContexts);
    }
}
