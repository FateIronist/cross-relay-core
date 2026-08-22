package top.fateironist.cross_relay_core.model.proxy;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Promise;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public abstract class ServerProxyContext extends ProxyContext{
    protected final Map<String, Promise<Object>> requesterWaitMap = new ConcurrentHashMap<>();

    public ServerProxyContext(String proxyId, TransportLayerProtocol protocol, List<ChannelHandlerContext> handlerContexts) {
        super(proxyId, protocol, handlerContexts);
    }

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

}
