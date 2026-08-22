package top.fateironist.cross_relay_core.model.proxy.tunnel.server;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Promise;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.proxy.ServerProxyContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

import java.util.List;
import java.util.concurrent.Future;

public class ServerTcpProxyContext extends ServerProxyContext {
    public ServerTcpProxyContext(String proxyId, List<ChannelHandlerContext> handlerContexts) {
        super(proxyId, TransportLayerProtocol.TCP, handlerContexts);
    }

    @Override
    public TunnelContext createNewTunnelContext(String tunnelId) {
        return new ServerTcpTunnelContext(tunnelId);
    }

    public Future<Object> registerRequester(String tunnelId) {
        Promise<Object> promise = DefaultEventLoopGroup.newPromise();
        requesterWaitMap.put(tunnelId, promise);
        return promise;
    }

    public ServerTcpTunnelContext registerClientProxy(String tunnelId, Channel channel) {
        Promise<Object> promise = requesterWaitMap.get(tunnelId);
        if (promise != null) {
            promise.setSuccess(channel);
            requesterWaitMap.remove(tunnelId);
        }

        return (ServerTcpTunnelContext) tunnelRegisterMap.get(tunnelId);
    }
}
