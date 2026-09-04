package top.fateironist.cross_relay_core.model.proxy;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ServerTcpTunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

import java.util.List;

public class ServerTcpProxyContext extends ServerProxyContext {
    public ServerTcpProxyContext(String proxyId, List<ChannelHandlerContext> handlerContexts, ControlContext controlContext) {
        super(proxyId, TransportLayerProtocol.TCP, handlerContexts, controlContext);
    }

    @Override
    public TunnelContext createNewTunnelContext(String tunnelId) {
        return new ServerTcpTunnelContext(tunnelId);
    }

    public Future<Object> registerRequester(String tunnelId, Channel requesterChannel) {
        Promise<Object> promise = DefaultEventLoopGroup.newPromise();

        ServerTcpTunnelContext tunnelContext = (ServerTcpTunnelContext) tunnelRegisterMap.get(tunnelId);
        if (tunnelContext != null) tunnelContext.setRequesterChannel(requesterChannel);

        requesterWaitMap.put(tunnelId, promise);

        requireChannel(tunnelId, TransportLayerProtocol.TCP);

        return promise;
    }

    public ServerTcpTunnelContext registerClientProxy(String tunnelId, Channel channel) {
        Promise<Object> promise = requesterWaitMap.get(tunnelId);
        ServerTcpTunnelContext tunnelContext = null;
        if (promise != null) {
            promise.setSuccess(channel);
            requesterWaitMap.remove(tunnelId);

            tunnelContext = (ServerTcpTunnelContext) tunnelRegisterMap.get(tunnelId);
            if (tunnelContext != null) tunnelContext.setClientProxyChannel(channel);
        }

        return tunnelContext;
    }
}
