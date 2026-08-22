package top.fateironist.cross_relay_core.model.proxy.tunnel.server;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Promise;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.proxy.ServerProxyContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

public class ServerUdpProxyContext extends ServerProxyContext {
    private final Map<InetSocketAddress, ServerUdpTunnelContext> addressContextMap = new ConcurrentHashMap<>();

    public ServerUdpProxyContext(String proxyId, List<ChannelHandlerContext> handlerContexts) {
        super(proxyId, TransportLayerProtocol.UDP, handlerContexts);
    }

    @Override
    public TunnelContext createNewTunnelContext(String tunnelId) {
        return new ServerUdpTunnelContext(tunnelId);
    }


    public Future<Object> registerRequester(String tunnelId, InetSocketAddress requesterAddress) {
        Promise<Object> promise = DefaultEventLoopGroup.newPromise();
        requesterWaitMap.put(tunnelId, promise);

        ServerUdpTunnelContext tunnelContext = (ServerUdpTunnelContext) tunnelRegisterMap.get(tunnelId);
        tunnelContext.setRequesterAddress(requesterAddress);

        addressContextMap.put(requesterAddress, tunnelContext);
        return promise;
    }

    public ServerUdpTunnelContext registerClientProxy(String tunnelId, InetSocketAddress clientProxyAddress) {
        Promise<Object> promise = requesterWaitMap.get(tunnelId);

        ServerUdpTunnelContext tunnelContext = null;

        if (promise != null) {

            tunnelContext = (ServerUdpTunnelContext) tunnelRegisterMap.get(tunnelId);
            tunnelContext.setClientProxyAddress(clientProxyAddress);

            promise.setSuccess(clientProxyAddress);
            requesterWaitMap.remove(tunnelId);
        }

        return tunnelContext;
    }
}
