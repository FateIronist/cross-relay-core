package top.fateironist.cross_relay_core.model.proxy;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ServerUdpTunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

public class ServerUdpProxyContext extends ServerProxyContext {
    protected final Map<InetSocketAddress, ServerUdpTunnelContext> addressContextMap = new ConcurrentHashMap<>();

    protected final ScheduledFuture<?> checkTimeoutScheduler = DefaultEventLoopGroup.GROUP.scheduleAtFixedRate((() -> {
        for (ServerUdpTunnelContext tunnelContext : addressContextMap.values()) {
            tunnelContext.checkTimeout();
        }
    }), 10, 10, TimeUnit.SECONDS);

    public ServerUdpProxyContext(String proxyId, List<ChannelHandlerContext> handlerContexts, ControlContext controlContext) {
        super(proxyId, TransportLayerProtocol.UDP, handlerContexts, controlContext);
    }

    @Override
    public TunnelContext createNewTunnelContext(String tunnelId) {
        return new ServerUdpTunnelContext(tunnelId);
    }


    public Future<Object> registerRequester(String tunnelId, InetSocketAddress requesterAddress, Channel requesterChannel) {
        Promise<Object> promise = DefaultEventLoopGroup.newPromise();
        requesterWaitMap.put(tunnelId, promise);

        ServerUdpTunnelContext tunnelContext = (ServerUdpTunnelContext) tunnelRegisterMap.get(tunnelId);
        tunnelContext.setRequester(requesterAddress, requesterChannel);

        addressContextMap.put(requesterAddress, tunnelContext);

        requireChannel(tunnelId, TransportLayerProtocol.UDP);
        return promise;
    }

    public ServerUdpTunnelContext registerClientProxy(String tunnelId, InetSocketAddress clientProxyAddress, Channel clientProxyChannel) {
        Promise<Object> promise = requesterWaitMap.get(tunnelId);

        ServerUdpTunnelContext tunnelContext = null;

        if (promise != null) {
            tunnelContext = (ServerUdpTunnelContext) tunnelRegisterMap.get(tunnelId);

            promise.setSuccess(clientProxyAddress);
            requesterWaitMap.remove(tunnelId);

            tunnelContext.setClientProxy(clientProxyAddress, clientProxyChannel);
        }

        return tunnelContext;
    }

    @Override
    protected Consumer<TunnelContext> tunnelCloseHook() {
        return new Consumer<TunnelContext>() {
            @Override
            public void accept(TunnelContext context) {
                ServerUdpTunnelContext tunnelContext = (ServerUdpTunnelContext) context;
                ServerUdpProxyContext.super.tunnelCloseHook().accept(context);
                addressContextMap.remove(tunnelContext.getClientProxyAddress());
                addressContextMap.remove(tunnelContext.getRequesterAddress());
            }
        };
    }

    @Override
    public Future<?> close() {
        checkTimeoutScheduler.cancel(true);
        return super.close();
    }

    public ServerUdpTunnelContext getTunnelContext(InetSocketAddress sender) {
        return addressContextMap.get(sender);
    }

}
