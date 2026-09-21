package top.fateironist.cross_relay_core.model.proxy;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;
import top.fateironist.constack.Container;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.args.proxy_server.RequesterProxyServerStartArgs;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ServerUdpTunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class ServerUdpProxyContext extends ServerProxyContext {
    /** 地址路由索引：requester 地址 -> 隧道（UDP 无连接，按地址寻址隧道） */
    protected final Map<InetSocketAddress, ServerUdpTunnelContext> addressContextMap = new ConcurrentHashMap<>();

    /** 隧道空闲巡检任务（本容器调度持有，销毁时经 Effect 取消） */
    private volatile ScheduledFuture<?> checkTimeoutScheduler;

    public ServerUdpProxyContext(Container parent, String proxyId, List<ChannelHandlerContext> handlerContexts, ControlContext controlContext) {
        super(parent, proxyId, TransportLayerProtocol.UDP, handlerContexts, controlContext);
    }

    @Override
    protected java.util.concurrent.Future<Object> start(Object... args) {
        RequesterProxyServerStartArgs serverArgs = (RequesterProxyServerStartArgs) args[0];
        this.requesterTimeout = serverArgs.getOptions().getRequesterTimeout();
        // 请求监听 channel 由所属 ProxyServer 在创建点绑定，经 channelHandlerContextList 持有并统一回收
        checkTimeoutScheduler = DefaultEventLoopGroup.GROUP.scheduleAtFixedRate(() -> {
            for (ServerUdpTunnelContext tunnelContext : addressContextMap.values()) {
                tunnelContext.checkTimeout();
            }
        }, 10, 10, TimeUnit.SECONDS);
        // Effect：取消空闲巡检任务（本容器调度的定时任务）
        effect(c -> {
            ScheduledFuture<?> scheduler = checkTimeoutScheduler;
            if (scheduler != null) {
                scheduler.cancel(true);
            }
        });

        top.fateironist.constack.Promise<Object> promise = new top.fateironist.constack.Promise<>();
        promise.setSuccess(null);
        return promise;
    }

    @Override
    public TunnelContext createNewTunnelContext(Container parent, String tunnelId) {
        return new ServerUdpTunnelContext(parent, tunnelId);
    }

    public Future<Object> registerRequester(String tunnelId, InetSocketAddress requesterAddress, Channel requesterChannel) {
        Promise<Object> promise = DefaultEventLoopGroup.newPromise();
        requesterWaitMap.put(tunnelId, promise);

        ServerUdpTunnelContext tunnelContext = (ServerUdpTunnelContext) tunnelRegisterMap.get(tunnelId);
        tunnelContext.setRequester(requesterAddress, requesterChannel);

        addressContextMap.put(requesterAddress, tunnelContext);

        requireChannel(tunnelId, TransportLayerProtocol.UDP);
        // 注册超时看护：超时销毁隧道并清理地址路由（共享数据报 channel 不随单隧道关闭）
        watchRegisterTimeout(tunnelId, promise, null, tunnelContext);
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

    /** 受控清理：隧道销毁时注销地址路由索引（本层及父容器两层索引） */
    @Override
    public void unregisterTunnel(String tunnelId) {
        TunnelContext tunnel = tunnelRegisterMap.get(tunnelId);
        if (tunnel instanceof ServerUdpTunnelContext udpTunnel) {
            if (udpTunnel.getClientProxyAddress() != null) {
                addressContextMap.remove(udpTunnel.getClientProxyAddress(), udpTunnel);
                if (parent() instanceof top.fateironist.cross_relay_core.proxy.ProxyUdpServer server) {
                    server.unregisterProxyAddress(udpTunnel.getClientProxyAddress());
                }
            }
            if (udpTunnel.getRequesterAddress() != null) {
                addressContextMap.remove(udpTunnel.getRequesterAddress(), udpTunnel);
            }
        }
        super.unregisterTunnel(tunnelId);
    }

    public ServerUdpTunnelContext getTunnelContext(InetSocketAddress sender) {
        return addressContextMap.get(sender);
    }
}
