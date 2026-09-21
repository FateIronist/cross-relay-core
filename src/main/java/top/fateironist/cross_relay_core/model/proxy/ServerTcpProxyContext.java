package top.fateironist.cross_relay_core.model.proxy;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import top.fateironist.constack.Container;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.args.proxy_server.RequesterProxyServerStartArgs;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ServerTcpTunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

import java.util.List;

public class ServerTcpProxyContext extends ServerProxyContext {
    public ServerTcpProxyContext(Container parent, String proxyId, List<ChannelHandlerContext> handlerContexts, ControlContext controlContext) {
        super(parent, proxyId, TransportLayerProtocol.TCP, handlerContexts, controlContext);
    }

    @Override
    protected java.util.concurrent.Future<Object> start(Object... args) {
        RequesterProxyServerStartArgs serverArgs = (RequesterProxyServerStartArgs) args[0];
        this.requesterTimeout = serverArgs.getOptions().getRequesterTimeout();
        // 请求监听 channel 由所属 ProxyServer 在创建点绑定，经 channelHandlerContextList 持有并统一回收
        top.fateironist.constack.Promise<Object> promise = new top.fateironist.constack.Promise<>();
        promise.setSuccess(null);
        return promise;
    }

    @Override
    public TunnelContext createNewTunnelContext(Container parent, String tunnelId) {
        return new ServerTcpTunnelContext(parent, tunnelId);
    }

    public Future<Object> registerRequester(String tunnelId, Channel requesterChannel) {
        Promise<Object> promise = DefaultEventLoopGroup.newPromise();

        ServerTcpTunnelContext tunnelContext = (ServerTcpTunnelContext) tunnelRegisterMap.get(tunnelId);
        if (tunnelContext != null) tunnelContext.setRequesterChannel(requesterChannel);

        requesterWaitMap.put(tunnelId, promise);
        requireChannel(tunnelId, TransportLayerProtocol.TCP);
        // 注册超时看护：超时销毁隧道并关闭 requester 连接
        watchRegisterTimeout(tunnelId, promise, requesterChannel, tunnelContext);

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
