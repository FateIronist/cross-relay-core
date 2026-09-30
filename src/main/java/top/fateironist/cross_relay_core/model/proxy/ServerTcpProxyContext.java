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

/**
 * 服务端 TCP 代理上下文：一个 proxyId 对应一个，且在这一端对应一个专属的 requester 监听（bind(0)，由 ProxyTcpServer 建立），
 * 该监听下到来的所有外部连接都归属本 proxyContext，每条连接再各自配对成一条隧道。
 */
public class ServerTcpProxyContext extends ServerProxyContext {
    /** 协议固定为 TCP */
    public ServerTcpProxyContext(String proxyId, List<ChannelHandlerContext> handlerContexts, ControlContext controlContext) {
        super(proxyId, TransportLayerProtocol.TCP, handlerContexts, controlContext);
    }

    /** 服务端 TCP 隧道由"外部 requester 连接 + 客户端回连连接"配对，二者齐备即 OPEN */
    @Override
    public TunnelContext createNewTunnelContext(String tunnelId) {
        return new ServerTcpTunnelContext(tunnelId);
    }

    /**
     * 外部 requester 连接到达时由 ProxyTcpServer 调用：把 requester channel 落到隧道上（隧道已由 newTunnelContext() 建好并入表），
     * 挂一个 Promise 进 requesterWaitMap，再经 control 发 REQUIRE_CHANNEL，最后把 Promise 交回给引导类做 future.get() 同步等待。
     * 时序上必须先入表再发 REQUIRE_CHANNEL：客户端回连极快时，注册包要靠 requesterWaitMap 里已有的 Promise 才能唤醒本次等待。
     */
    public Future<Object> registerRequester(String tunnelId, Channel requesterChannel) {
        Promise<Object> promise = DefaultEventLoopGroup.newPromise();

        ServerTcpTunnelContext tunnelContext = (ServerTcpTunnelContext) tunnelRegisterMap.get(tunnelId);
        if (tunnelContext != null) tunnelContext.setRequesterChannel(requesterChannel);

        requesterWaitMap.put(tunnelId, promise);

        requireChannel(tunnelId, TransportLayerProtocol.TCP);

        return promise;
    }

    /**
     * 客户端回连的注册包到达 client-proxy 端口时由 ProxyTcpServer 调用，是"控制面发起、数据面闭环"的闭环点。
     * 这是纯本地动作、不走 control：取出该 tunnelId 上挂起的 Promise 并 setSuccess(channel) 唤醒等待方，随后出表，再把 channel 落到隧道上（触发 tryOpen，双 channel 齐备即 OPEN）。
     * 返回 null 表示没有对应等待（隧道已关或 tunnelId 非法），调用方据此判定注册失败（ProxyTcpServer 会关闭该 channel）。
     */
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
