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

/**
 * 服务端 UDP 代理上下文：对应一条专属的 requester channel（bind(0)，由 ProxyUdpServer 建立），
 * 该 channel 被多条连接复用——同一 channel 上按 requester 地址区分不同隧道，因此除隧道表外还必须维护地址路由表。
 * 又因 UDP 没有断连事件，本类自带 10s 周期扫描定时器，作为隧道级 30s 无活动超时的触发兜底。
 */
public class ServerUdpProxyContext extends ServerProxyContext {
    // 地址 -> 隧道 的路由表，键为 requester 地址或 clientProxy 地址；requester 端收包后靠它决定"建新隧道"还是"投给既有隧道"
    protected final Map<InetSocketAddress, ServerUdpTunnelContext> addressContextMap = new ConcurrentHashMap<>();

    // 10s 周期扫描本 proxy 下所有隧道并触发 checkTimeout()：UDP 感知不到对端消失，只能靠这条定时器兜底回收资源；
    // 随上下文创建即启动（字段初始化即注册调度），close() 中会 cancel，故上下文一旦下线该定时器不再存活
    protected final ScheduledFuture<?> checkTimeoutScheduler = DefaultEventLoopGroup.GROUP.scheduleAtFixedRate((() -> {
        for (ServerUdpTunnelContext tunnelContext : addressContextMap.values()) {
            tunnelContext.checkTimeout();
        }
    }), 10, 10, TimeUnit.SECONDS);

    /** 协议固定为 UDP */
    public ServerUdpProxyContext(String proxyId, List<ChannelHandlerContext> handlerContexts, ControlContext controlContext) {
        super(proxyId, TransportLayerProtocol.UDP, handlerContexts, controlContext);
    }

    /** 服务端 UDP 隧道以"requester 与 clientProxy 各自的地址 + channel"四元组齐备为 OPEN 条件 */
    @Override
    public TunnelContext createNewTunnelContext(String tunnelId) {
        return new ServerUdpTunnelContext(tunnelId);
    }


    /**
     * requester 首包到达时由 ProxyUdpServer 调用（UDP 无连接建立事件，只能惰性建隧道）：
     * 先把 requester 的地址与 channel 落到隧道并登记地址路由，再挂 Promise 进 requesterWaitMap，最后经 control 发 REQUIRE_CHANNEL。
     * 注意：本方法对 tunnelRegisterMap.get(tunnelId) 的结果未做空判断（依赖调用方先 newTunnelContext()），传入未知 tunnelId 会直接空指针，这一点与 TCP 版不同。
     */
    public Future<Object> registerRequester(String tunnelId, InetSocketAddress requesterAddress, Channel requesterChannel) {
        Promise<Object> promise = DefaultEventLoopGroup.newPromise();
        requesterWaitMap.put(tunnelId, promise);

        // 与 TCP 版对称：未知 tunnelId 不强转调用，避免 NPE
        ServerUdpTunnelContext tunnelContext = (ServerUdpTunnelContext) tunnelRegisterMap.get(tunnelId);
        if (tunnelContext != null) tunnelContext.setRequester(requesterAddress, requesterChannel);

        addressContextMap.put(requesterAddress, tunnelContext);

        requireChannel(tunnelId, TransportLayerProtocol.UDP);
        return promise;
    }

    /**
     * 客户端 duplex channel 的注册包到达时由 ProxyUdpServer 调用，是 UDP 侧的"控制面发起、数据面闭环"闭环点。
     * 与 TCP 不同：UDP 的客户端回连不是一条新连接，而是"来源地址 + 同一条 client-proxy channel"，
     * 这里记下地址与 channel 后由 setClientProxy 补齐四元组触发 tryOpen，并 setSuccess(clientProxyAddress) 唤醒挂起的 REQUIRE_CHANNEL 等待方。
     * Promise 不存在（隧道已关或超时回收）时返回 null，调用方仅记日志、不回应，客户端对此无感知（这也是 UDP 注册失败无反馈的原因）。
     */
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

    /**
     * 覆写出表钩子：除基类的"移出 tunnelRegisterMap、清掉等待中的 Promise"外，再按 clientProxy/requester 两条地址清 addressContextMap，
     * 使同一地址在隧道关闭后可以再次建隧道（否则旧映射会把后续包投给已关闭的隧道）。
     */
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

    /** 关闭前先停掉周期超时扫描，避免上下文已下线而定时器仍在遍历、引用本上下文名下的隧道；其余编排走基类 */
    @Override
    public Future<?> close() {
        checkTimeoutScheduler.cancel(true);
        return super.close();
    }

    /** 按来源地址反查隧道；ProxyUdpServer 的 requester 端据此判断是首包（需建隧道并挂起）还是既有隧道的后续数据 */
    public ServerUdpTunnelContext getTunnelContext(InetSocketAddress sender) {
        return addressContextMap.get(sender);
    }

}
