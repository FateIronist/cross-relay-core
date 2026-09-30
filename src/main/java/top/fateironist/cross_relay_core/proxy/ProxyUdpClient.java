package top.fateironist.cross_relay_core.proxy;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import lombok.extern.slf4j.Slf4j;
import top.fateironist.cross_relay_core.Client;
import top.fateironist.cross_relay_core.ClientStatus;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.proxy_client.ProxyClientConnectArgs;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.control.event.CommonInfo;
import top.fateironist.cross_relay_core.model.proxy.ClientUdpProxyContext;
import top.fateironist.cross_relay_core.model.proxy.ProxyContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ClientUdpTunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;
import top.fateironist.cross_relay_core.proxy.listener.ProxyClientListener;
import top.fateironist.cross_relay_core.util.JsonUtil;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 客户端 UDP 数据面：一条隧道只 bind 一条 duplex NioDatagramChannel，同时承担「连服务端」与「连内网服务」两个方向——
 * 因 UDP 是包协议、每包自带来源地址（方向由 msg.sender() 判别），无需像 TCP 那样一隧道两条连接；
 * channelActive 绑定 duplexChannel / serviceAddress / serverProxyAddress 三要素（齐备即 tryOpen → OPEN）后随即发注册包，与 control 的交互统一收敛在 ProxyContext 体系。
 */
@Slf4j
public class ProxyUdpClient implements Client {
    // 服务端 client-proxy 端地址，既是注册包的目标，也是上行流量的目标
    private final InetSocketAddress serverProxyRequestAddress;
    private final EventLoopGroup workerGroup;
    // 生命周期通知钩子，仅做通知，不参与隧道编排
    private final ProxyClientListener listener;

    // proxyId → proxy 级上下文，跨隧道复用（见 createContext 的 computeIfAbsent），内部还维护「地址 → tunnel」的分发映射
    private final Map<String, ClientUdpProxyContext> proxyContextMap = new ConcurrentHashMap<>();

    // 客户端状态机：INIT → OPEN → CLOSING → CLOSED
    public volatile ClientStatus status = ClientStatus.INIT;

    /** 由 ProxyClient 聚合门面按协议实例化；构造时尚未 bind 任何 channel，绑定动作全部在 connect() 中 */
    public ProxyUdpClient(InetSocketAddress serverProxyRequestAddress, EventLoopGroup workerGroup, ProxyClientListener listener) {
        this.serverProxyRequestAddress = serverProxyRequestAddress;
        this.workerGroup = workerGroup;
        this.listener = listener;
    }

    /**
     * bind 一条 duplex NioDatagramChannel 即完成隧道建立：channelActive 中绑定三要素并发出注册包，三要素齐备时 tryOpen 置 OPEN。
     * 返回的 Future 在 bind().sync() 成功后完成；超时回收由 tunnel 级 30s checkTimeout() 兜底（UDP 感知不到对端消失）。
     * 注意：onTunnelEstablished 目前在 channelActive 与本方法末尾各回调一次，与 TCP 侧已修复的双触发问题不同，UDP 侧尚未收敛。
     */
    @Override
    public Future<Void> connect(AbstractArgs arg) {
        ProxyClientConnectArgs args = (ProxyClientConnectArgs) arg;
        Promise<Void> promise = workerGroup.next().newPromise();

        // 1. 按 proxyId 取/建 proxy 级上下文（跨隧道复用），再由它创建本隧道的上下文
        ClientUdpProxyContext proxyContext = createContext(args.getProxyId(), args.getControlContext());

        ClientUdpTunnelContext tunnelContext = (ClientUdpTunnelContext) proxyContext.newTunnelContext(args.getTunnelId());

        // 2. 只需一条 duplex channel：既向服务端 client-proxy 端收发，也向内网服务收发，方向由 msg.sender() 判别
        Bootstrap duplexBootstrap = new Bootstrap();
        duplexBootstrap.group(workerGroup)
                .channel(NioDatagramChannel.class)
                .handler(new ChannelInitializer<DatagramChannel>() {
                    @Override
                    protected void initChannel(DatagramChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new SimpleChannelInboundHandler<DatagramPacket>() {
                            /** duplex channel 收到任意数据包：按 msg.sender() 判别来源（内网服务或服务端），再反向转发，转发动作内聚在 ClientUdpTunnelContext.writeToOpposite() */
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {
                                ClientUdpProxyContext proxyContext = (ClientUdpProxyContext) ctx.channel().attr(ProxyContext.KEY).get();
                                ClientUdpTunnelContext tunnelContext = proxyContext.getTunnelContext(msg.sender());

                                log.debug("[ProxyClient] UDP [{}，L:{}，R:{}] SERVER-DATA-RECEIVED",
                                        tunnelContext.getTunnelId(), ctx.channel().localAddress(), msg.sender());
                                tunnelContext.writeToOpposite(msg.sender(), msg.content());
                            }

                            /** bind 完成即触发：绑定三要素（齐备则 tryOpen → OPEN）并向服务端发注册包；注意此处已回调一次 onTunnelEstablished */
                            @Override
                            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                proxyContext.addHandlerContext(ctx);
                                tunnelContext.setDuplexChannel(ctx.channel());
                                tunnelContext.setServiceAddress(args.getServiceAddress());
                                tunnelContext.setServerProxyAddress(serverProxyRequestAddress);
                                ctx.channel().attr(ProxyContext.KEY).set(proxyContext);
                                // 补挂 TunnelContext attr：channelInactive/exceptionCaught 按它取隧道上下文，缺失会导致级联关闭失效
                                ctx.channel().attr(TunnelContext.KEY).set(tunnelContext);

                                // 发送初始认证信息：UDP 无握手，注册包兼作「连接建立」语义，服务端 client-proxy 端以首包识别并按 proxyId 完成配对
                                ByteBuf byteBuf = ctx.alloc().buffer();
                                CommonInfo clientProxyRegisterDTO = new CommonInfo(args.getTunnelId(), args.getProxyId());
                                byteBuf.writeBytes(JsonUtil.OBJECT_MAPPER.writeValueAsBytes(clientProxyRegisterDTO));
                                tunnelContext.writeToServerProxyAndFlush(byteBuf);
                                byteBuf.release();

                                listener.onTunnelEstablished(tunnelContext);
                                log.debug("[ProxyClient] UDP [{}，L:{}，R:{}] SERVER-CONNECTOR-ACTIVE",
                                        tunnelContext.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

                            /** duplex channel 关闭：通知 listener 并尝试 closeGracefully() 级联释放；注意本 pipeline 只写了 ProxyContext.KEY attr，未写 TunnelContext.KEY，故此处 context 恒为 null，级联关闭实际不会执行 */
                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                ClientUdpTunnelContext context = (ClientUdpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyClient] UDP [{}，L:{}，R:{}] SERVER-CONNECTOR-INACTIVE",
                                        context != null ? context.getTunnelId() : tunnelContext.getTunnelId(),
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                listener.onTunnelClose(context);
                                if (context != null) context.closeGracefully();
                            }

                            /** duplex channel 异常：通知 listener 后按 closeLocal() 本地释放（不通知对端）；同上，context 恒为 null，实际走 ctx.close() */
                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ClientUdpTunnelContext context = (ClientUdpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyClient] UDP [{}，L:{}，R:{}] SERVER-CONNECTOR-EXCEPTION",
                                        context != null ? context.getTunnelId() : tunnelContext.getTunnelId(),
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress(), cause);

                                listener.caughtTunnelException(tunnelContext, cause);
                                if (context != null) context.closeLocal();
                                else ctx.close();
                            }
                        });
                    }
                });

        log.debug("[ProxyClient] [{}] Binding UDP duplex channel for service {} and server {}",
                tunnelContext.getTunnelId(), args.getServiceAddress(),
                serverProxyRequestAddress);

        // 3. 虚拟线程中等 bind 完成（channelActive 已在事件循环中执行过三要素绑定与注册包发送）
        Thread.ofVirtual().start(() -> {
            try {
                duplexBootstrap.bind().sync();
            } catch (InterruptedException e) {
                promise.setFailure(e);
                return;
            }

            // 4. bind 成功即隧道建立完成；此处是 onTunnelEstablished 的第二个调用点（首个在 channelActive，属已知双触发）
            listener.onTunnelEstablished(tunnelContext);
            promise.setSuccess(null);
        });

        // 5. bind 成功即把客户端状态推进到 OPEN
        promise.addListener(f -> {
            if (f.isSuccess()) {
                status = ClientStatus.OPEN;
            }
        });

        return promise;
    }

    /** 关闭某个 proxyId 下的全部隧道（实际由 ClientUdpProxyContext.close 逐隧道 closeGracefully）；仅在 OPEN 状态可用，否则返回失败 Future */
    public Future<?> closeProxy(String proxyId) {
        if (status == ClientStatus.OPEN) {
            return proxyContextMap.get(proxyId).close();
        }
        return DefaultEventLoopGroup.failFuture(new Exception("Proxy is not open"));
    }

    /**
     * 优雅关闭：置 CLOSING 后逐 proxy 级上下文调用 close()（内部会 closeGracefully 其下所有隧道并通知对端），
     * 全部完成后置 CLOSED；仅在 OPEN / INIT 状态有效，其余状态返回空 Future。
     */
    @Override
    public Future<?> close() {
        if (status == ClientStatus.OPEN || status == ClientStatus.INIT) {
            status = ClientStatus.CLOSING;
            Promise<?> promise = DefaultEventLoopGroup.newPromise();

            // 1. 在虚拟线程中串行等待各 proxy 关闭，避免阻塞调用方线程
            Thread.ofVirtual().start(() -> {
                proxyContextMap.forEach((k, v) -> {
                    try {
                        v.close().sync();
                    } catch (Exception e) {
                        promise.setFailure(e);
                    }
                });
            });
            promise.addListener(f -> status = ClientStatus.CLOSED);

            return promise;
        }

        return DefaultEventLoopGroup.emptyFuture();
    }

    /** 立即关闭：当前线程直接关闭各 proxy 级上下文，不等待结果也不向上抛出异常（关闭动作内部已吞掉异常），用于进程退出等场景 */
    @Override
    public void closeNow() {
        if (status == ClientStatus.OPEN || status == ClientStatus.INIT) {
            status = ClientStatus.CLOSING;
            proxyContextMap.forEach((k, v) -> {
                try {
                    v.close();
                } catch (Exception e) {

                }
            });
            status = ClientStatus.CLOSED;
        }
    }

    /** 按 proxyId 取或新建 proxy 级上下文：同一 proxyId 跨隧道复用同一实例；controlContext 在此注入，是 proxy 侧回发控制事件的唯一依赖 */
    public ClientUdpProxyContext createContext(String proxyId, ControlContext controlContext) {
        ClientUdpProxyContext context = proxyContextMap.computeIfAbsent(proxyId, k -> {
            ClientUdpProxyContext newContext = new ClientUdpProxyContext(k, new ArrayList<>(), controlContext);
            decorateContext(newContext);
            return newContext;
        });
        return context;
    }

    /** 为 proxy 级上下文挂 closeProxyHook：proxy 关闭时把自身从 proxyContextMap 移除，避免 map 残留（业务层可覆写本方法追加装饰） */
    public void decorateContext(ClientUdpProxyContext context) {
        context.setCloseProxyHook(ctx -> {
            proxyContextMap.remove(ctx.getProxyId());
        });
    }
}
