package top.fateironist.cross_relay_core.proxy;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
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
import top.fateironist.cross_relay_core.model.proxy.ClientTcpProxyContext;
import top.fateironist.cross_relay_core.model.proxy.ProxyContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ClientTcpTunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;
import top.fateironist.cross_relay_core.proxy.listener.ProxyClientListener;
import top.fateironist.cross_relay_core.util.JsonUtil;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 客户端 TCP 数据面：一条隧道对应两条连接——先连内网服务（serviceProxyBootstrap），再回连服务端 requester 端口（serverConnecterBootstrap）；
 * 双 channel 的配对、OPEN 判定与级联关闭全部内聚在 ClientTcpProxyContext / ClientTcpTunnelContext，本类只负责建连、挂 attr 与向上抛通知，
 * 不直接触碰 control 通道（控制面交互统一收敛在 ProxyContext 体系）。
 */
@Slf4j
public class ProxyTcpClient implements Client {
    // 服务端 requester 端口地址：每隧道一条回连连接的目标
    private final InetSocketAddress serverProxyRequestAddress;
    private final EventLoopGroup workerGroup;
    // 生命周期通知钩子，仅做通知，不参与隧道编排
    private final ProxyClientListener listener;

    // proxyId → proxy 级上下文，跨隧道复用（见 createContext 的 computeIfAbsent），并承担 proxy 级注册表职责
    private final Map<String, ClientTcpProxyContext> proxyContextMap = new ConcurrentHashMap<>();

    // 客户端状态机：INIT → OPEN → CLOSING → CLOSED
    public volatile ClientStatus status = ClientStatus.INIT;

    /** 由 ProxyClient 聚合门面按协议实例化；构造时尚未建立任何连接，建连动作全部在 connect() 中 */
    public ProxyTcpClient(InetSocketAddress serverProxyRequestAddress, EventLoopGroup workerGroup, ProxyClientListener listener) {
        this.serverProxyRequestAddress = serverProxyRequestAddress;
        this.workerGroup = workerGroup;
        this.listener = listener;
    }

    /**
     * 建立一条 TCP 隧道的两条连接，虚拟线程中顺序执行：
     * ① serviceProxyBootstrap 连内网服务 → channelActive 设 serviceChannel；② serverConnecterBootstrap 回连服务端 requester 端口 → channelActive 立即发 JSON 注册包。
     * 顺序刻意固定为「先 service 后 server」：保证注册包发出时内网服务侧已就绪，两端 tryOpen() 时不会出现长时间半开的 channel。
     * 返回的 Future 在两条连接均 sync 成功后完成，此时 tunnel 必然为 OPEN；异常只在 InterruptedException 时置为失败。
     */
    @Override
    public Future<Void> connect(AbstractArgs arg) {
        ProxyClientConnectArgs args = (ProxyClientConnectArgs) arg;
        Promise<Void> promise = workerGroup.next().newPromise();

        // 1. 按 proxyId 取/建 proxy 级上下文（跨隧道复用），再由它创建本隧道的上下文（tunnel 默认自动绑定 closeHook 与 closeRemoteTunnelHook）
        ClientTcpProxyContext proxyContext = createContext(args.getProxyId(), args.getControlContext());

        ClientTcpTunnelContext tunnelContext = (ClientTcpTunnelContext) proxyContext.newTunnelContext(args.getTunnelId());

        // 2. 内网服务侧连接：连上后写入 serviceChannel 并挂 attr，数据由此向服务端回连 channel 转发
        Bootstrap serviceProxyBootstrap = new Bootstrap();
        serviceProxyBootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                            /** 内网服务回包：原样转发给服务端回连 channel（writeToClientProxyAndFlush 仅在 tunnel OPEN 时真正转发，杜绝半开隧道漏数据） */
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                ClientTcpTunnelContext context = (ClientTcpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyClient] TCP [{}，L:{}，R:{}] SERVICE-DATA-RECEIVED",
                                        context.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                context.writeToClientProxyAndFlush(msg);
                            }

                            /** 内网服务连接就绪：登记 serviceChannel 并挂上 ProxyContext / TunnelContext 两个 attr；setServiceChannel 内部会自动 tryOpen()（此时另一侧尚未就绪，故仍为 INIT） */
                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                tunnelContext.setServiceChannel(ctx.channel());
                                ctx.channel().attr(ProxyContext.KEY).set(proxyContext);
                                ctx.channel().attr(TunnelContext.KEY).set(tunnelContext);
                                log.debug("[ProxyClient] TCP [{}，L:{}，R:{}] SERVICE-PROXY-ACTIVE",
                                        tunnelContext.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

                            /** 内网服务侧断开：这是 TCP 级联关闭的两个触发点之一——先通知 listener，再 closeGracefully()（本地出 map + 经 ProxyContext 通知对端 TUNNEL_CLOSE + 关闭双 channel） */
                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                ClientTcpTunnelContext context = (ClientTcpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyClient] TCP [{}，L:{}，R:{}] SERVICE-PROXY-INACTIVE",
                                        context != null ? context.getTunnelId() : tunnelContext.getTunnelId(),
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                listener.onTunnelClose(context);
                                if (context != null) context.closeGracefully();
                            }

                            /** 内网服务侧异常：通知 listener 后按 closeLocal() 本地释放（不通知对端）；attr 尚未绑定或已释放时退化为直接关 channel */
                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ClientTcpTunnelContext context = (ClientTcpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyClient] TCP [{}，L:{}，R:{}] SERVICE-PROXY-EXCEPTION",
                                        context != null ? context.getTunnelId() : tunnelContext.getTunnelId(),
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress(), cause);

                                listener.caughtTunnelException(context, cause);
                                if (context != null) context.closeLocal();
                                else ctx.close();
                            }

                        });
                    }
                });

        // 3. 服务端回连侧连接：连上后立即发注册包与服务端挂起的 tunnel 配对，数据由此向内网服务转发
        Bootstrap serverConnecterBootstrap = new Bootstrap();
        serverConnecterBootstrap.group(workerGroup).channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                            /** 服务端回连侧收到数据：原样转发给内网服务 channel（writeToServiceAndFlush 仅在 tunnel OPEN 时真正转发） */
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                ClientTcpTunnelContext context = (ClientTcpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyClient] TCP [{}，L:{}，R:{}] SERVER-DATA-RECEIVED",
                                        context.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                context.writeToServiceAndFlush(msg);
                            }

                            /** 服务端回连就绪：立即发 JSON 注册包完成配对，随后挂 attr 并 tryOpen()（service 侧早已就绪，故此处通常直接置 OPEN） */
                            @Override
                            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                tunnelContext.setClientProxyChannel(ctx.channel());

                                // 1. 发送初始认证信息：服务端 client-proxy pipeline 以「首包」识别注册包（该 channel 尚无 TunnelContext attr），按 proxyId 定位 proxyContext 后与本端 tunnel 配对
                                ByteBuf byteBuf = ctx.alloc().buffer();

                                CommonInfo clientProxyRegisterDTO = new CommonInfo(args.getTunnelId(), args.getProxyId());
                                byteBuf.writeBytes(JsonUtil.OBJECT_MAPPER.writeValueAsBytes(clientProxyRegisterDTO));
                                tunnelContext.writeToClientProxyAndFlush(byteBuf);

                                ctx.channel().attr(ProxyContext.KEY).set(proxyContext);
                                ctx.channel().attr(TunnelContext.KEY).set(tunnelContext);

                                log.debug("[ProxyClient] TCP [{}，L:{}，R:{}] SERVER-CONNECTOR-ACTIVE",
                                        tunnelContext.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

                            /** 回连侧（client-proxy 中继段）断开：目前仅记录日志，不触发级联关闭（已知问题），对端与本地资源的释放依赖服务端侧动作或超时兜底 */
                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                ClientTcpTunnelContext context = (ClientTcpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyClient] TCP [{}，L:{}，R:{}] SERVER-CONNECTOR-INACTIVE",
                                        context != null ? context.getTunnelId() : tunnelContext.getTunnelId(),
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

                            /** 回连侧异常：通知 listener 后按 closeLocal() 本地释放（不通知对端），此时 service 侧通道会被一并关闭 */
                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ClientTcpTunnelContext context = (ClientTcpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyClient] TCP [{}，L:{}，R:{}] SERVER-CONNECTOR-EXCEPTION",
                                        context != null ? context.getTunnelId() : tunnelContext.getTunnelId(),
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress(), cause);

                                listener.caughtTunnelException(tunnelContext, cause);
                                if (context != null) context.closeLocal();
                                else ctx.close();
                            }
                        });
                    }
                });

        log.debug("[ProxyClient] [{}] Connecting service-proxy to {} and server-connecter to {}",
                tunnelContext.getTunnelId(), args.getServiceAddress(), serverProxyRequestAddress);

        // 4. 用虚拟线程顺序 sync 两条连接：先内网服务、后服务端回连，确保注册包发出时内网服务侧已就绪
        Thread.ofVirtual().start(() -> {
            try {
                serviceProxyBootstrap.connect(args.getServiceAddress()).sync();
                serverConnecterBootstrap.connect(serverProxyRequestAddress).sync();
            } catch (InterruptedException e) {
                promise.setFailure(e);
                return;
            }

            // 5. 两条连接均就绪，tunnel 必然已 OPEN，此时才回调 listener（仅此一处调用点，避免重复通知）
            listener.onTunnelEstablished(tunnelContext);
            promise.setSuccess(null);
        });

        // 6. 建连成功即把客户端状态推进到 OPEN
        promise.addListener(f -> {
            if (f.isSuccess()) {
                status = ClientStatus.OPEN;
            }
        });

        return promise;
    }

    /** 关闭某个 proxyId 下的全部隧道（实际由 ClientTcpProxyContext.close 逐隧道 closeGracefully）；仅在 OPEN 状态可用，否则返回失败 Future */
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

    /** 按 proxyId 取或新建 proxy 级上下文：同一 proxyId 跨隧道复用同一实例，proxyContextMap 即 proxy 级注册表；controlContext 在此注入，是 proxy 侧回发控制事件的唯一依赖 */
    public ClientTcpProxyContext createContext(String proxyId, ControlContext controlContext) {
        ClientTcpProxyContext context = proxyContextMap.computeIfAbsent(proxyId, k -> {
            ClientTcpProxyContext newContext = new ClientTcpProxyContext(k, new ArrayList<>(), controlContext);
            decorateContext(newContext);
            return newContext;
        });
        return context;
    }
    
    /** 为 proxy 级上下文挂 closeProxyHook：proxy 关闭时把自身从 proxyContextMap 移除，避免 map 残留（业务层可覆写本方法追加装饰） */
    public void decorateContext(ClientTcpProxyContext context) {
        context.setCloseProxyHook(ctx -> {
            proxyContextMap.remove(ctx.getProxyId());
        });
    }
}
