package top.fateironist.cross_relay_core.proxy;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import lombok.extern.slf4j.Slf4j;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.Server;
import top.fateironist.cross_relay_core.ServerStatus;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.proxy_server.ClientProxyServerStartArgs;
import top.fateironist.cross_relay_core.model.args.proxy_server.RequesterProxyServerStartArgs;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.control.event.CommonInfo;
import top.fateironist.cross_relay_core.model.proxy.ProxyContext;
import top.fateironist.cross_relay_core.model.proxy.ServerUdpProxyContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ServerUdpTunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;
import top.fateironist.cross_relay_core.proxy.listener.ProxyServerListener;
import top.fateironist.cross_relay_core.util.JsonUtil;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * 服务端 UDP 数据面：在 requester 与 client-proxy 两端之间搬运数据报，明文、无加密。
 * 监听拓扑：client-proxy 端全局仅一条 NioDatagramChannel、所有客户端共享；requester 端（bind(0)）每个 ProxyContext 一条、一一对应。
 * UDP 无连接、channel 被多连接复用，对端一律靠 InetSocketAddress 区分，因此分发必须在两端维护地址 map；本类不直接触碰 control 通道。
 */
@Slf4j
public class ProxyUdpServer implements Server {
    private final EventLoopGroup workerGroup;
    // 生命周期钩子
    private final ProxyServerListener listener;

    // proxyId → 代理上下文；客户端首包注册包里的 proxyId 在此查
    private final Map<String, ServerUdpProxyContext> proxyContextIdMap = new ConcurrentHashMap<>();
    // 客户端来源地址 → 代理上下文；无连接协议下用来源地址识别客户端，相当于"会话表"
    private final Map<InetSocketAddress, ServerUdpProxyContext> proxyContextAddressMap = new ConcurrentHashMap<>();

    // 生命周期状态机
    public volatile ServerStatus status = ServerStatus.INIT;

    public ProxyUdpServer(EventLoopGroup workerGroup, ProxyServerListener listener) {
        this.workerGroup = workerGroup;
        this.listener = listener;
    }

    /**
     * 启动 client-proxy 监听，这是服务端 UDP 数据面的入口。仅允许在 INIT 状态调用，绑定成功后转 RUNNING。
     * 注意：requester 端 channel 不在此启动，而是由上层在状态变为 RUNNING 后针对每个 ProxyContext 单独调用 startUdpRequesterProxyServer 拉起。
     */
    @Override
    public Future<Void> start(AbstractArgs arg) {
        if (status == ServerStatus.INIT) {
            ClientProxyServerStartArgs args = (ClientProxyServerStartArgs) arg;
            return startUdpClientProxyServer(args).addListener(f -> {
                status = ServerStatus.RUNNING;
            });
        }

        return DefaultEventLoopGroup.failFuture(new Exception("Server is not in INIT state"));
    }


    /**
     * 绑定全局唯一的 client-proxy UDP 端口，所有客户端的注册包与隧道数据都进入这同一条 channel。
     * 该 channel 被多客户端复用，处理器只能靠 msg.sender() 查 proxyContextAddressMap 分辨客户端：查不到即视作新客户端，首包必为注册包。
     */
    public ChannelFuture startUdpClientProxyServer(ClientProxyServerStartArgs args) {
        var options = args.getOptions();
        Bootstrap udpClientProxyBootstrap = new Bootstrap();
        udpClientProxyBootstrap.group(workerGroup)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_RCVBUF, options.getMaxUdpReceiveBuffer())
                .option(ChannelOption.SO_SNDBUF, options.getMaxUdpSendBuffer())
                .handler(new ChannelInitializer<DatagramChannel>() {
                    @Override
                    protected void initChannel(DatagramChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        // 数据面 pipeline：按来源地址分拣，注册包与业务数据共用这一条 handler
                        pipeline.addLast(new SimpleChannelInboundHandler<DatagramPacket>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) throws Exception {
                                ServerUdpProxyContext proxyContext = proxyContextAddressMap.get(msg.sender());
                                ServerUdpTunnelContext tunnelContext = null;

                                // 来源地址不在会话表内 → 该客户端是首次出现，本包必是首包注册包
                                if (proxyContext == null) {
                                    // 1.解析注册包 CommonInfo{tunnelId, proxyId}；此处一次性 readString，假定注册包整包到达
                                    ByteBuf content = msg.content();
                                    String json = content.readString(content.readableBytes(), CharsetUtil.UTF_8);
                                    CommonInfo registerDTO = JsonUtil.OBJECT_MAPPER.readValue(json, CommonInfo.class);

                                    String tunnelId = registerDTO.getTunnelId();
                                    String proxyId = registerDTO.getProxyId ();

                                    // 2.按 proxyId 定位代理上下文，并先把来源地址登记进会话表，后续该地址的包不再走注册解析
                                    proxyContext = proxyContextIdMap.get(proxyId);

                                    if (proxyContext == null) {
                                        log.debug("[ProxyServer] UDP [L:{}] CLIENT-TO-SERVER-REGISTER-FAILED: unknown proxyContext id:{}", ctx.channel().localAddress(), proxyId);
                                        return;
                                    }

                                    proxyContextAddressMap.put(msg.sender(), proxyContext);

                                    // 3.用 tunnelId 与 requester 侧挂起的 Promise 配对（成功即唤醒服务端 future.get()）
                                    tunnelContext = proxyContext.registerClientProxy(tunnelId, msg.sender(), ctx.channel());

                                    if (tunnelContext != null) {
                                        log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] CLIENT-TO-SERVER-REGISTER",
                                                tunnelContext.getTunnelId(), ctx.channel().localAddress(), msg.sender());
                                        // 配对成功、四元组齐备（OPEN）后才回调，与 TCP 侧语义对齐
                                        listener.onTunnelEstablished(tunnelContext);
                                        return;
                                    }

                                    // 注册失败：回滚刚写入的会话表，避免来源地址永久指向脏路由；仅打日志不回错误包，客户端无失败感知（已知问题）
                                    proxyContextAddressMap.remove(msg.sender());
                                    log.debug("[ProxyServer] UDP [L:{}] CLIENT-TO-SERVER-REGISTER-FAILED: unknown tunnel id:{}", ctx.channel().localAddress(), tunnelId);
                                    return;

                                } else {
                                    // 已在会话表内 → 按来源地址取出隧道，走纯转发；地址无对应隧道时为 null（如隧道已关闭），跳过转发
                                    tunnelContext = proxyContext.getTunnelContext(msg.sender());
                                    if (tunnelContext == null) {
                                        log.debug("[ProxyServer] UDP [L:{}] UNKNOWN-TUNNEL for sender {}", ctx.channel().localAddress(), msg.sender());
                                        return;
                                    }
                                }

                                log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] CLIENT-DATA-RECEIVED",
                                        tunnelContext.getTunnelId(), ctx.channel().localAddress(), msg.sender());
                                tunnelContext.writeToRequesterAndFlush(msg.content());
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] CLIENT-TO-SERVER-ACTIVE",
                                        null, ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

                            /** UDP 无断连语义，此回调仅对应 channel 本身被关闭（该 channel 为所有客户端共享） */
                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                // fixme: 这里Udp的channel是多个连接复用的
                                log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] CLIENT-TO-SERVER-INACTIVE",
                                        null,
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

                            /** 注意：关闭的是所有客户端共享的那条 channel，会一并中断其余客户端 */
                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                // fixme: 这里Udp的channel是多个连接复用的
                                log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] CLIENT-TO-SERVER-EXCEPTION",
                                        null,
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress(), cause);

                                ctx.close();
                            }
                        });
                    }
                });

        log.debug("[ProxyServer] Binding UDP client-proxy server to port {}", options.getClientProxyPort());
        return udpClientProxyBootstrap.bind(options.getClientProxyPort());
    }

    /**
     * 为某个 ProxyContext 单独拉起 requester 端的 NioDatagramChannel（bind(0)，端口随机），接收外部请求者的数据报。
     * 必须在本服务器已 RUNNING 后调用；按拓扑约定，每个 ProxyContext 只应有一条这样的 channel，它被该上下文下所有 requester 复用。
     * 与 TCP 不同，UDP 没有连接建立事件，隧道由该 channel 上到达的首个数据包惰性触发创建。
     */
    public Future<Void> startUdpRequesterProxyServer(RequesterProxyServerStartArgs args) {
        if (status != ServerStatus.RUNNING) {
            return DefaultEventLoopGroup.failFuture(new Exception("Server is not in RUNNING state"));
        }

        var options = args.getOptions();
        Bootstrap reqBootstrap = new Bootstrap();
        reqBootstrap.group(workerGroup)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_RCVBUF, options.getMaxUdpReceiveBuffer())
                .option(ChannelOption.SO_SNDBUF, options.getMaxUdpSendBuffer())
                .handler(new ChannelInitializer<DatagramChannel>() {
                    @Override
                    protected void initChannel(DatagramChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        // fixme: IdleStateHandler 需要 IdleEventHandler 消费超时事件，当前无人处理，挂上也是无效的，先注释
                        // 该项失效后，UDP 的资源回收依赖客户端 tunnel 的 30s checkTimeout 与 ServerUdpProxyContext 的 10s 周期扫描
                        // pipeline.addLast(new IdleStateHandler(0, 0, options.getRequesterTimeout(), TimeUnit.MILLISECONDS));
                        // 数据面 pipeline：无连接事件，全靠首包惰性建 tunnel
                        pipeline.addLast(new SimpleChannelInboundHandler<DatagramPacket>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {
                                // 1.准入过滤：返回 false 时静默丢弃该包（不关 channel，因为该 channel 为多个 requester 复用）
                                if (!listener.beforeRequesterToServerConnectionAccept(TransportLayerProtocol.UDP, ctx.channel(), msg.sender())) {
                                    log.debug("[ProxyServer] REJECT requester {} (beforeRequesterToServerConnectionAccept returned false)", msg.sender());
                                    return;
                                }

                                ServerUdpProxyContext proxyContext = (ServerUdpProxyContext) ctx.channel().attr(ProxyContext.KEY).get();
                                ServerUdpTunnelContext tunnelContext = proxyContext.getTunnelContext(msg.sender());

                                // 2.该来源地址还没有隧道 → 首个数据包惰性触发建 tunnel（客户端此时尚未注册，不可能先建）
                                if (tunnelContext == null) {
                                    tunnelContext = (ServerUdpTunnelContext) proxyContext.newTunnelContext();
                                    tunnelContext.setRequester(msg.sender(), ctx.channel());

                                    log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] REQUESTER-ACTIVE",
                                            tunnelContext.getTunnelId(), ctx.channel().localAddress(), msg.sender());

                                    // 3.登记 requester 侧并经 control 通道发 REQUIRE_CHANNEL（本类不直接触碰 control）
                                    // 4.同步等待 Promise：客户端 duplex channel 发注册包调用 registerClientProxy 后才会被唤醒；失败仅记日志
                                    Future<Object> future = proxyContext.registerRequester(tunnelContext.getTunnelId(), msg.sender(), ctx.channel());

                                    try {
                                        future.get();
                                        log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] TUNNEL-ESTABLISHED",
                                                tunnelContext.getTunnelId(), ctx.channel().localAddress(), msg.sender());
                                    } catch (Exception e) {
                                        log.debug("[ProxyServer] UDP [{}] TUNNEL-ESTABLISH-FAILED", tunnelContext.getTunnelId(), e);
                                    }
                                    return;
                                }

                                // 5.已有隧道 → 纯转发给客户端回连地址
                                log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] REQUESTER-DATA-RECEIVED",
                                        tunnelContext.getTunnelId(), ctx.channel().localAddress(), msg.sender());

                                tunnelContext.writeToClientProxyAndFlush(msg.content());
                            }

                            /** 绑定成功：为这条 requester channel 创建专属的 ServerUdpProxyContext 并绑定到 channel */
                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                ServerUdpProxyContext proxyContext = createContext(ProxyContext.generateProxyId(), List.of(ctx), args.getControlContext());
                                ctx.channel().attr(ProxyContext.KEY).set(proxyContext);
                            }

                            /** UDP 无断连语义，该回调仅对应 channel 本身被关闭（此 channel 被该上下文下所有 requester 复用） */
                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                // fixme: 这里Udp的channel是多个连接复用的
                                log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] REQUESTER-TO-SERVER-INACTIVE",
                                        null,
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress());

                            }

                            /** 注意：关闭的是该上下文下所有 requester 共享的那条 channel */
                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                // fixme: 这里Udp的channel是多个连接复用的
                                log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] REQUESTER-TO-SERVER-EXCEPTION",
                                        null,
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress(), cause);

                                ctx.close();
                            }
                        });
                    }
                });

        log.debug("[ProxyServer] Binding UDP requester-proxy server");
        return reqBootstrap.bind(0);
    }

    /**
     * 关闭指定 proxyId 的代理上下文：出 map、通知对端 PROXY_CLOSE、逐隧道优雅关闭并关闭其 requester channel。
     * 注意：proxyId 不存在时 get 返回 null 会抛 NPE，未做空值保护。
     */
    public Future<?> closeProxy(String proxyId) {
        return proxyContextIdMap.get(proxyId).close();
    }

    /**
     * 优雅关闭：在虚拟线程中逐个关闭全部代理上下文（每个内部含通知对端、关闭隧道与 channel），成功后转 SHUTDOWN。
     * 注：当前实现未回收 client-proxy 监听本身。
     */
    @Override
    public java.util.concurrent.Future<?> shutdown() {
        if (status == ServerStatus.RUNNING || status == ServerStatus.INIT) {
            status = ServerStatus.STOPPING;

            Promise<?> promise = DefaultEventLoopGroup.newPromise();
            Thread.ofVirtual().start(() -> {
                proxyContextIdMap.forEach((k, v) -> {
                    try {
                        v.close().sync();
                    } catch (Exception e) {
                        promise.setFailure(e);
                    }
                });
            });

            promise.addListener(f -> status = ServerStatus.SHUTDOWN);
            return promise;
        }
        return DefaultEventLoopGroup.emptyFuture();
    }

    /** 立即关闭全部代理上下文，不等待结果（异常被吞掉） */
    @Override
    public void shutdownNow() {
        if (status == ServerStatus.RUNNING || status == ServerStatus.INIT) {
            status = ServerStatus.STOPPING;
            proxyContextIdMap.forEach((k, v) -> {
                try {
                    v.close();
                } catch (Exception e) {

                }
            });
            status = ServerStatus.SHUTDOWN;
        }
    }

    /**
     * 以 proxyId 为键获取或创建代理上下文；新建时会额外包装 tunnelCloseHook，
     * 使隧道关闭时不仅出 tunnelRegisterMap，还要按 requester / clientProxy 两个地址清理各级地址映射，避免路由残留。
     * 注意：装饰只在新建时执行，已存在的上下文不会重新装饰。
     */
    public ServerUdpProxyContext createContext(String proxyId, List<ChannelHandlerContext> handlerContexts, ControlContext controlContext) {
        ServerUdpProxyContext context = proxyContextIdMap.computeIfAbsent(proxyId, k -> {
            ServerUdpProxyContext newContext = new ServerUdpProxyContext(proxyId, handlerContexts, controlContext) {
                Consumer<TunnelContext> tunnelCloseHook = super.tunnelCloseHook();
                @Override
                protected Consumer<TunnelContext> tunnelCloseHook() {
                    return new Consumer<TunnelContext>() {
                        @Override
                        public void accept(TunnelContext context) {
                            ServerUdpTunnelContext tunnelContext = (ServerUdpTunnelContext) context;
                            tunnelCloseHook.accept(context);
                            addressContextMap.remove(tunnelContext.getClientProxyAddress());
                            addressContextMap.remove(tunnelContext.getRequesterAddress());

                            // 移除Address-代理上下文
                            proxyContextAddressMap.remove(tunnelContext.getClientProxyAddress());
                            proxyContextAddressMap.remove(tunnelContext.getRequesterAddress());
                        }
                    };
                }
            };

            decorateContext(newContext);
            return newContext;
        });
        return context;
    }

    /** 挂上关闭钩子，使上下文 close 时自动从 proxyContextIdMap 摘除，避免残留 */
    public void decorateContext(ServerUdpProxyContext context) {
        context.setCloseProxyHook(ctx -> {
            proxyContextIdMap.remove(ctx.getProxyId());
        });
    }
}
