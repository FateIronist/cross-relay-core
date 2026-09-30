package top.fateironist.cross_relay_core.proxy;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
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
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;
import top.fateironist.cross_relay_core.model.proxy.ServerTcpProxyContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ServerTcpTunnelContext;
import top.fateironist.cross_relay_core.proxy.listener.ProxyServerListener;
import top.fateironist.cross_relay_core.util.JsonUtil;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 服务端 TCP 数据面：建立并维持 requester ⇄ client-proxy 两段连接之间的隧道，转发明文裸字节流（pipeline 无编解码、无加密）。
 * 监听拓扑是理解全部分拣逻辑的前提：client-proxy 端口（默认 3418）全局仅一个、所有客户端共享，靠回连连接的首包 JSON 注册包按 proxyId 分拣；
 * requester 端口（bind(0) 随机）每个 ProxyContext 一个、一一对应，天然按监听隔离。本类不直接触碰 control 通道，与 control 的交互全部经 ProxyContext 体系的 registerRequester 完成。
 */
@Slf4j
public class ProxyTcpServer implements Server {
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    // 生命周期钩子
    private final ProxyServerListener listener;

    // proxyId → 代理上下文；客户端回连时按注册包里的 proxyId 在此定位上下文，跨隧道复用
    private final Map<String, ServerTcpProxyContext> proxyContextMap = new ConcurrentHashMap<>();

    // 生命周期状态机
    public volatile ServerStatus status = ServerStatus.INIT;

    public ProxyTcpServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup, ProxyServerListener listener) {
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.listener = listener;
    }

    /**
     * 启动 client-proxy 监听，这是服务端 TCP 数据面的入口。仅允许在 INIT 状态调用，绑定成功后转 RUNNING。
     * 注意：requester 监听不在此启动，而是由上层在状态变为 RUNNING 后针对每个 ProxyContext 单独调用 startTcpRequesterProxyServer 拉起。
     */
    @Override
    public Future<Void> start(AbstractArgs arg) {
        if (status == ServerStatus.INIT) {
            ClientProxyServerStartArgs args = (ClientProxyServerStartArgs) arg;
            return startTcpClientProxyServer(args).addListener(f -> {
                status = ServerStatus.RUNNING;
            });
        }

        return DefaultEventLoopGroup.failFuture(new Exception("Server is not in INIT state"));
    }


    /**
     * 绑定全局唯一的 client-proxy 端口，接收所有内网客户端的回连（该端口被全部客户端共享）。
     * 父 handler 只做准入过滤；子 pipeline 极简，仅一个 ByteBuf handler，业务数据为明文。
     * 分拣依据是连接上是否已有 TunnelContext.KEY attr：注册包一定是该连接的首包且此时 attr 为空，注册完成后连接转为纯转发。
     */
    public ChannelFuture startTcpClientProxyServer(ClientProxyServerStartArgs args) {
        var options = args.getOptions();
        ServerBootstrap tcpClientProxyBootstrap = new ServerBootstrap();
        tcpClientProxyBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new ChannelInboundHandlerAdapter() {
                    /** 父 handler：msg 即刚 accept 出来的子连接。准入通过就放行给 acceptor 注册，否则直接关闭 */
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) {
                        Channel childChannel = (Channel) msg;

                        if (listener.beforeClientToServerConnectionAccept(TransportLayerProtocol.TCP, ctx.channel(), childChannel)) {
                            log.debug("[ProxyServer] ACCEPT client channel {} -> {}", childChannel.remoteAddress(), childChannel.localAddress());

                            ctx.fireChannelRead(msg);
                        } else {
                            childChannel.unsafe().close(childChannel.voidPromise());
                            log.debug("[ProxyServer] REJECT client channel {} (beforeClientToServerChannelAccept returned false)", childChannel.remoteAddress());
                        }
                    }
                })
                .option(ChannelOption.SO_BACKLOG, options.getMaxTcpConnections())
                .childOption(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        // 数据面 child pipeline：只做隧道级的分拣与转发，bytes 原样透传
                        pipeline.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
                                ServerTcpProxyContext proxyContext = (ServerTcpProxyContext) ctx.channel().attr(ProxyContext.KEY).get();
                                ServerTcpTunnelContext tunnelContext = (ServerTcpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();

                                // tunnelContext 为空说明该连接尚未绑定隧道，则本包必是首包注册包
                                if (tunnelContext == null) {

                                    // 1.解析注册包 CommonInfo{tunnelId, proxyId}；此处一次性 readString，假定注册包整包到达，无粘包/拆包处理
                                    String json = msg.readString(msg.readableBytes(), CharsetUtil.UTF_8);
                                    CommonInfo registerDTO = JsonUtil.OBJECT_MAPPER.readValue(json, CommonInfo.class);

                                    String tunnelId = registerDTO.getTunnelId();
                                    String proxyId = registerDTO.getProxyId();

                                    // 2.按 proxyId 定位代理上下文（共享的 client-proxy 端口靠它区分不同客户端），并绑定到该连接
                                    proxyContext = proxyContextMap.get(proxyId);
                                    ctx.channel().attr(ProxyContext.KEY).set(proxyContext);

                                    // 3.用 tunnelId 与 requester 侧挂起的 Promise 配对（成功即唤醒服务端 future.get()），此后本连接转为纯转发
                                    tunnelContext = proxyContext.registerClientProxy(tunnelId, ctx.channel());

                                    if (tunnelContext == null) {
                                        log.debug("[ProxyServer] TCP [L:{}] CLIENT-TO-SERVER-REGISTER-FAILED: unknown tunnel {}", ctx.channel().localAddress(), json);
                                        ctx.close();
                                        return;
                                    }

                                    ctx.channel().attr(TunnelContext.KEY).set(tunnelContext);

                                    // 4.配对成功、双 channel 齐备（OPEN）后才回调，与客户端侧「隧道必然 OPEN」语义对齐
                                    listener.onTunnelEstablished(tunnelContext);

                                    log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] CLIENT-TO-SERVER-REGISTER",
                                            tunnelContext.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                    return;
                                }

                                // 4.已配对连接的后续包：原样搬给 requester 侧
                                log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] CLIENT-DATA-RECEIVED",
                                        tunnelContext.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                tunnelContext.writeToRequesterAndFlush(msg);
                            }

                            /** 此时注册包尚未到达，还不知道该连接归属哪条隧道，故 tunnelId 只能记 null */
                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] CLIENT-TO-SERVER-ACTIVE",
                                        null, ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

                            /**
                             * 中继段（client-proxy）断开。注意：当前仅记日志、不级联关闭，服务端 requester 侧不会收到任何通知（已知问题）
                             */
                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                ServerTcpTunnelContext context = (ServerTcpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] CLIENT-TO-SERVER-INACTIVE",
                                        context != null ? context.getTunnelId() : null,
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

                            /** 回调 listener 后仍由核心负责收尾：已配对则只关本地（不通知对端），未配对则直接关连接 */
                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ServerTcpTunnelContext context = (ServerTcpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] CLIENT-TO-SERVER-EXCEPTION",
                                        context != null ? context.getTunnelId() : null,
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress(), cause);

                                listener.caughtTunnelException(context, cause);
                                if (context != null) {
                                    context.closeLocal();
                                } else {
                                    ctx.close();
                                }
                            }
                        });
                    }
                });

        log.debug("[ProxyServer] Binding TCP client-proxy server to port {}", options.getClientProxyPort());
        return tcpClientProxyBootstrap.bind(options.getClientProxyPort());
    }

    /**
     * 为某个 ProxyContext 单独拉起 requester 监听（bind(0)，端口随机），接收外部请求者的连接。
     * 必须在本服务器已 RUNNING 后调用；按拓扑约定，每个 ProxyContext 只应有一个这样的监听，因此同一监听下的 requester 天然属于同一代理上下文。
     * 父 handler 在 channelActive（监听绑定成功）时创建并绑定上下文，在 channelRead（外部连接到达）时完成准入与隧道建立。
     */
    public Future<Void> startTcpRequesterProxyServer(RequesterProxyServerStartArgs args) {
        if (status != ServerStatus.RUNNING) {
            return DefaultEventLoopGroup.failFuture(new Exception("Server is not in RUNNING state"));
        }

        var options = args.getOptions();
        ServerBootstrap reqBootstrap = new ServerBootstrap();
        reqBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new ChannelInboundHandlerAdapter() {
                    /**
                     * 父 handler：msg 为外部 requester 的新连接。一条 requester 连接 = 一条完整隧道，在此建 tunnel 并挂起等待客户端回连注册。
                     */
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) {
                        ServerTcpProxyContext proxyContext = (ServerTcpProxyContext) ctx.channel().attr(ProxyContext.KEY).get();
                        Channel childChannel = (Channel) msg;

                        if (listener.beforeRequesterToServerConnectionAccept(TransportLayerProtocol.TCP, ctx.channel(), childChannel)) {
                            log.debug("[ProxyServer] ACCEPT requester {} -> {}", childChannel.remoteAddress(), childChannel.localAddress());

                            // 1.外部流量到达即触发建 tunnel：newTunnelContext 会生成 tunnelId 并挂好出 map / 通知对端两个生命周期钩子
                            ServerTcpTunnelContext context = (ServerTcpTunnelContext) proxyContext.newTunnelContext();
                            childChannel.attr(TunnelContext.KEY).set(context);

                            log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] REQUESTER-ACTIVE",
                                    context.getTunnelId(), childChannel.localAddress(), childChannel.remoteAddress());

                            // 2.登记 requester 侧，并在 control 通道上发出 REQUIRE_CHANNEL 请求客户端开通道（本类不直接触碰 control）
                            // 3.同步等待 Promise：客户端回连并以注册包调用 registerClientProxy 后才会被唤醒，此处会阻塞当前 boss 线程
                            Future<Object> future = proxyContext.registerRequester(context.getTunnelId(), childChannel);
                            try {
                                future.get();
                                log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] TUNNEL-ESTABLISHED",
                                        context.getTunnelId(), childChannel.localAddress(), childChannel.remoteAddress());
                            } catch (Exception e) {
                                log.debug("[ProxyServer] TCP [{}] TUNNEL-ESTABLISH-FAILED", context.getTunnelId(), e);
                            }

                            // 4.无论配对成功与否都放行子连接（fireChannelRead 放在最后，保证子连接的 channelActive / 首包在配对完成之后才被处理）；
                            // 失败时仅记日志，listener 仍会收到 onTunnelEstablished
                            ctx.fireChannelRead(msg);
                        } else {
                            childChannel.unsafe().close(childChannel.voidPromise());
                            log.debug("[ProxyServer] REJECT requester {} (beforeRequesterToServerChannelAccept returned false)", childChannel.remoteAddress());
                        }
                    }

                    /** 监听绑定成功：为这条 requester 监听创建专属的 ServerTcpProxyContext 并绑定到父 channel */
                    @Override
                    public void channelActive(ChannelHandlerContext ctx) throws Exception {
                        ServerTcpProxyContext serverTcpProxyContext = createContext(ProxyContext.generateProxyId(), List.of(ctx), args.getControlContext());
                        ctx.channel().attr(ProxyContext.KEY).set(serverTcpProxyContext);

                        super.channelActive(ctx);
                    }
                })
                .option(ChannelOption.SO_BACKLOG, options.getMaxTcpConnections())
                .childOption(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        // 数据面 child pipeline：拿到父 handler 已绑定的 tunnel，单向搬给客户端回连连接
                        pipeline.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                ServerTcpTunnelContext context = (ServerTcpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] REQUESTER-DATA-RECEIVED",
                                        context.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                context.writeToClientProxyAndFlush(msg);
                            }

                            /** 子连接激活：仅记录日志。onTunnelEstablished 改在注册配对成功后触发（见 client-proxy 侧），保证回调时隧道必然 OPEN */

                            /**
                             * 级联关闭的两个触发点之一（另一个是客户端侧内网服务断开）：外部 requester 断开 →
                             * closeGracefully 会出 map 并经 ProxyContext 发 TUNNEL_CLOSE 通知客户端同步释放
                             */
                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                ServerTcpTunnelContext context = (ServerTcpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] REQUESTER-TO-SERVER-INACTIVE",
                                        context != null ? context.getTunnelId() : null,
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                listener.onTunnelClose(context);
                                if (context != null) {
                                    context.closeGracefully();
                                }
                            }

                            /** 回调 listener 后收尾：已配对只关本地，未配对直接关连接（异常不级联到对端） */
                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ServerTcpTunnelContext context = (ServerTcpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] REQUESTER-TO-SERVER-EXCEPTION",
                                        context != null ? context.getTunnelId() : null,
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress(), cause);

                                listener.caughtTunnelException(context, cause);
                                if (context != null) {
                                    context.closeLocal();
                                } else {
                                    ctx.close();
                                }
                            }
                        });
                    }
                });

        log.debug("[ProxyServer] Binding TCP requester-proxy server");
        return reqBootstrap.bind(0);
    }

    /**
     * 关闭指定 proxyId 的代理上下文：出 map、通知对端 PROXY_CLOSE、逐隧道优雅关闭并关闭其监听。
     * 注意：proxyId 不存在时 get 返回 null 会抛 NPE，未做空值保护。
     */
    public Future<?> closeProxy(String proxyId) {
        return proxyContextMap.get(proxyId).close();
    }

    /**
     * 优雅关闭：在虚拟线程中逐个关闭全部代理上下文（每个内部含通知对端、关闭隧道与 channel），成功后转 SHUTDOWN。
     * 注：当前实现未回收 client-proxy 监听本身。
     */
    @Override
    public Future<?> shutdown() {
        if (status == ServerStatus.RUNNING || status == ServerStatus.INIT) {
            status = ServerStatus.STOPPING;
            Promise<?> promise = DefaultEventLoopGroup.newPromise();

            Thread.ofVirtual().start(() -> {
                proxyContextMap.forEach((k, v) -> {
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
            proxyContextMap.forEach((k, v) -> {
                try {
                    v.close();
                } catch (Exception e) {

                }
            });
            status = ServerStatus.SHUTDOWN;
        }

    }

    /**
     * 以 proxyId 为键获取或创建代理上下文（同一个 proxyId 跨隧道复用同一个上下文）。
     * 注意：装饰只在新建时执行，已存在的上下文不会重新装饰，其持有的 handlerContextList 也保持首次创建时的内容。
     */
    public ServerTcpProxyContext createContext(String proxyId, List<ChannelHandlerContext> handlerContexts, ControlContext controlContext) {
        ServerTcpProxyContext context = proxyContextMap.computeIfAbsent(proxyId, k -> {
            ServerTcpProxyContext newContext = new ServerTcpProxyContext(proxyId, handlerContexts, controlContext);
            decorateContext(newContext);
            return newContext;
        });
        return context;
    }

    /** 挂上关闭钩子，使上下文 close 时自动从 proxyContextMap 摘除，避免残留 */
    public void decorateContext(ServerTcpProxyContext context) {
        context.setCloseProxyHook(ctx -> {
            proxyContextMap.remove(ctx.getProxyId());
        });
    }
}
