package top.fateironist.cross_relay_core.proxy;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Future;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import top.fateironist.constack.Container;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.FutureBridge;
import top.fateironist.cross_relay_core.Server;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.proxy_server.ClientProxyServerStartArgs;
import top.fateironist.cross_relay_core.model.args.proxy_server.RequesterProxyServerStartArgs;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.control.event.CommonInfo;
import top.fateironist.cross_relay_core.model.proxy.ProxyContext;
import top.fateironist.cross_relay_core.model.proxy.ServerTcpProxyContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ServerTcpTunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;
import top.fateironist.cross_relay_core.proxy.listener.ProxyServerListener;
import top.fateironist.cross_relay_core.util.JsonUtil;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TCP 代理服务端：顶层容器，load = 绑定共享的 client-proxy 监听端口即激活；
 * 每个 requester 监听端口对应一个 ServerTcpProxyContext 子容器，随控制连接级联销毁。
 * Listener 为纯通知钩子，不参与状态机与资源回收。
 */
@Slf4j
public class ProxyTcpServer extends Container implements Server {
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ProxyServerListener listener;

    // 路由索引：proxyId -> 代理容器；索引项随容器销毁经 Effect 移除
    private final Map<String, ServerTcpProxyContext> proxyContextMap = new ConcurrentHashMap<>();

    /** 共享的 client-proxy 监听 channel（本容器绑定的 Netty Channel） */
    @Getter
    private volatile Channel clientProxyChannel;

    public ProxyTcpServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup, ProxyServerListener listener) {
        super();
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.listener = listener;

        // Effect：关闭共享 client-proxy 监听 channel（本容器绑定持有，外部注入的 EventLoopGroup 不清理）
        effect(c -> {
            Channel channel = clientProxyChannel;
            if (channel != null) {
                channel.close();
            }
        });
    }

    @Override
    public java.util.concurrent.Future<Void> start(AbstractArgs arg) {
        return FutureBridge.bridge(load(arg), null);
    }

    @Override
    protected java.util.concurrent.Future<Object> start(Object... args) {
        ClientProxyServerStartArgs serverArgs = (ClientProxyServerStartArgs) args[0];
        var options = serverArgs.getOptions();

        ServerBootstrap tcpClientProxyBootstrap = new ServerBootstrap();
        tcpClientProxyBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) {
                        ServerTcpProxyContext serverTcpProxyContext = (ServerTcpProxyContext) ctx.channel().attr(ProxyContext.KEY).get();
                        Channel childChannel = (Channel) msg;

                        if (listener.beforeClientToServerConnectionAccept(TransportLayerProtocol.TCP, ctx.channel(), childChannel)) {
                            log.debug("[ProxyServer] ACCEPT client channel {} -> {}", childChannel.remoteAddress(), childChannel.localAddress());

                            childChannel.attr(ProxyContext.KEY).set(serverTcpProxyContext);

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
                        pipeline.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
                                ServerTcpProxyContext proxyContext = (ServerTcpProxyContext) ctx.channel().attr(ProxyContext.KEY).get();
                                ServerTcpTunnelContext tunnelContext = (ServerTcpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();

                                if (tunnelContext == null) {

                                    String json = msg.readString(msg.readableBytes(), CharsetUtil.UTF_8);
                                    CommonInfo registerDTO = JsonUtil.OBJECT_MAPPER.readValue(json, CommonInfo.class);

                                    String tunnelId = registerDTO.getTunnelId();
                                    String proxyId = registerDTO.getProxyId();

                                    proxyContext = proxyContextMap.get(proxyId);
                                    ctx.channel().attr(ProxyContext.KEY).set(proxyContext);

                                    tunnelContext = proxyContext.registerClientProxy(tunnelId, ctx.channel());

                                    if (tunnelContext == null) {
                                        log.debug("[ProxyServer] TCP [L:{}] CLIENT-TO-SERVER-REGISTER-FAILED: unknown tunnel {}", ctx.channel().localAddress(), json);
                                        ctx.close();
                                        return;
                                    }

                                    ctx.channel().attr(TunnelContext.KEY).set(tunnelContext);

                                    log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] CLIENT-TO-SERVER-REGISTER",
                                            tunnelContext.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                    return;
                                }

                                log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] CLIENT-DATA-RECEIVED",
                                        tunnelContext.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                tunnelContext.writeToRequesterAndFlush(msg);
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] CLIENT-TO-SERVER-ACTIVE",
                                        null, ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                ServerTcpTunnelContext context = (ServerTcpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] CLIENT-TO-SERVER-INACTIVE",
                                        context != null ? context.getTunnelId() : null,
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

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
        top.fateironist.constack.Promise<Object> promise = new top.fateironist.constack.Promise<>();
        ChannelFuture bindFuture = tcpClientProxyBootstrap.bind(options.getClientProxyPort());
        bindFuture.addListener(f -> {
            if (f.isSuccess()) {
                clientProxyChannel = bindFuture.channel();
                promise.setSuccess(null);
            } else {
                promise.setFailure(f.cause());
            }
        });
        return promise;
    }

    /**
     * 启动一个 requester 监听端口并创建对应代理子容器；requester 注册经超时看护，不阻塞事件循环。
     */
    public ChannelFuture startTcpRequesterProxyServer(RequesterProxyServerStartArgs args) {
        var options = args.getOptions();
        ServerBootstrap reqBootstrap = new ServerBootstrap();
        reqBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) {
                        ServerTcpProxyContext proxyContext = (ServerTcpProxyContext) ctx.channel().attr(ProxyContext.KEY).get();
                        Channel childChannel = (Channel) msg;

                        if (listener.beforeRequesterToServerConnectionAccept(TransportLayerProtocol.TCP, ctx.channel(), childChannel)) {
                            log.debug("[ProxyServer] ACCEPT requester {} -> {}", childChannel.remoteAddress(), childChannel.localAddress());

                            ServerTcpTunnelContext context = (ServerTcpTunnelContext) proxyContext.newTunnelContext();
                            childChannel.attr(TunnelContext.KEY).set(context);

                            log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] REQUESTER-ACTIVE",
                                    context.getTunnelId(), childChannel.localAddress(), childChannel.remoteAddress());

                            Future<Object> future = proxyContext.registerRequester(context.getTunnelId(), childChannel);
                            future.addListener(f -> {
                                if (f.isSuccess()) {
                                    log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] TUNNEL-ESTABLISHED",
                                            context.getTunnelId(), childChannel.localAddress(), childChannel.remoteAddress());
                                } else {
                                    log.debug("[ProxyServer] TCP [{}] TUNNEL-ESTABLISH-FAILED", context.getTunnelId(), f.cause());
                                }
                            });

                            ctx.fireChannelRead(msg);
                        } else {
                            childChannel.unsafe().close(childChannel.voidPromise());
                            log.debug("[ProxyServer] REJECT requester {} (beforeRequesterToServerChannelAccept returned false)", childChannel.remoteAddress());
                        }
                    }

                    @Override
                    public void channelActive(ChannelHandlerContext ctx) throws Exception {
                        // 创建代理子容器并启动其生命周期（请求监听 channel 由本方法所属 Server 持有并注册回收）
                        ServerTcpProxyContext serverTcpProxyContext = createContext(ProxyContext.generateProxyId(), List.of(ctx), args.getControlContext());
                        ctx.channel().attr(ProxyContext.KEY).set(serverTcpProxyContext);
                        serverTcpProxyContext.load(args);

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
                        pipeline.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                ServerTcpTunnelContext context = (ServerTcpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] REQUESTER-DATA-RECEIVED",
                                        context.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                context.writeToClientProxyAndFlush(msg);
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                ServerTcpTunnelContext context = (ServerTcpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                listener.onTunnelEstablished(context);
                            }

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

    public java.util.concurrent.Future<?> closeProxy(String proxyId) {
        ServerTcpProxyContext context = proxyContextMap.get(proxyId);
        if (context == null) {
            return DefaultEventLoopGroup.failFuture(new IllegalStateException("unknown proxyId: " + proxyId));
        }
        return context.close();
    }

    @Override
    public java.util.concurrent.Future<?> shutdown() {
        return disposal();
    }

    @Override
    public void shutdownNow() {
        try {
            disposal().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 创建代理子容器并登记路由索引；索引项随容器销毁经 Effect 移除，避免悬挂引用。
     */
    public ServerTcpProxyContext createContext(String proxyId, List<ChannelHandlerContext> handlerContexts, ControlContext controlContext) {
        try {
            ServerTcpProxyContext context = (ServerTcpProxyContext) child(proxyId, handlerContexts, controlContext).get();
            proxyContextMap.put(proxyId, context);
            context.effect(c -> proxyContextMap.remove(proxyId));
            return context;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Container createChild(Container parent, Object... args) {
        String proxyId = (String) args[0];
        @SuppressWarnings("unchecked")
        List<ChannelHandlerContext> handlerContexts = (List<ChannelHandlerContext>) args[1];
        ControlContext controlContext = (ControlContext) args[2];
        return new ServerTcpProxyContext(parent, proxyId, handlerContexts, controlContext);
    }
}
