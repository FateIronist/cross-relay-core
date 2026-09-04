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

@Slf4j
public class ProxyTcpServer implements Server {
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ProxyServerListener listener;

    private final Map<String, ServerTcpProxyContext> proxyContextMap = new ConcurrentHashMap<>();

    public volatile ServerStatus status = ServerStatus.INIT;

    public ProxyTcpServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup, ProxyServerListener listener) {
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.listener = listener;
    }

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


    public ChannelFuture startTcpClientProxyServer(ClientProxyServerStartArgs args) {
        var options = args.getOptions();
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
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
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
        return tcpClientProxyBootstrap.bind(options.getClientProxyPort());
    }

    public ChannelFuture startTcpRequesterProxyServer(RequesterProxyServerStartArgs args) {
        if (status != ServerStatus.RUNNING) {
            return DefaultEventLoopGroup.failFuture(new Exception("Server is not in RUNNING state"));
        }

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
                            try {
                                future.get();
                                log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] TUNNEL-ESTABLISHED",
                                        context.getTunnelId(), childChannel.localAddress(), childChannel.remoteAddress());
                            } catch (Exception e) {
                                log.debug("[ProxyServer] TCP [{}] TUNNEL-ESTABLISH-FAILED", context.getTunnelId(), e);
                            }

                            ctx.fireChannelRead(msg);
                        } else {
                            childChannel.unsafe().close(childChannel.voidPromise());
                            log.debug("[ProxyServer] REJECT requester {} (beforeRequesterToServerChannelAccept returned false)", childChannel.remoteAddress());
                        }
                    }

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

    public Future<?> closeProxy(String proxyId) {
        return proxyContextMap.get(proxyId).close();
    }

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

    public ServerTcpProxyContext createContext(String proxyId, List<ChannelHandlerContext> handlerContexts, ControlContext controlContext) {
        ServerTcpProxyContext context = proxyContextMap.computeIfAbsent(proxyId, k -> {
            ServerTcpProxyContext newContext = new ServerTcpProxyContext(proxyId, handlerContexts, controlContext);
            decorateContext(newContext);
            return newContext;
        });
        return context;
    }

    public void decorateContext(ServerTcpProxyContext context) {
        context.setCloseProxyHook(ctx -> {
            proxyContextMap.remove(ctx.getProxyId());
        });
    }
}
