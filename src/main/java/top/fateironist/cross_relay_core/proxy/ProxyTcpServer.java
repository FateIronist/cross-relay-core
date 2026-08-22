package top.fateironist.cross_relay_core.proxy;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import lombok.extern.slf4j.Slf4j;
import top.fateironist.cross_relay_core.Server;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.proxy_server.ClientProxyServerStartArgs;
import top.fateironist.cross_relay_core.model.args.proxy_server.RequesterProxyServerStartArgs;
import top.fateironist.cross_relay_core.model.info.OriginalRequesterInfo;
import top.fateironist.cross_relay_core.model.proxy.tunnel.server.ServerTcpTunnelContext;
import top.fateironist.cross_relay_core.proxy.listener.ProxyServerListener;

import java.net.InetSocketAddress;
import java.util.UUID;

@Slf4j
public class ProxyTcpServer implements Server {
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ProxyServerListener listener;

    private int maxTcpConnections;

    private TcpTunnelRouter tcpTunnelRouter;

    public ProxyTcpServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup, ProxyServerListener listener) {
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.listener = listener;
    }

    @Override
    public Future<Void> start(AbstractArgs arg) {
        ClientProxyServerStartArgs args = (ClientProxyServerStartArgs) arg;
        var opts = args.getOptions();
        return startTcpClientProxyServer(args);
    }


    public ChannelFuture startTcpClientProxyServer(ClientProxyServerStartArgs args) {
        tcpTunnelRouter = new TcpTunnelRouter();

        var options = args.getOptions();
        ServerBootstrap tcpClientProxyBootstrap = new ServerBootstrap();
        tcpClientProxyBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) {
                        Channel childChannel = (Channel) msg;

                        if (listener.beforeClientToServerConnectionAccept(TransportLayerProtocol.TCP, ctx.channel(),childChannel)) {
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
                        pipeline.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                ServerTcpTunnelContext context = (ServerTcpTunnelContext) tcpTunnelRouter.getTunnelContext(ctx);

                                if (context == null) {
                                    String id = msg.readString(msg.readableBytes(), CharsetUtil.UTF_8);

                                    context = (ServerTcpTunnelContext) tcpTunnelRouter.registerClientProxy(id);
                                    context.setServerToClientChannel(ctx.channel(), (InetSocketAddress) ctx.channel().remoteAddress());
                                    tcpTunnelRouter.setTunnelContext(id, context);
                                    log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] CLIENT-TO-SERVER-REGISTER",
                                            context.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                    return;
                                }

                                log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] CLIENT-DATA-RECEIVED",
                                        context.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                context.writeToRequesterAndFlush(msg);
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] CLIENT-TO-SERVER-ACTIVE",
                                        null, ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                ServerTcpTunnelContext context = (ServerTcpTunnelContext) tcpTunnelRouter.getTunnelContext(ctx);
                                log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] CLIENT-TO-SERVER-INACTIVE",
                                        context != null ? context.getTunnelId() : null,
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ServerTcpTunnelContext context = (ServerTcpTunnelContext) tcpTunnelRouter.getTunnelContext(ctx);
                                log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] CLIENT-TO-SERVER-EXCEPTION",
                                        context != null ? context.getTunnelId() : null,
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress(), cause);

                                listener.caughtTunnelException(context, cause);
                                if (context != null) {
                                    context.closeLocal(workerGroup.next());
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
        var options = args.getOptions();
        ServerBootstrap reqBootstrap = new ServerBootstrap();
        reqBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) {
                        Channel childChannel = (Channel) msg;

                        if (listener.beforeRequesterToServerConnectionAccept(TransportLayerProtocol.TCP, ctx.channel(),childChannel)) {
                            log.debug("[ProxyServer] ACCEPT requester {} -> {}", childChannel.remoteAddress(), childChannel.localAddress());

                            OriginalRequesterInfo originalRequesterInfo = new OriginalRequesterInfo((InetSocketAddress) childChannel.remoteAddress(),
                                    TransportLayerProtocol.TCP);

                            ServerTcpTunnelContext context = new ServerTcpTunnelContext(UUID.randomUUID().toString(),
                                    TransportLayerProtocol.TCP,
                                    options.getClientServiceInfo(),
                                    options.getProxyClientInfo(),
                                    options.getProxyServerInfo(),
                                    originalRequesterInfo
                            );

                            context.setServerToRequesterChannel(childChannel, (InetSocketAddress) childChannel.remoteAddress());

                            tcpTunnelRouter.setTunnelContext(ctx, context);
                            log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] REQUESTER-ACTIVE",
                                    context.getTunnelId(), childChannel.localAddress(), childChannel.remoteAddress());

                            Future<ServerTcpTunnelContext> future = tcpTunnelRouter.registerRequester(context.getTunnelId(), context, workerGroup.next(), listener::onRequesterRequireTunnel);
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
                })
                .option(ChannelOption.SO_BACKLOG, maxTcpConnections)
                .childOption(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                ServerTcpTunnelContext context = (ServerTcpTunnelContext) tcpTunnelRouter.getTunnelContext(ctx);
                                log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] REQUESTER-DATA-RECEIVED",
                                        context.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                context.writeToClientAndFlush(msg);
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                ServerTcpTunnelContext context = (ServerTcpTunnelContext) tcpTunnelRouter.getTunnelContext(ctx);
                                listener.onTunnelEstablished(context);
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                ServerTcpTunnelContext context = (ServerTcpTunnelContext) tcpTunnelRouter.getTunnelContext(ctx);
                                log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] REQUESTER-TO-SERVER-INACTIVE",
                                        context != null ? context.getTunnelId() : null,
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                listener.onTunnelClose(context);
                                context.closeGracefully(listener::closeRemoteTunnel, workerGroup.next());
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ServerTcpTunnelContext context = (ServerTcpTunnelContext) tcpTunnelRouter.getTunnelContext(ctx);
                                log.debug("[ProxyServer] TCP [{}，L:{}，R:{}] REQUESTER-TO-SERVER-EXCEPTION",
                                        context != null ? context.getTunnelId() : null,
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress(), cause);

                                listener.caughtTunnelException(context, cause);
                                if (context != null) {
                                    context.closeLocal(workerGroup.next());
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

    @Override
    public java.util.concurrent.Future<?> shutdown() {
        EventLoopGroup shutdownEventLoopGroup = new NioEventLoopGroup(1);
        EventLoop eventLoop = shutdownEventLoopGroup.next();
        Promise<?> promise = eventLoop.newPromise();
        eventLoop.execute(() -> {
            try {
                bossGroup.shutdownGracefully().sync();
                workerGroup.shutdownGracefully().sync();
            } catch (InterruptedException e) {
                promise.setFailure(e);
            }

            promise.setSuccess(null);
        });

        return promise;
    }

    @Override
    public void shutdownNow() {
        bossGroup.shutdownNow();
        workerGroup.shutdownNow();
    }
}
