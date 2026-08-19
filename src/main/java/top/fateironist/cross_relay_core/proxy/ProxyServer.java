package top.fateironist.cross_relay_core.proxy;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import lombok.extern.slf4j.Slf4j;
import top.fateironist.cross_relay_core.Server;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.info.OriginalRequesterInfo;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.proxy_server.ClientProxyServerStartArgs;
import top.fateironist.cross_relay_core.model.args.proxy_server.RequesterProxyServerStartArgs;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ServerTunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;
import top.fateironist.cross_relay_core.proxy.listener.ProxyServerListener;

import java.net.InetSocketAddress;
import java.util.UUID;


@Slf4j
public class ProxyServer implements Server {
    private final ServerBootstrap tcpClientProxyBootstrap;
    private final ServerBootstrap udpClientProxyBootstrap;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ProxyServerListener listener;


    private int maxTcpConnections;
    private int maxUdpReceiveBuffer;
    private int maxUdpSendBuffer;

    public ProxyServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup, ProxyServerListener listener) {
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.listener = listener;
        this.tcpClientProxyBootstrap = new ServerBootstrap();
        this.udpClientProxyBootstrap = new ServerBootstrap();
    }

    @Override
    public Future<Void> start(AbstractArgs arg) {
        ClientProxyServerStartArgs args = (ClientProxyServerStartArgs) arg;
        var opts = args.getOptions();

        maxTcpConnections = opts.getMaxTcpConnections();
        maxUdpReceiveBuffer = opts.getMaxUdpReceiveBuffer();
        maxUdpSendBuffer = opts.getMaxUdpSendBuffer();

        if (opts.isEnableProxyTcp() && opts.isEnableProxyUdp()) {
            EventLoop eventLoop = workerGroup.next();
            Promise<Void> promise = eventLoop.newPromise();
            Thread.ofVirtual().start(() -> {
                Future<Void> tcpFuture = startTcpClientProxyServer(args);
                Future<Void> udpFuture = startUdpClientProxyServer(args);

                try {
                    tcpFuture.sync();
                    udpFuture.sync();
                } catch (Exception e) {
                    promise.setFailure(e);
                    return;
                }

                promise.setSuccess(null);
            });
            return promise;
        } else if (opts.isEnableProxyTcp()) {
            return startTcpClientProxyServer(args);
        } else if (opts.isEnableProxyUdp()) {
            return startUdpClientProxyServer(args);
        }else {
            return null;
        }
    }

    public ChannelFuture startTcpClientProxyServer(ClientProxyServerStartArgs args) {
        var options = args.getOptions();
        tcpClientProxyBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) {
                        Channel childChannel = (Channel) msg;

                        if (listener.beforeClientToServerChannelAccept(TransportLayerProtocol.TCP, childChannel)) {
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
                                ServerTunnelContext context = (ServerTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();

                                if (context == null) {
                                    String id = msg.readString(msg.readableBytes(), CharsetUtil.UTF_8);

                                    context = (ServerTunnelContext) listener.onClientToServerChannelRegister(id, ctx.channel());
                                    context.setServerToClientChannel(ctx.channel());
                                    ctx.channel().attr(TunnelContext.KEY).set(context);
                                    log.debug("[ProxyServer] [{}，L:{}，R:{}] CLIENT-TO-SERVER-REGISTER",
                                            context.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                    return;
                                }

                                log.debug("[ProxyServer] [{}，L:{}，R:{}] CLIENT-DATA-RECEIVED",
                                        context.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                context.writeToRequesterAndFlush(msg);
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                log.debug("[ProxyServer] [{}，L:{}，R:{}] CLIENT-TO-SERVER-ACTIVE",
                                        null, ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                ServerTunnelContext context = (ServerTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyServer] [{}，L:{}，R:{}] CLIENT-TO-SERVER-INACTIVE",
                                        context != null ? context.getTunnelId() : null,
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ServerTunnelContext context = (ServerTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyServer] [{}，L:{}，R:{}] CLIENT-TO-SERVER-EXCEPTION",
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
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                        Channel childChannel = (Channel) msg;

                        if (listener.beforeRequesterToServerChannelAccept(TransportLayerProtocol.TCP, childChannel)) {
                            log.debug("[ProxyServer] ACCEPT requester {} -> {}", childChannel.remoteAddress(), childChannel.localAddress());

                            OriginalRequesterInfo originalRequesterInfo = new OriginalRequesterInfo((InetSocketAddress) childChannel.remoteAddress(),
                                    TransportLayerProtocol.TCP);

                            ServerTunnelContext context = new ServerTunnelContext(UUID.randomUUID().toString(),
                                    TransportLayerProtocol.TCP,
                                    options.getClientServiceInfo(),
                                    options.getProxyClientInfo(),
                                    options.getProxyServerInfo(),
                                    originalRequesterInfo
                                    );

                            context.setServerToRequesterChannel(childChannel);

                            childChannel.attr(TunnelContext.KEY).set(context);
                            log.debug("[ProxyServer] [{}，L:{}，R:{}] REQUESTER-ACTIVE",
                                    context.getTunnelId(), childChannel.localAddress(), childChannel.remoteAddress());

                            Future<Channel> future = listener.onRequesterRequireTunnel(context, workerGroup.next());
                            Channel serverToClientChannel = future.get();

                            context.setServerToClientChannel(serverToClientChannel);
                            log.debug("[ProxyServer] [{}，L:{}，R:{}] TUNNEL-ESTABLISHED",
                                    context.getTunnelId(), childChannel.localAddress(), childChannel.remoteAddress());

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
                                ServerTunnelContext context = (ServerTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyServer] [{}，L:{}，R:{}] REQUESTER-DATA-RECEIVED",
                                        context.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                context.writeToClientAndFlush(msg);
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                ServerTunnelContext context = (ServerTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                listener.onTunnelEstablished(context);
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                ServerTunnelContext context = (ServerTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyServer] [{}，L:{}，R:{}] REQUESTER-TO-SERVER-INACTIVE",
                                        context != null ? context.getTunnelId() : null,
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                listener.onTunnelClose(context);
                                context.closeGracefully(listener::closeRemoteTunnel, workerGroup.next());
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ServerTunnelContext context = (ServerTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyServer] [{}，L:{}，R:{}] REQUESTER-TO-SERVER-EXCEPTION",
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

    public ChannelFuture startUdpClientProxyServer(ClientProxyServerStartArgs args) {
        return null;
    }

    public ChannelFuture startUdpRequesterProxyServer(RequesterProxyServerStartArgs args) {
        return null;
    }

    @Override
    public Future<?> shutdown() {
        return null;
    }

    @Override
    public void shutdownNow() {

    }
}
