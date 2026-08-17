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
import top.fateironist.cross_relay_core.Server;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.info.OriginalRequesterInfo;
import top.fateironist.cross_relay_core.model.options.Options;
import top.fateironist.cross_relay_core.model.options.proxy_server.ClientProxyServerStartOptions;
import top.fateironist.cross_relay_core.model.options.proxy_server.RequesterProxyServerStartOptions;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ServerTunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;
import top.fateironist.cross_relay_core.proxy.listener.ProxyServerListener;

import java.net.InetSocketAddress;
import java.util.UUID;


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
    public Future<Void> start(Options option) {
        ClientProxyServerStartOptions options = (ClientProxyServerStartOptions) option;

        maxTcpConnections = options.maxTcpConnections;
        maxUdpReceiveBuffer = options.maxUdpReceiveBuffer;
        maxUdpSendBuffer = options.maxUdpSendBuffer;

        if (options.enableProxyTcp && options.enableProxyUdp) {
            EventLoop eventLoop = workerGroup.next();
            Promise<Void> promise = eventLoop.newPromise();
            Thread.ofVirtual().start(() -> {
                Future<Void> tcpFuture = startTcpClientProxyServer(options);
                Future<Void> udpFuture = startUdpClientProxyServer(options);

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
        } else if (options.enableProxyTcp) {
            return startTcpClientProxyServer(options);
        } else if (options.enableProxyUdp) {
            return startUdpClientProxyServer(options);
        }else {
            return null;
        }
    }

    public Future<Void> startTcpClientProxyServer(ClientProxyServerStartOptions options) {
        tcpClientProxyBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) {
                        Channel childChannel = (Channel) msg;

                        if (listener.beforeClientToServerChannelAccept(TransportLayerProtocol.TCP, childChannel)) {
                            ctx.fireChannelRead(msg);
                        } else {
                            childChannel.unsafe().close(childChannel.voidPromise());
                        }
                    }
                })
                .option(ChannelOption.SO_BACKLOG, options.maxTcpConnections)
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

                                    context = (ServerTunnelContext) listener.onClientToServerChannelRegister(id);
                                    context.setServerToClientChannel(ctx.channel());
                                    ctx.channel().attr(TunnelContext.KEY).set(context);
                                    return;
                                }

                                context.writeToRequesterAndFlush(msg);
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ServerTunnelContext context = (ServerTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();

                                if (context != null) {
                                    context.closeLocal();
                                } else {
                                    ctx.close();
                                }
                            }
                        });
                    }
                });

        return tcpClientProxyBootstrap.bind(options.clientProxyPort);
    }

    public ChannelFuture startTcpRequesterProxyServer(RequesterProxyServerStartOptions options) {
        ServerBootstrap reqBootstrap = new ServerBootstrap();
        reqBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                        Channel childChannel = (Channel) msg;

                        if (listener.beforeRequesterToServerChannelAccept(TransportLayerProtocol.TCP, childChannel)) {

                            OriginalRequesterInfo originalRequesterInfo = new OriginalRequesterInfo((InetSocketAddress) childChannel.remoteAddress(),
                                    TransportLayerProtocol.TCP);

                            ServerTunnelContext context = new ServerTunnelContext(UUID.randomUUID().toString(),
                                    TransportLayerProtocol.TCP,
                                    options.clientServiceInfo,
                                    options.proxyClientInfo,
                                    options.proxyServerInfo,
                                    originalRequesterInfo
                                    );

                            context.setServerToRequesterChannel(childChannel);

                            ctx.channel().attr(TunnelContext.KEY).set(context);

                            Future<Channel> future = listener.onRequesterRequireTunnel(context);
                            Channel serverToClientChannel = future.get();

                            context.setServerToClientChannel(serverToClientChannel);

                            ctx.fireChannelRead(msg);
                        } else {
                            childChannel.unsafe().close(childChannel.voidPromise());
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
                        pipeline.addLast(new LoggingHandler(LogLevel.INFO));
                        pipeline.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                ServerTunnelContext context = (ServerTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();

                                context.writeToClientAndFlush(msg);
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                ServerTunnelContext context = (ServerTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();

                                listener.onTunnelClose(context);
                                context.closeGracefully(listener::closeRemoteTunnel);
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ServerTunnelContext context = (ServerTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();

                                if (context != null) {
                                    context.closeLocal();
                                } else {
                                    ctx.close();
                                }
                            }
                        });
                    }
                });

        return reqBootstrap.bind();
    }

    public Future<Void> startUdpClientProxyServer(ClientProxyServerStartOptions options) {
        return null;
    }

    public ChannelFuture startUdpRequesterProxyServer(RequesterProxyServerStartOptions options) {
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
