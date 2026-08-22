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
import lombok.extern.slf4j.Slf4j;
import top.fateironist.cross_relay_core.Server;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.proxy_server.ClientProxyServerStartArgs;
import top.fateironist.cross_relay_core.model.args.proxy_server.RequesterProxyServerStartArgs;
import top.fateironist.cross_relay_core.model.info.OriginalRequesterInfo;
import top.fateironist.cross_relay_core.model.proxy.tunnel.server.ServerTcpTunnelContext;
import top.fateironist.cross_relay_core.proxy.listener.ProxyServerListener;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ProxyUdpServer implements Server {
    private final EventLoopGroup workerGroup;
    private final ProxyServerListener listener;

    private int maxUdpReceiveBuffer;
    private int maxUdpSendBuffer;

    private UdpTunnelRouter udpTunnelRouter;

    public ProxyUdpServer(EventLoopGroup workerGroup, ProxyServerListener listener) {
        this.workerGroup = workerGroup;
        this.listener = listener;
        this.udpTunnelRouter = new UdpTunnelRouter();
    }

    @Override
    public Future<Void> start(AbstractArgs arg) {
        ClientProxyServerStartArgs args = (ClientProxyServerStartArgs) arg;
        var opts = args.getOptions();

        return startUdpClientProxyServer(args);
    }


    public ChannelFuture startUdpClientProxyServer(ClientProxyServerStartArgs args) {
        udpTunnelRouter = new UdpTunnelRouter();

        maxUdpReceiveBuffer = args.getOptions().getMaxUdpReceiveBuffer();
        maxUdpSendBuffer = args.getOptions().getMaxUdpSendBuffer();

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
                        pipeline.addLast(new SimpleChannelInboundHandler<DatagramPacket>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {
                                ServerUdpTunnelContext context = (ServerUdpTunnelContext) udpTunnelRouter.getTunnelContext(msg.sender());

                                if (context == null) {
                                    ByteBuf content = msg.content();
                                    String id = content.readString(content.readableBytes(), CharsetUtil.UTF_8);

                                    context = (ServerUdpTunnelContext) udpTunnelRouter.registerClientProxy(id, msg.sender());
                                    context.setServerToClientChannel(ctx.channel(), msg.sender());

                                    log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] CLIENT-TO-SERVER-REGISTER",
                                            context.getTunnelId(), ctx.channel().localAddress(), msg.sender());
                                    return;
                                }

                                log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] CLIENT-DATA-RECEIVED",
                                        context.getTunnelId(), ctx.channel().localAddress(), msg.sender());
                                context.writeToRequesterAndFlush(msg.content());
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] CLIENT-TO-SERVER-ACTIVE",
                                        null, ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                // fixme: 这里Udp的channel是多个连接复用的
                                log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] CLIENT-TO-SERVER-INACTIVE",
                                        null,
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

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

    public ChannelFuture startUdpRequesterProxyServer(RequesterProxyServerStartArgs args) {
        var options = args.getOptions();
        Bootstrap reqBootstrap = new Bootstrap();
        reqBootstrap.group(workerGroup)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_RCVBUF, maxUdpReceiveBuffer)
                .option(ChannelOption.SO_SNDBUF, maxUdpSendBuffer)
                .handler(new ChannelInitializer<DatagramChannel>() {
                    @Override
                    protected void initChannel(DatagramChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new IdleStateHandler(0, 0, options.getRequesterTimeout(), TimeUnit.MILLISECONDS));
                        pipeline.addLast(new SimpleChannelInboundHandler<DatagramPacket>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {
                                if (listener.beforeRequesterToServerConnectionAccept(TransportLayerProtocol.UDP, ctx.channel(), msg.sender())) {
                                    log.debug("[ProxyServer] ACCEPT client channel {} -> {}", msg.sender(), ctx.channel().localAddress());
                                } else {
                                    ctx.close();
                                    log.debug("[ProxyServer] REJECT client channel {} (beforeRequesterToServerConnectionAccept returned false)", msg.sender());
                                    return;
                                }


                                ServerUdpTunnelContext context = (ServerUdpTunnelContext) udpTunnelRouter.getTunnelContext(msg.sender());

                                if (context == null) {
                                    if (listener.beforeRequesterToServerConnectionAccept(TransportLayerProtocol.UDP, ctx.channel(), msg.sender())) {
                                        log.debug("[ProxyServer] ACCEPT requester {} -> {}", msg.sender(), ctx.channel().localAddress());

                                        OriginalRequesterInfo originalRequesterInfo = new OriginalRequesterInfo(msg.sender(),
                                                TransportLayerProtocol.UDP);

                                        ServerUdpTunnelContext newContext = new ServerUdpTunnelContext(UUID.randomUUID().toString(),
                                                TransportLayerProtocol.UDP,
                                                options.getClientServiceInfo(),
                                                options.getProxyClientInfo(),
                                                options.getProxyServerInfo(),
                                                originalRequesterInfo
                                        );

                                        newContext.setServerToRequesterChannel(ctx.channel(), msg.sender());

                                        log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] REQUESTER-ACTIVE",
                                                newContext.getTunnelId(), ctx.channel().localAddress(), msg.sender());

                                        Future<ServerTcpTunnelContext> future = udpTunnelRouter.registerRequester(newContext.getTunnelId(), newContext, workerGroup.next(), listener::onRequesterRequireTunnel);
                                        try {
                                            future.get();
                                            log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] TUNNEL-ESTABLISHED",
                                                    newContext.getTunnelId(), ctx.channel().localAddress(), msg.sender());
                                        } catch (Exception e) {
                                            log.debug("[ProxyServer] UDP [{}] TUNNEL-ESTABLISH-FAILED", newContext.getTunnelId(), e);
                                        }
                                    } else {
                                        log.debug("[ProxyServer] REJECT requester {} (beforeRequesterToServerChannelAccept returned false)", msg.sender());
                                    }
                                    return;
                                }

                                log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] REQUESTER-DATA-RECEIVED",
                                        context.getTunnelId(), ctx.channel().localAddress(), msg.sender());

                                context.writeToClientAndFlush(msg.content());
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                // fixme: 这里Udp的channel是多个连接复用的
                                log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] REQUESTER-TO-SERVER-INACTIVE",
                                        null,
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress());

                            }

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

    @Override
    public java.util.concurrent.Future<?> shutdown() {
        return workerGroup.shutdownGracefully();
    }

    @Override
    public void shutdownNow() {
        workerGroup.shutdownNow();
    }
}
