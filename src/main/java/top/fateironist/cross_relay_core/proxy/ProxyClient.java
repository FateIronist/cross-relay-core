package top.fateironist.cross_relay_core.proxy;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.Promise;
import top.fateironist.cross_relay_core.Client;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.info.ClientServiceInfo;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.proxy_client.ProxyClientConnectArgs;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ClientTunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;
import top.fateironist.cross_relay_core.proxy.listener.ProxyClientListener;

import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import io.netty.util.concurrent.Future;

@Slf4j
public class ProxyClient implements Client {
    private final ClientServiceInfo clientServiceInfo;
    private final ProxyServerInfo proxyServerInfo;
    private final EventLoopGroup workerGroup;

    private final ProxyClientListener listener;

    public ProxyClient(EventLoopGroup workerGroup, ClientServiceInfo clientServiceInfo, ProxyServerInfo proxyServerInfo, ProxyClientListener proxyClientListener) {
        this.workerGroup = workerGroup;
        this.clientServiceInfo = clientServiceInfo;
        this.proxyServerInfo = proxyServerInfo;
        this.listener = proxyClientListener;
    }

    @Override
    public Future<Void> connect(AbstractArgs arg) {
        ProxyClientConnectArgs args = (ProxyClientConnectArgs) arg;
        var opts = args.getOptions();
        Promise<Void> promise = workerGroup.next().newPromise();

        if (proxyServerInfo.getAddress() == null || proxyServerInfo.getAddress().getServerProxyRequestAddress()== null) {

            if (proxyServerInfo.getAddress().getServerInfoServerAddress() != null) {
                proxyServerInfo.getMetaDataFromServerInfoServer(3, 1000);
            }

            if (!proxyServerInfo.getAddress().isSufficient()) {
                promise.setFailure(new Exception("Proxy server request address is null"));
                return promise;
            }

        }

        ClientTunnelContext tunnelContext= new ClientTunnelContext(args.getTunnelId(),
                args.getProtocol(),
                clientServiceInfo,
                opts.getProxyClientInfo(),
                proxyServerInfo,
                opts.getOriginalRequesterInfo()
                );

        Bootstrap serviceProxyBootstrap = new Bootstrap();
        Bootstrap serverConnecterBootstrap = new Bootstrap();

        if (args.getProtocol() == TransportLayerProtocol.TCP) {
            serviceProxyBootstrap.group(workerGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, false)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                    ClientTunnelContext context = (ClientTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                    log.debug("[ProxyClient] [{}，L:{}，R:{}] SERVICE-DATA-RECEIVED",
                                            context.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                    context.writeToServerAndFlush(msg);
                                }

                                @Override
                                public void channelActive(ChannelHandlerContext ctx) {
                                    tunnelContext.setClientToServiceChannel(ctx.channel());
                                    ctx.channel().attr(TunnelContext.KEY).set(tunnelContext);
                                    log.debug("[ProxyClient] [{}，L:{}，R:{}] SERVICE-PROXY-ACTIVE",
                                            tunnelContext.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                }

                                @Override
                                public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                    ClientTunnelContext context = (ClientTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                    log.debug("[ProxyClient] [{}，L:{}，R:{}] SERVICE-PROXY-INACTIVE",
                                            context != null ? context.getTunnelId() : tunnelContext.getTunnelId(),
                                            ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                    listener.onTunnelClose(context);
                                    if (context != null) context.closeGracefully(listener::closeRemoteTunnel, workerGroup.next());
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                    ClientTunnelContext context = (ClientTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                    log.debug("[ProxyClient] [{}，L:{}，R:{}] SERVICE-PROXY-EXCEPTION",
                                            context != null ? context.getTunnelId() : tunnelContext.getTunnelId(),
                                            ctx.channel().localAddress(), ctx.channel().remoteAddress(), cause);

                                    listener.caughtTunnelException(context, cause);
                                    if (context != null) context.closeLocal(workerGroup.next());
                                    else ctx.close();
                                }

                            });
                        }
                    });

            serverConnecterBootstrap.group(workerGroup).channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, false)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                    ClientTunnelContext context = (ClientTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                    log.debug("[ProxyClient] [{}，L:{}，R:{}] SERVER-DATA-RECEIVED",
                                            context.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                    context.writeToServiceAndFlush(msg);
                                }

                                @Override
                                public void channelActive(ChannelHandlerContext ctx) {
                                    tunnelContext.setClientToServerChannel(ctx.channel());

                                    // 1. 发送初始认证信息
                                    ByteBuf byteBuf = ctx.alloc().buffer();
                                    byteBuf.writeBytes(args.getTunnelId().getBytes(StandardCharsets.UTF_8));
                                    tunnelContext.writeToServerAndFlush(byteBuf);
                                    byteBuf.release();

                                    ctx.channel().attr(TunnelContext.KEY).set(tunnelContext);

                                    listener.onTunnelEstablished(tunnelContext);
                                    log.debug("[ProxyClient] [{}，L:{}，R:{}] SERVER-CONNECTOR-ACTIVE",
                                            tunnelContext.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                }

                                @Override
                                public void channelInactive(ChannelHandlerContext ctx) {
                                    ClientTunnelContext context = (ClientTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                    log.debug("[ProxyClient] [{}，L:{}，R:{}] SERVER-CONNECTOR-INACTIVE",
                                            context != null ? context.getTunnelId() : tunnelContext.getTunnelId(),
                                            ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                    ClientTunnelContext context = (ClientTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                    log.debug("[ProxyClient] [{}，L:{}，R:{}] SERVER-CONNECTOR-EXCEPTION",
                                            context != null ? context.getTunnelId() : tunnelContext.getTunnelId(),
                                            ctx.channel().localAddress(), ctx.channel().remoteAddress(), cause);

                                    listener.caughtTunnelException(tunnelContext, cause);
                                    if (context != null) context.closeLocal(workerGroup.next());
                                    else ctx.close();
                                }
                            });
                        }
                    });

            log.debug("[ProxyClient] [{}] Connecting service-proxy to {} and server-connecter to {}",
                    tunnelContext.getTunnelId(), clientServiceInfo.getAddress(),
                    proxyServerInfo.getAddress().getServerProxyRequestAddress());


        } else {

        }

        Thread.ofVirtual().start(() -> {
            try {
                serviceProxyBootstrap.connect(clientServiceInfo.getAddress()).sync();
                serverConnecterBootstrap.connect(proxyServerInfo.getAddress().getServerProxyRequestAddress()).sync();
            } catch (InterruptedException e) {
                promise.setFailure(e);
            }

            listener.onTunnelEstablished(tunnelContext);
            promise.setSuccess(null);
        });

        return promise;
    }

    @Override
    public Future<?> close() {
        return null;
    }

    @Override
    public void closeNow() {

    }

}
