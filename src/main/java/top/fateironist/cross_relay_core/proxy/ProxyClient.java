package top.fateironist.cross_relay_core.proxy;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Promise;
import top.fateironist.cross_relay_core.Client;
import top.fateironist.cross_relay_core.Server;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.info.ClientServiceInfo;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;
import top.fateironist.cross_relay_core.model.options.Options;
import top.fateironist.cross_relay_core.model.options.proxy_client.ProxyClientConnectOptions;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ClientTunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;
import top.fateironist.cross_relay_core.proxy.listener.ProxyClientListener;

import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.concurrent.Future;

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
    public Future<Void> connect(Options option) {
        ProxyClientConnectOptions options = (ProxyClientConnectOptions) option;
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

        ClientTunnelContext tunnelContext= new ClientTunnelContext(options.tunnelId,
                options.protocol,
                clientServiceInfo,
                options.proxyClientInfo,
                proxyServerInfo,
                options.originalRequesterInfo
                );

        if (options.protocol == TransportLayerProtocol.TCP) {
            Bootstrap serviceProxyBootstrap = new Bootstrap();
            serviceProxyBootstrap.group(workerGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, false)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new LoggingHandler(LogLevel.INFO));
                            pipeline.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                    ClientTunnelContext context = (ClientTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();

                                    context.writeToServerAndFlush(msg);
                                }

                                @Override
                                public void channelActive(ChannelHandlerContext ctx) {
                                    tunnelContext.setClientToServiceChannel(ctx.channel());
                                    ctx.channel().attr(TunnelContext.KEY).set(tunnelContext);
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {

                                }

                                @Override
                                public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                }

                            });
                        }
                    });

            Bootstrap serverConnecterBootstrap = new Bootstrap();
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

                                    context.writeToServiceAndFlush(msg);
                                }

                                @Override
                                public void channelActive(ChannelHandlerContext ctx) {
                                    tunnelContext.setClientToServerChannel(ctx.channel());

                                    // 1. 发送初始认证信息
                                    ByteBuf byteBuf = ctx.alloc().buffer();
                                    byteBuf.writeBytes(((ProxyClientConnectOptions) option).tunnelId.getBytes(StandardCharsets.UTF_8));
                                    tunnelContext.writeToServerAndFlush(byteBuf);
                                    byteBuf.release();

                                    ctx.channel().attr(TunnelContext.KEY).set(tunnelContext);
                                }

                                @Override
                                public void channelInactive(ChannelHandlerContext ctx) {

                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {

                                }
                            });
                        }
                    });

            Thread.ofVirtual().start(() -> {
                try {
                serviceProxyBootstrap.connect(clientServiceInfo.getAddress()).sync();
                serverConnecterBootstrap.connect(proxyServerInfo.getAddress().getServerProxyRequestAddress()).sync();
                } catch (InterruptedException e) {
                    promise.setFailure(e);
                }

                promise.setSuccess(null);
            });
        } else {

        }







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
