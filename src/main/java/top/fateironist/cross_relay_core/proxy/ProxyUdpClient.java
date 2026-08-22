package top.fateironist.cross_relay_core.proxy;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import lombok.extern.slf4j.Slf4j;
import top.fateironist.cross_relay_core.Client;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.proxy_client.ProxyClientConnectArgs;
import top.fateironist.cross_relay_core.model.info.ClientServiceInfo;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;
import top.fateironist.cross_relay_core.model.proxy.tunnel.OldTunnelContext;
import top.fateironist.cross_relay_core.proxy.listener.ProxyClientListener;

import java.nio.charset.StandardCharsets;

@Slf4j
public class ProxyUdpClient implements Client {
    private final ClientServiceInfo clientServiceInfo;
    private final ProxyServerInfo proxyServerInfo;
    private final EventLoopGroup workerGroup;

    private final ProxyClientListener listener;

    public ProxyUdpClient(ClientServiceInfo clientServiceInfo, ProxyServerInfo proxyServerInfo, EventLoopGroup workerGroup, ProxyClientListener listener) {
        this.clientServiceInfo = clientServiceInfo;
        this.proxyServerInfo = proxyServerInfo;
        this.workerGroup = workerGroup;
        this.listener = listener;
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
                promise.setFailure(new Exception("ProxyContext server request address is null"));
                return promise;
            }

        }

        ClientTcpTunnelContext tunnelContext= new ClientTcpTunnelContext(args.getTunnelId(),
                args.getProtocol(),
                clientServiceInfo,
                opts.getProxyClientInfo(),
                proxyServerInfo,
                opts.getOriginalRequesterInfo()
        );

        Bootstrap serviceProxyBootstrap = new Bootstrap();
        serviceProxyBootstrap.group(workerGroup)
                .channel(NioDatagramChannel.class)
                .handler(new ChannelInitializer<DatagramChannel>() {
                    @Override
                    protected void initChannel(DatagramChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new SimpleChannelInboundHandler<DatagramPacket>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {
                                ClientUdpTunnelContext context = (ClientUdpTunnelContext) ctx.channel().attr(OldTunnelContext.KEY).get();
                                log.debug("[ProxyClient] UDP [{}，L:{}，R:{}] SERVICE-DATA-RECEIVED",
                                        context.getTunnelId(), ctx.channel().localAddress(), msg.sender());

                                context.writeToServerAndFlush(msg.content());
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                tunnelContext.setClientToServiceChannel(ctx.channel(), clientServiceInfo.getAddress());
                                ctx.channel().attr(OldTunnelContext.KEY).set(tunnelContext);
                                log.debug("[ProxyClient] UDP [{}，L:{}，R:{}] SERVICE-PROXY-ACTIVE",
                                        tunnelContext.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                ClientUdpTunnelContext context = (ClientUdpTunnelContext) ctx.channel().attr(OldTunnelContext.KEY).get();
                                log.debug("[ProxyClient] UDP [{}，L:{}，R:{}] SERVICE-PROXY-INACTIVE",
                                        context != null ? context.getTunnelId() : tunnelContext.getTunnelId(),
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                listener.onTunnelClose(context);
                                if (context != null) context.closeGracefully(listener::closeRemoteTunnel, workerGroup.next());
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ClientUdpTunnelContext context = (ClientUdpTunnelContext) ctx.channel().attr(OldTunnelContext.KEY).get();
                                log.debug("[ProxyClient] UDP [{}，L:{}，R:{}] SERVICE-PROXY-EXCEPTION",
                                        context != null ? context.getTunnelId() : tunnelContext.getTunnelId(),
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress(), cause);

                                listener.caughtTunnelException(context, cause);
                                if (context != null) context.closeLocal(workerGroup.next());
                                else ctx.close();
                            }

                        });
                    }
                });

        Bootstrap serverConnecterBootstrap = new Bootstrap();
        serverConnecterBootstrap.group(workerGroup)
                .channel(NioDatagramChannel.class)
                .handler(new ChannelInitializer<DatagramChannel>() {
                    @Override
                    protected void initChannel(DatagramChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new SimpleChannelInboundHandler<DatagramPacket>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {
                                ClientUdpTunnelContext context = (ClientUdpTunnelContext) ctx.channel().attr(OldTunnelContext.KEY).get();
                                log.debug("[ProxyClient] UDP [{}，L:{}，R:{}] SERVER-DATA-RECEIVED",
                                        context.getTunnelId(), ctx.channel().localAddress(), msg.sender());

                                context.writeToServiceAndFlush(msg.content());
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                tunnelContext.setClientToServerChannel(ctx.channel(), proxyServerInfo.getAddress().getServerProxyRequestAddress());

                                // 1. 发送初始认证信息
                                ByteBuf byteBuf = ctx.alloc().buffer();
                                byteBuf.writeBytes(args.getTunnelId().getBytes(StandardCharsets.UTF_8));
                                tunnelContext.writeToServerAndFlush(byteBuf);
                                byteBuf.release();

                                ctx.channel().attr(OldTunnelContext.KEY).set(tunnelContext);

                                listener.onTunnelEstablished(tunnelContext);
                                log.debug("[ProxyClient] UDP [{}，L:{}，R:{}] SERVER-CONNECTOR-ACTIVE",
                                        tunnelContext.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                ClientUdpTunnelContext context = (ClientUdpTunnelContext) ctx.channel().attr(OldTunnelContext.KEY).get();
                                log.debug("[ProxyClient] UDP [{}，L:{}，R:{}] SERVER-CONNECTOR-INACTIVE",
                                        context != null ? context.getTunnelId() : tunnelContext.getTunnelId(),
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ClientUdpTunnelContext context = (ClientUdpTunnelContext) ctx.channel().attr(OldTunnelContext.KEY).get();
                                log.debug("[ProxyClient] UDP [{}，L:{}，R:{}] SERVER-CONNECTOR-EXCEPTION",
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

        Thread.ofVirtual().start(() -> {
            try {
                serviceProxyBootstrap.bind().sync();
                serverConnecterBootstrap.bind().sync();
            } catch (InterruptedException e) {
                promise.setFailure(e);
            }

            listener.onTunnelEstablished(tunnelContext);
            promise.setSuccess(null);
        });
        return null;
    }

    @Override
    public Future<?> close() {
        return workerGroup.shutdownGracefully();
    }

    @Override
    public void closeNow() {
        workerGroup.shutdownNow();
    }
}
