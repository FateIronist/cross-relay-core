package top.fateironist.cross_relay_core.proxy;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import lombok.extern.slf4j.Slf4j;
import top.fateironist.cross_relay_core.Client;
import top.fateironist.cross_relay_core.ClientStatus;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.proxy_client.ProxyClientConnectArgs;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.control.event.CommonInfo;
import top.fateironist.cross_relay_core.model.proxy.ClientTcpProxyContext;
import top.fateironist.cross_relay_core.model.proxy.ProxyContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ClientTcpTunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;
import top.fateironist.cross_relay_core.proxy.listener.ProxyClientListener;
import top.fateironist.cross_relay_core.util.JsonUtil;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 客户端与服务器一一对应
 */
@Slf4j
public class ProxyTcpClient implements Client {
    private final InetSocketAddress serverProxyRequestAddress;
    private final EventLoopGroup workerGroup;
    private final ProxyClientListener listener;

    private final Map<String, ClientTcpProxyContext> proxyContextMap = new ConcurrentHashMap<>();

    public volatile ClientStatus status = ClientStatus.INIT;

    public ProxyTcpClient(InetSocketAddress serverProxyRequestAddress, EventLoopGroup workerGroup, ProxyClientListener listener) {
        this.serverProxyRequestAddress = serverProxyRequestAddress;
        this.workerGroup = workerGroup;
        this.listener = listener;
    }

    @Override
    public Future<Void> connect(AbstractArgs arg) {
        ProxyClientConnectArgs args = (ProxyClientConnectArgs) arg;
        Promise<Void> promise = workerGroup.next().newPromise();

        ClientTcpProxyContext proxyContext = createContext(args.getProxyId(), args.getControlContext());

        ClientTcpTunnelContext tunnelContext = (ClientTcpTunnelContext) proxyContext.newTunnelContext(args.getTunnelId());

        Bootstrap serviceProxyBootstrap = new Bootstrap();
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
                                ClientTcpTunnelContext context = (ClientTcpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyClient] TCP [{}，L:{}，R:{}] SERVICE-DATA-RECEIVED",
                                        context.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                context.writeToClientProxyAndFlush(msg);
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                tunnelContext.setServiceChannel(ctx.channel());
                                ctx.channel().attr(ProxyContext.KEY).set(proxyContext);
                                ctx.channel().attr(TunnelContext.KEY).set(tunnelContext);
                                log.debug("[ProxyClient] TCP [{}，L:{}，R:{}] SERVICE-PROXY-ACTIVE",
                                        tunnelContext.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                ClientTcpTunnelContext context = (ClientTcpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyClient] TCP [{}，L:{}，R:{}] SERVICE-PROXY-INACTIVE",
                                        context != null ? context.getTunnelId() : tunnelContext.getTunnelId(),
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                listener.onTunnelClose(context);
                                if (context != null) context.closeGracefully();
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ClientTcpTunnelContext context = (ClientTcpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyClient] TCP [{}，L:{}，R:{}] SERVICE-PROXY-EXCEPTION",
                                        context != null ? context.getTunnelId() : tunnelContext.getTunnelId(),
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress(), cause);

                                listener.caughtTunnelException(context, cause);
                                if (context != null) context.closeLocal();
                                else ctx.close();
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
                                ClientTcpTunnelContext context = (ClientTcpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyClient] TCP [{}，L:{}，R:{}] SERVER-DATA-RECEIVED",
                                        context.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                context.writeToServiceAndFlush(msg);
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                tunnelContext.setClientProxyChannel(ctx.channel());

                                // 1. 发送初始认证信息
                                ByteBuf byteBuf = ctx.alloc().buffer();

                                CommonInfo clientProxyRegisterDTO = new CommonInfo(args.getTunnelId(), args.getProxyId());
                                byteBuf.writeBytes(JsonUtil.OBJECT_MAPPER.writeValueAsBytes(clientProxyRegisterDTO));
                                tunnelContext.writeToClientProxyAndFlush(byteBuf);

                                ctx.channel().attr(ProxyContext.KEY).set(proxyContext);
                                ctx.channel().attr(TunnelContext.KEY).set(tunnelContext);

                                listener.onTunnelEstablished(tunnelContext);
                                log.debug("[ProxyClient] TCP [{}，L:{}，R:{}] SERVER-CONNECTOR-ACTIVE",
                                        tunnelContext.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                ClientTcpTunnelContext context = (ClientTcpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyClient] TCP [{}，L:{}，R:{}] SERVER-CONNECTOR-INACTIVE",
                                        context != null ? context.getTunnelId() : tunnelContext.getTunnelId(),
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ClientTcpTunnelContext context = (ClientTcpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyClient] TCP [{}，L:{}，R:{}] SERVER-CONNECTOR-EXCEPTION",
                                        context != null ? context.getTunnelId() : tunnelContext.getTunnelId(),
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress(), cause);

                                listener.caughtTunnelException(tunnelContext, cause);
                                if (context != null) context.closeLocal();
                                else ctx.close();
                            }
                        });
                    }
                });

        log.debug("[ProxyClient] [{}] Connecting service-proxy to {} and server-connecter to {}",
                tunnelContext.getTunnelId(), args.getServiceAddress(), serverProxyRequestAddress);

        Thread.ofVirtual().start(() -> {
            try {
                serviceProxyBootstrap.connect(args.getServiceAddress()).sync();
                serverConnecterBootstrap.connect(serverProxyRequestAddress).sync();
            } catch (InterruptedException e) {
                promise.setFailure(e);
                return;
            }

            listener.onTunnelEstablished(tunnelContext);
            promise.setSuccess(null);
        });

        promise.addListener(f -> {
            if (f.isSuccess()) {
                status = ClientStatus.OPEN;
            }
        });

        return promise;
    }

    public Future<?> closeProxy(String proxyId) {
        if (status == ClientStatus.OPEN) {
            return proxyContextMap.get(proxyId).close();
        }
        return DefaultEventLoopGroup.failFuture(new Exception("Proxy is not open"));
    }

    @Override
    public Future<?> close() {
        if (status == ClientStatus.OPEN || status == ClientStatus.INIT) {
            status = ClientStatus.CLOSING;
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
            promise.addListener(f -> status = ClientStatus.CLOSED);
            return promise;
        }

        return DefaultEventLoopGroup.emptyFuture();
    }

    @Override
    public void closeNow() {
        if (status == ClientStatus.OPEN || status == ClientStatus.INIT) {
            status = ClientStatus.CLOSING;
            proxyContextMap.forEach((k, v) -> {
                try {
                    v.close();
                } catch (Exception e) {

                }
            });
            status = ClientStatus.CLOSED;
        }
    }

    public ClientTcpProxyContext createContext(String proxyId, ControlContext controlContext) {
        ClientTcpProxyContext context = proxyContextMap.computeIfAbsent(proxyId, k -> {
            ClientTcpProxyContext newContext = new ClientTcpProxyContext(k, new ArrayList<>(), controlContext);
            decorateContext(newContext);
            return newContext;
        });
        return context;
    }
    
    public void decorateContext(ClientTcpProxyContext context) {
        context.setCloseProxyHook(ctx -> {
            proxyContextMap.remove(ctx.getProxyId());
        });
    }
}
