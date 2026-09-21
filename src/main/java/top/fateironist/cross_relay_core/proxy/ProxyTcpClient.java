package top.fateironist.cross_relay_core.proxy;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;
import top.fateironist.constack.Container;
import top.fateironist.constack.Promise;
import top.fateironist.cross_relay_core.Client;
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
import java.util.concurrent.Future;

/**
 * TCP 代理客户端：顶层容器（无共享资源，load 即激活）；
 * 每次 connect 在所属代理子容器下创建一条隧道，隧道双端（本地服务 + server-proxy）齐备才激活。
 * Listener 为纯通知钩子，不参与状态机与资源回收。
 */
@Slf4j
public class ProxyTcpClient extends Container implements Client {
    private final InetSocketAddress serverProxyRequestAddress;
    private final EventLoopGroup workerGroup;
    private final ProxyClientListener listener;

    // 路由索引：proxyId -> 代理容器；索引项随容器销毁经 Effect 移除
    private final Map<String, ClientTcpProxyContext> proxyContextMap = new ConcurrentHashMap<>();

    public ProxyTcpClient(InetSocketAddress serverProxyRequestAddress, EventLoopGroup workerGroup, ProxyClientListener listener) {
        super();
        this.serverProxyRequestAddress = serverProxyRequestAddress;
        this.workerGroup = workerGroup;
        this.listener = listener;
    }

    @Override
    public Future<Void> connect(AbstractArgs arg) {
        ProxyClientConnectArgs args = (ProxyClientConnectArgs) arg;
        Promise<Void> promise = new Promise<>();

        // 顶层容器仅首次 connect 前需要激活（无共享资源）；此后每次 connect 创建隧道
        if (state() == State.PENDING) {
            load();
        }

        Thread.startVirtualThread(() -> {
            try {
                connectTunnel(args);
                promise.setSuccess(null);
            } catch (Exception e) {
                promise.setFailure(e);
            }
        });

        return promise;
    }

    /** 创建代理容器（若不存在）与隧道，并同时建立本地服务与 server-proxy 两端连接 */
    private void connectTunnel(ProxyClientConnectArgs args) {
        ClientTcpProxyContext proxyContext = createContext(args.getProxyId(), args.getControlContext());
        proxyContext.load();

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
                            public void channelInactive(ChannelHandlerContext ctx) {
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
                            public void channelActive(ChannelHandlerContext ctx) throws Exception {
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

        try {
            serviceProxyBootstrap.connect(args.getServiceAddress()).sync();
            serverConnecterBootstrap.connect(serverProxyRequestAddress).sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public java.util.concurrent.Future<?> closeProxy(String proxyId) {
        ClientTcpProxyContext context = proxyContextMap.get(proxyId);
        if (context == null) {
            return DefaultEventLoopGroup.failFuture(new IllegalStateException("unknown proxyId: " + proxyId));
        }
        return context.close();
    }

    @Override
    public java.util.concurrent.Future<?> close() {
        return disposal();
    }

    @Override
    public void closeNow() {
        try {
            disposal().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 取得或创建代理子容器并登记路由索引；索引项随容器销毁经 Effect 移除，避免悬挂引用。
     */
    public ClientTcpProxyContext createContext(String proxyId, ControlContext controlContext) {
        return proxyContextMap.computeIfAbsent(proxyId, k -> {
            try {
                ClientTcpProxyContext context = (ClientTcpProxyContext) child(k, new ArrayList<>(), controlContext).get();
                context.effect(c -> proxyContextMap.remove(k));
                return context;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    protected Container createChild(Container parent, Object... args) {
        String proxyId = (String) args[0];
        @SuppressWarnings("unchecked")
        java.util.List<ChannelHandlerContext> handlerContexts = (java.util.List<ChannelHandlerContext>) args[1];
        ControlContext controlContext = (ControlContext) args[2];
        return new ClientTcpProxyContext(parent, proxyId, handlerContexts, controlContext);
    }
}
