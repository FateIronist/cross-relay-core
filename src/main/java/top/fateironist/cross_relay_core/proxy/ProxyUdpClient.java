package top.fateironist.cross_relay_core.proxy;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import lombok.extern.slf4j.Slf4j;
import top.fateironist.constack.Container;
import top.fateironist.constack.Promise;
import top.fateironist.cross_relay_core.Client;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.proxy_client.ProxyClientConnectArgs;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.control.event.CommonInfo;
import top.fateironist.cross_relay_core.model.proxy.ClientUdpProxyContext;
import top.fateironist.cross_relay_core.model.proxy.ProxyContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ClientUdpTunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;
import top.fateironist.cross_relay_core.proxy.listener.ProxyClientListener;
import top.fateironist.cross_relay_core.util.JsonUtil;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

/**
 * UDP 代理客户端：顶层容器（无共享资源，load 即激活）；
 * 每次 connect 在所属代理子容器下创建一条隧道，隧道双端地址（本地服务 + server-proxy）与 duplex 齐备才激活。
 * Listener 为纯通知钩子，不参与状态机与资源回收。
 */
@Slf4j
public class ProxyUdpClient extends Container implements Client {
    private final InetSocketAddress serverProxyRequestAddress;
    private final EventLoopGroup workerGroup;
    private final ProxyClientListener listener;

    // 路由索引：proxyId -> 代理容器；索引项随容器销毁经 Effect 移除
    private final Map<String, ClientUdpProxyContext> proxyContextMap = new ConcurrentHashMap<>();

    public ProxyUdpClient(InetSocketAddress serverProxyRequestAddress, EventLoopGroup workerGroup, ProxyClientListener listener) {
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

    /** 创建代理容器（若不存在）与隧道，并绑定本端 duplex 数据报通道 */
    private void connectTunnel(ProxyClientConnectArgs args) {
        ClientUdpProxyContext proxyContext = createContext(args.getProxyId(), args.getControlContext());
        proxyContext.load();

        ClientUdpTunnelContext tunnelContext = (ClientUdpTunnelContext) proxyContext.newTunnelContext(args.getTunnelId());

        Bootstrap duplexBootstrap = new Bootstrap();
        duplexBootstrap.group(workerGroup)
                .channel(NioDatagramChannel.class)
                .handler(new ChannelInitializer<DatagramChannel>() {
                    @Override
                    protected void initChannel(DatagramChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new SimpleChannelInboundHandler<DatagramPacket>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {
                                ClientUdpProxyContext proxyContext = (ClientUdpProxyContext) ctx.channel().attr(ProxyContext.KEY).get();
                                ClientUdpTunnelContext tunnelContext = proxyContext.getTunnelContext(msg.sender());

                                log.debug("[ProxyClient] UDP [{}，L:{}，R:{}] SERVER-DATA-RECEIVED",
                                        tunnelContext.getTunnelId(), ctx.channel().localAddress(), msg.sender());
                                tunnelContext.writeToOpposite(msg.sender(), msg.content());
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                proxyContext.addHandlerContext(ctx);
                                tunnelContext.setDuplexChannel(ctx.channel());
                                tunnelContext.setServiceAddress(args.getServiceAddress());
                                tunnelContext.setServerProxyAddress(serverProxyRequestAddress);
                                ctx.channel().attr(ProxyContext.KEY).set(proxyContext);

                                // 发送初始认证信息
                                ByteBuf byteBuf = ctx.alloc().buffer();
                                CommonInfo clientProxyRegisterDTO = new CommonInfo(args.getTunnelId(), args.getProxyId());
                                byteBuf.writeBytes(JsonUtil.OBJECT_MAPPER.writeValueAsBytes(clientProxyRegisterDTO));
                                tunnelContext.writeToServerProxyAndFlush(byteBuf);
                                byteBuf.release();

                                listener.onTunnelEstablished(tunnelContext);
                                log.debug("[ProxyClient] UDP [{}，L:{}，R:{}] SERVER-CONNECTOR-ACTIVE",
                                        tunnelContext.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                ClientUdpTunnelContext context = (ClientUdpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyClient] UDP [{}，L:{}，R:{}] SERVER-CONNECTOR-INACTIVE",
                                        context != null ? context.getTunnelId() : tunnelContext.getTunnelId(),
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                listener.onTunnelClose(context);
                                if (context != null) context.closeGracefully();
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ClientUdpTunnelContext context = (ClientUdpTunnelContext) ctx.channel().attr(TunnelContext.KEY).get();
                                log.debug("[ProxyClient] UDP [{}，L:{}，R:{}] SERVER-CONNECTOR-EXCEPTION",
                                        context != null ? context.getTunnelId() : tunnelContext.getTunnelId(),
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress(), cause);

                                listener.caughtTunnelException(tunnelContext, cause);
                                if (context != null) context.closeLocal();
                                else ctx.close();
                            }
                        });
                    }
                });

        log.debug("[ProxyClient] [{}] Binding UDP duplex channel for service {} and server {}",
                tunnelContext.getTunnelId(), args.getServiceAddress(),
                serverProxyRequestAddress);

        try {
            duplexBootstrap.bind().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public java.util.concurrent.Future<?> closeProxy(String proxyId) {
        ClientUdpProxyContext context = proxyContextMap.get(proxyId);
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
    public ClientUdpProxyContext createContext(String proxyId, ControlContext controlContext) {
        return proxyContextMap.computeIfAbsent(proxyId, k -> {
            try {
                ClientUdpProxyContext context = (ClientUdpProxyContext) child(k, new ArrayList<>(), controlContext).get();
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
        return new ClientUdpProxyContext(parent, proxyId, handlerContexts, controlContext);
    }
}
