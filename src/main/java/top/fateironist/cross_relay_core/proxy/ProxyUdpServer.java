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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import top.fateironist.constack.Container;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.FutureBridge;
import top.fateironist.cross_relay_core.Server;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.proxy_server.ClientProxyServerStartArgs;
import top.fateironist.cross_relay_core.model.args.proxy_server.RequesterProxyServerStartArgs;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.control.event.CommonInfo;
import top.fateironist.cross_relay_core.model.proxy.ProxyContext;
import top.fateironist.cross_relay_core.model.proxy.ServerUdpProxyContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ServerUdpTunnelContext;
import top.fateironist.cross_relay_core.proxy.listener.ProxyServerListener;
import top.fateironist.cross_relay_core.util.JsonUtil;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * UDP 代理服务端：顶层容器，load = 绑定共享的 client-proxy 数据报通道即激活；
 * 每个 requester 数据报通道对应一个 ServerUdpProxyContext 子容器，随控制连接级联销毁。
 * Listener 为纯通知钩子，不参与状态机与资源回收。
 */
@Slf4j
public class ProxyUdpServer extends Container implements Server {
    private final EventLoopGroup workerGroup;
    private final ProxyServerListener listener;

    // 路由索引：proxyId -> 代理容器；索引项随容器销毁经 Effect 移除
    private final Map<String, ServerUdpProxyContext> proxyContextIdMap = new ConcurrentHashMap<>();
    // 路由索引：client-proxy 地址 -> 代理容器；索引项随隧道注销经受控方法移除
    private final Map<InetSocketAddress, ServerUdpProxyContext> proxyContextAddressMap = new ConcurrentHashMap<>();

    /** 共享的 client-proxy 数据报 channel（本容器绑定的 Netty Channel） */
    @Getter
    private volatile Channel clientProxyChannel;

    public ProxyUdpServer(EventLoopGroup workerGroup, ProxyServerListener listener) {
        super();
        this.workerGroup = workerGroup;
        this.listener = listener;

        // Effect：关闭共享 client-proxy 数据报 channel（本容器绑定持有，外部注入的 EventLoopGroup 不清理）
        effect(c -> {
            Channel channel = clientProxyChannel;
            if (channel != null) {
                channel.close();
            }
        });
    }

    @Override
    public java.util.concurrent.Future<Void> start(AbstractArgs arg) {
        return FutureBridge.bridge(load(arg), null);
    }

    @Override
    protected java.util.concurrent.Future<Object> start(Object... args) {
        ClientProxyServerStartArgs serverArgs = (ClientProxyServerStartArgs) args[0];
        var options = serverArgs.getOptions();

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
                            protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) throws Exception {
                                ServerUdpProxyContext proxyContext = proxyContextAddressMap.get(msg.sender());
                                ServerUdpTunnelContext tunnelContext = null;

                                if (proxyContext == null) {
                                    ByteBuf content = msg.content();
                                    String json = content.readString(content.readableBytes(), CharsetUtil.UTF_8);
                                    CommonInfo registerDTO = JsonUtil.OBJECT_MAPPER.readValue(json, CommonInfo.class);

                                    String tunnelId = registerDTO.getTunnelId();
                                    String proxyId = registerDTO.getProxyId();

                                    proxyContext = proxyContextIdMap.get(proxyId);

                                    if (proxyContext == null) {
                                        log.debug("[ProxyServer] UDP [L:{}] CLIENT-TO-SERVER-REGISTER-FAILED: unknown proxyContext id:{}", ctx.channel().localAddress(), proxyId);
                                        return;
                                    }

                                    proxyContextAddressMap.put(msg.sender(), proxyContext);

                                    tunnelContext = proxyContext.registerClientProxy(tunnelId, msg.sender(), ctx.channel());

                                    if (tunnelContext != null) {
                                        log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] CLIENT-TO-SERVER-REGISTER",
                                                tunnelContext.getTunnelId(), ctx.channel().localAddress(), msg.sender());
                                        return;
                                    }

                                    log.debug("[ProxyServer] UDP [L:{}] CLIENT-TO-SERVER-REGISTER-FAILED: unknown tunnel id:{}", ctx.channel().localAddress(), tunnelId);

                                } else {
                                    tunnelContext = proxyContext.getTunnelContext(msg.sender());
                                }

                                log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] CLIENT-DATA-RECEIVED",
                                        tunnelContext.getTunnelId(), ctx.channel().localAddress(), msg.sender());
                                tunnelContext.writeToRequesterAndFlush(msg.content());
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] CLIENT-TO-SERVER-ACTIVE",
                                        null, ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                // UDP channel 为多个连接复用，单个容器销毁不随 channel inactive 触发
                                log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] CLIENT-TO-SERVER-INACTIVE",
                                        null,
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                // UDP channel 为多个连接复用，异常不直接导致整通道关闭
                                log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] CLIENT-TO-SERVER-EXCEPTION",
                                        null,
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress(), cause);

                                ctx.close();
                            }
                        });
                    }
                });

        log.debug("[ProxyServer] Binding UDP client-proxy server to port {}", options.getClientProxyPort());
        top.fateironist.constack.Promise<Object> promise = new top.fateironist.constack.Promise<>();
        ChannelFuture bindFuture = udpClientProxyBootstrap.bind(options.getClientProxyPort());
        bindFuture.addListener(f -> {
            if (f.isSuccess()) {
                clientProxyChannel = bindFuture.channel();
                promise.setSuccess(null);
            } else {
                promise.setFailure(f.cause());
            }
        });
        return promise;
    }

    /**
     * 启动一个 requester 数据报通道并创建对应代理子容器；requester 注册经超时看护，不阻塞事件循环。
     */
    public ChannelFuture startUdpRequesterProxyServer(RequesterProxyServerStartArgs args) {
        var options = args.getOptions();
        Bootstrap reqBootstrap = new Bootstrap();
        reqBootstrap.group(workerGroup)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_RCVBUF, options.getMaxUdpReceiveBuffer())
                .option(ChannelOption.SO_SNDBUF, options.getMaxUdpSendBuffer())
                .handler(new ChannelInitializer<DatagramChannel>() {
                    @Override
                    protected void initChannel(DatagramChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new IdleStateHandler(0, 0, options.getRequesterTimeout(), TimeUnit.MILLISECONDS));
                        pipeline.addLast(new SimpleChannelInboundHandler<DatagramPacket>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {
                                if (!listener.beforeRequesterToServerConnectionAccept(TransportLayerProtocol.UDP, ctx.channel(), msg.sender())) {
                                    log.debug("[ProxyServer] REJECT requester {} (beforeRequesterToServerConnectionAccept returned false)", msg.sender());
                                    return;
                                }

                                ServerUdpProxyContext proxyContext = (ServerUdpProxyContext) ctx.channel().attr(ProxyContext.KEY).get();
                                ServerUdpTunnelContext tunnelContext = proxyContext.getTunnelContext(msg.sender());

                                if (tunnelContext == null) {
                                    tunnelContext = (ServerUdpTunnelContext) proxyContext.newTunnelContext();
                                    tunnelContext.setRequester(msg.sender(), ctx.channel());

                                    log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] REQUESTER-ACTIVE",
                                            tunnelContext.getTunnelId(), ctx.channel().localAddress(), msg.sender());

                                    Future<Object> future = proxyContext.registerRequester(tunnelContext.getTunnelId(), msg.sender(), ctx.channel());
                                    final ServerUdpTunnelContext pendingTunnel = tunnelContext;
                                    future.addListener(f -> {
                                        if (f.isSuccess()) {
                                            log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] TUNNEL-ESTABLISHED",
                                                    pendingTunnel.getTunnelId(), ctx.channel().localAddress(), msg.sender());
                                        } else {
                                            log.debug("[ProxyServer] UDP [{}] TUNNEL-ESTABLISH-FAILED", pendingTunnel.getTunnelId(), f.cause());
                                        }
                                    });
                                    return;
                                }

                                log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] REQUESTER-DATA-RECEIVED",
                                        tunnelContext.getTunnelId(), ctx.channel().localAddress(), msg.sender());

                                tunnelContext.writeToClientProxyAndFlush(msg.content());
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                // 创建代理子容器并启动其生命周期（请求数据报 channel 由本方法所属 Server 持有并注册回收）
                                ServerUdpProxyContext proxyContext = createContext(ProxyContext.generateProxyId(), List.of(ctx), args.getControlContext());
                                ctx.channel().attr(ProxyContext.KEY).set(proxyContext);
                                proxyContext.load(args);
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                // UDP channel 为多个连接复用，单个容器销毁不随 channel inactive 触发
                                log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] REQUESTER-TO-SERVER-INACTIVE",
                                        null,
                                        ctx.channel().localAddress(), ctx.channel().remoteAddress());

                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                // UDP channel 为多个连接复用，异常不直接导致整通道关闭
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

    public java.util.concurrent.Future<?> closeProxy(String proxyId) {
        ServerUdpProxyContext context = proxyContextIdMap.get(proxyId);
        if (context == null) {
            return DefaultEventLoopGroup.failFuture(new IllegalStateException("unknown proxyId: " + proxyId));
        }
        return context.close();
    }

    @Override
    public java.util.concurrent.Future<?> shutdown() {
        return disposal();
    }

    @Override
    public void shutdownNow() {
        try {
            disposal().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 创建代理子容器并登记路由索引；索引项随容器销毁经 Effect 移除，避免悬挂引用。
     */
    public ServerUdpProxyContext createContext(String proxyId, List<ChannelHandlerContext> handlerContexts, ControlContext controlContext) {
        try {
            ServerUdpProxyContext context = (ServerUdpProxyContext) child(proxyId, handlerContexts, controlContext).get();
            proxyContextIdMap.put(proxyId, context);
            context.effect(c -> proxyContextIdMap.remove(proxyId));
            return context;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** 受控方法：隧道注销时移除 client-proxy 地址路由索引（由子容器注销流程回调） */
    public void unregisterProxyAddress(InetSocketAddress address) {
        proxyContextAddressMap.remove(address);
    }

    @Override
    protected Container createChild(Container parent, Object... args) {
        String proxyId = (String) args[0];
        @SuppressWarnings("unchecked")
        List<ChannelHandlerContext> handlerContexts = (List<ChannelHandlerContext>) args[1];
        ControlContext controlContext = (ControlContext) args[2];
        return new ServerUdpProxyContext(parent, proxyId, handlerContexts, controlContext);
    }
}
