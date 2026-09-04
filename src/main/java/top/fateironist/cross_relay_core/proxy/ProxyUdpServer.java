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
import io.netty.util.concurrent.Promise;
import lombok.extern.slf4j.Slf4j;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.Server;
import top.fateironist.cross_relay_core.ServerStatus;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.proxy_server.ClientProxyServerStartArgs;
import top.fateironist.cross_relay_core.model.args.proxy_server.RequesterProxyServerStartArgs;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.control.event.CommonInfo;
import top.fateironist.cross_relay_core.model.proxy.ProxyContext;
import top.fateironist.cross_relay_core.model.proxy.ServerUdpProxyContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ServerUdpTunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;
import top.fateironist.cross_relay_core.proxy.listener.ProxyServerListener;
import top.fateironist.cross_relay_core.util.JsonUtil;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Slf4j
public class ProxyUdpServer implements Server {
    private final EventLoopGroup workerGroup;
    private final ProxyServerListener listener;

    private final Map<String, ServerUdpProxyContext> proxyContextIdMap = new ConcurrentHashMap<>();
    private final Map<InetSocketAddress, ServerUdpProxyContext> proxyContextAddressMap = new ConcurrentHashMap<>();

    public volatile ServerStatus status = ServerStatus.INIT;

    public ProxyUdpServer(EventLoopGroup workerGroup, ProxyServerListener listener) {
        this.workerGroup = workerGroup;
        this.listener = listener;
    }

    @Override
    public Future<Void> start(AbstractArgs arg) {
        if (status == ServerStatus.INIT) {
            ClientProxyServerStartArgs args = (ClientProxyServerStartArgs) arg;
            return startUdpClientProxyServer(args).addListener(f -> {
                status = ServerStatus.RUNNING;
            });
        }

        return DefaultEventLoopGroup.failFuture(new Exception("Server is not in INIT state"));
    }


    public ChannelFuture startUdpClientProxyServer(ClientProxyServerStartArgs args) {
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
                                ServerUdpProxyContext proxyContext = proxyContextAddressMap.get(msg.sender());
                                ServerUdpTunnelContext tunnelContext = null;

                                if (proxyContext == null) {
                                    ByteBuf content = msg.content();
                                    String json = content.readString(content.readableBytes(), CharsetUtil.UTF_8);
                                    CommonInfo registerDTO = JsonUtil.OBJECT_MAPPER.readValue(json, CommonInfo.class);

                                    String tunnelId = registerDTO.getTunnelId();
                                    String proxyId = registerDTO.getProxyId ();

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
        if (status != ServerStatus.RUNNING) {
            return DefaultEventLoopGroup.failFuture(new Exception("Server is not in RUNNING state"));
        }

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

                                    try {
                                        future.get();
                                        log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] TUNNEL-ESTABLISHED",
                                                tunnelContext.getTunnelId(), ctx.channel().localAddress(), msg.sender());
                                    } catch (Exception e) {
                                        log.debug("[ProxyServer] UDP [{}] TUNNEL-ESTABLISH-FAILED", tunnelContext.getTunnelId(), e);
                                    }
                                    return;
                                }

                                log.debug("[ProxyServer] UDP [{}，L:{}，R:{}] REQUESTER-DATA-RECEIVED",
                                        tunnelContext.getTunnelId(), ctx.channel().localAddress(), msg.sender());

                                tunnelContext.writeToClientProxyAndFlush(msg.content());
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                ServerUdpProxyContext proxyContext = createContext(ProxyContext.generateProxyId(), List.of(ctx), args.getControlContext());
                                ctx.channel().attr(ProxyContext.KEY).set(proxyContext);
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

    public Future<?> closeProxy(String proxyId) {
        return proxyContextIdMap.get(proxyId).close();
    }

    @Override
    public java.util.concurrent.Future<?> shutdown() {
        if (status == ServerStatus.RUNNING || status == ServerStatus.INIT) {
            status = ServerStatus.STOPPING;

            Promise<?> promise = DefaultEventLoopGroup.newPromise();
            Thread.ofVirtual().start(() -> {
                proxyContextIdMap.forEach((k, v) -> {
                    try {
                        v.close().sync();
                    } catch (Exception e) {
                        promise.setFailure(e);
                    }
                });
            });

            promise.addListener(f -> status = ServerStatus.SHUTDOWN);
            return promise;
        }
        return DefaultEventLoopGroup.emptyFuture();
    }

    @Override
    public void shutdownNow() {
        if (status == ServerStatus.RUNNING || status == ServerStatus.INIT) {
            status = ServerStatus.STOPPING;
            proxyContextIdMap.forEach((k, v) -> {
                try {
                    v.close();
                } catch (Exception e) {

                }
            });
            status = ServerStatus.SHUTDOWN;
        }
    }

    public ServerUdpProxyContext createContext(String proxyId, List<ChannelHandlerContext> handlerContexts, ControlContext controlContext) {
        ServerUdpProxyContext context = proxyContextIdMap.computeIfAbsent(proxyId, k -> {
            ServerUdpProxyContext newContext = new ServerUdpProxyContext(proxyId, handlerContexts, controlContext) {
                Consumer<TunnelContext> tunnelCloseHook = super.tunnelCloseHook();
                @Override
                protected Consumer<TunnelContext> tunnelCloseHook() {
                    return new Consumer<TunnelContext>() {
                        @Override
                        public void accept(TunnelContext context) {
                            ServerUdpTunnelContext tunnelContext = (ServerUdpTunnelContext) context;
                            tunnelCloseHook.accept(context);
                            addressContextMap.remove(tunnelContext.getClientProxyAddress());
                            addressContextMap.remove(tunnelContext.getRequesterAddress());

                            // 移除Address-代理上下文
                            proxyContextAddressMap.remove(tunnelContext.getClientProxyAddress());
                            proxyContextAddressMap.remove(tunnelContext.getRequesterAddress());
                        }
                    };
                }
            };

            decorateContext(newContext);
            return newContext;
        });
        return context;
    }

    public void decorateContext(ServerUdpProxyContext context) {
        context.setCloseProxyHook(ctx -> {
            proxyContextIdMap.remove(ctx.getProxyId());
        });
    }
}
