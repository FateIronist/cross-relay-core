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
import top.fateironist.cross_relay_core.ClientStatus;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ProxyUdpClient implements Client {
    private final InetSocketAddress serverProxyRequestAddress;
    private final EventLoopGroup workerGroup;
    private final ProxyClientListener listener;

    private final Map<String, ClientUdpProxyContext> proxyContextMap = new ConcurrentHashMap<>();

    public volatile ClientStatus status = ClientStatus.INIT;

    public ProxyUdpClient(InetSocketAddress serverProxyRequestAddress, EventLoopGroup workerGroup, ProxyClientListener listener) {
        this.serverProxyRequestAddress = serverProxyRequestAddress;
        this.workerGroup = workerGroup;
        this.listener = listener;
    }

    @Override
    public Future<Void> connect(AbstractArgs arg) {
        ProxyClientConnectArgs args = (ProxyClientConnectArgs) arg;
        Promise<Void> promise = workerGroup.next().newPromise();

        ClientUdpProxyContext proxyContext = createContext(args.getProxyId(), args.getControlContext());

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
                            public void channelActive(ChannelHandlerContext ctx) {
                                proxyContext.addHandlerContext(ctx);
                                tunnelContext.setDuplexChannel(ctx.channel());
                                tunnelContext.setServiceAddress(args.getServiceAddress());
                                tunnelContext.setServerProxyAddress(serverProxyRequestAddress);
                                ctx.channel().attr(ProxyContext.KEY).set(proxyContext);

                                // 发送初始认证信息
                                ByteBuf byteBuf = ctx.alloc().buffer();
                                CommonInfo clientProxyRegisterDTO = new CommonInfo(args.getTunnelId(), args.getProxyId());
                                byteBuf.writeBytes(JsonUtil.OBJECT_MAPPER.writeValueAsBytes(clientProxyRegisterDTO).getBytes(StandardCharsets.UTF_8));
                                tunnelContext.writeToServerProxyAndFlush(byteBuf);
                                byteBuf.release();

                                listener.onTunnelEstablished(tunnelContext);
                                log.debug("[ProxyClient] UDP [{}，L:{}，R:{}] SERVER-CONNECTOR-ACTIVE",
                                        tunnelContext.getTunnelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) throws Exception {
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

        Thread.ofVirtual().start(() -> {
            try {
                duplexBootstrap.bind().sync();
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

    public ClientUdpProxyContext createContext(String proxyId, ControlContext controlContext) {
        ClientUdpProxyContext context = proxyContextMap.computeIfAbsent(proxyId, k -> {
            ClientUdpProxyContext newContext = new ClientUdpProxyContext(k, new ArrayList<>(), controlContext);
            decorateContext(newContext);
            return newContext;
        });
        return context;
    }

    public void decorateContext(ClientUdpProxyContext context) {
        context.setCloseProxyHook(ctx -> {
            proxyContextMap.remove(ctx.getProxyId());
        });
    }
}
