package top.fateironist.cross_relay_core.control;

import com.fasterxml.jackson.core.type.TypeReference;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.Promise;
import lombok.extern.slf4j.Slf4j;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.Server;
import top.fateironist.cross_relay_core.ServerStatus;
import top.fateironist.cross_relay_core.control.handler.EventEncryptHandler;
import top.fateironist.cross_relay_core.control.handler.IdleEventHandler;
import top.fateironist.cross_relay_core.control.handler.JsonDecoder;
import top.fateironist.cross_relay_core.control.handler.JsonEncoder;
import top.fateironist.cross_relay_core.control.listener.ControlServerListener;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.control.event.ControlEvent;
import top.fateironist.cross_relay_core.model.control.ControlProtocolEventEnum;
import top.fateironist.cross_relay_core.model.control.event.Error;
import top.fateironist.cross_relay_core.model.info.ProxyClientInfo;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;
import top.fateironist.cross_relay_core.model.args.control.ControlServerStartArgs;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.util.EncryptUtil;
import top.fateironist.cross_relay_core.util.JsonUtil;

import javax.crypto.SecretKey;
import java.net.InetSocketAddress;
import java.security.PublicKey;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * 只负责构建加密通道和传递身份信息，其余如注册代理、业务事件发送由上层业务层在ControlClientListener通过ControlManager实现
 */
@Slf4j
public class ControlServer implements Server {
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;

    public volatile ServerStatus status = ServerStatus.INIT;

    // 此处依赖用于向客户端发送服务端信息
    private final ProxyServerInfo proxyServerInfo;
    // 生命周期钩子
    private final ControlServerListener controlServerListener;
    private final Map<String, ControlContext> controlContextMap = new ConcurrentHashMap<>();

    public ControlServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup, ProxyServerInfo proxyServerInfo, ControlServerListener controlServerListener) {
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.proxyServerInfo = proxyServerInfo;
        this.controlServerListener = controlServerListener;
    }

    @Override
    public ChannelFuture start(AbstractArgs arg) {
        if (status == ServerStatus.INIT) {
            ControlServerStartArgs args = (ControlServerStartArgs) arg;
            var opts = args.getOptions();

            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new ChannelInitializer<NioServerSocketChannel>() {
                    @Override
                    protected void initChannel(NioServerSocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                                Channel childChannel = (Channel) msg;

                                // 1.接收器，处理ip封禁等问题
                                if (controlServerListener.beforeAccept(childChannel)) {
                                    log.debug("[ControlServer] ACCEPT {} -> {}", childChannel.remoteAddress(), childChannel.localAddress());
                                    ctx.fireChannelRead(msg);
                                } else {
                                    childChannel.unsafe().close(childChannel.voidPromise());
                                    log.debug("[ControlServer] REJECT {} (beforeConnect returned false)", childChannel.remoteAddress());
                                }
                            }
                        });
                    }
                })
                .option(ChannelOption.SO_BACKLOG, opts.getMaxConnections())
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new IdleStateHandler(opts.getPingTimeout(), 0, 0, TimeUnit.MILLISECONDS));
                        pipeline.addLast(new IdleEventHandler(controlServerListener::onTimeOut));
                        pipeline.addLast(new LengthFieldBasedFrameDecoder(65535, 0, 4, 0, 4));
                        pipeline.addLast(new LengthFieldPrepender(4));
                        pipeline.addLast(new EventEncryptHandler());
                        pipeline.addLast(new JsonEncoder());
                        pipeline.addLast(new JsonDecoder<>(new TypeReference<ControlEvent<Map<String, Object>>>() {}));
                        pipeline.addLast(new SimpleChannelInboundHandler<ControlEvent<Map<String, Object>>>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ControlEvent<Map<String, Object>> event) {
                                ControlContext context = ctx.channel().attr(ControlContext.KEY).get();
                                log.debug("[ControlServer] [{}，L:{}，R:{}] RECEIVED data", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                // 3.进行密钥互换
                                if (!context.isEncrypted()) {
                                    if (ControlProtocolEventEnum.SESSION_PUBLIC_KEY.equals(event.getType())) {
                                        log.debug("[ControlServer] [{}，L:{}，R:{}] PUBLIC-KEY-RECEIVED", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                        String pubKey = (String) event.getBody().get("publicKey");
                                        PublicKey publicKey = EncryptUtil.base64ToPublicKey(pubKey);

                                        SecretKey secretKey = EncryptUtil.generateAESKey();
                                        String secretKeyStr = EncryptUtil.aesKeyToString(secretKey);
                                        secretKeyStr = EncryptUtil.rsaEncrypt(secretKeyStr, publicKey);

                                        ControlEvent<Map<String, Object>> handShake = new ControlEvent<>(ControlProtocolEventEnum.SESSION_SECRET_KEY.getType(), Map.of("secretKey", secretKeyStr));
                                        context.writeAndFlush(handShake);

                                        ctx.channel().attr(EventEncryptHandler.SESSION_PUBLIC_KEY).set(publicKey);
                                        ctx.channel().attr(EventEncryptHandler.SESSION_SECRET_KEY).set(secretKey);

                                        log.debug("[ControlServer] [{}，L:{}，R:{}] SECRET-KEY-SENT", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                        return;
                                    }

                                    if (ControlProtocolEventEnum.SESSION_SECRET_ACK.equals(event.getType())) {
                                        ctx.channel().attr(EventEncryptHandler.SESSION_PUBLIC_KEY).set(null);

                                        // 补全客户端代理信息，尤其是credentials
                                        ProxyClientInfo proxyClientInfo = JsonUtil.OBJECT_MAPPER.convertValue(event.getBody(), ProxyClientInfo.class);
                                        context.getProxyClientInfo().setAdditional(proxyClientInfo);

                                        context.setEncrypted(true);

                                        log.debug("[ControlServer] [{}，L:{}，R:{}] ENCRYPTED", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                        // 4.进行权限验证，验证通过则允许正式连接
                                        if (controlServerListener.beforePermit(context, event)) {
                                            context.setPermit(true);
                                            log.debug("[ControlServer] [{}，L:{}，R:{}] PERMITTED", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                            context.writeAndFlush(new ControlEvent<>(ControlProtocolEventEnum.CONNECTION_PERMIT.getType(), Map.of("controlId", context.getControlId(), "proxyServerInfo", proxyServerInfo)));
                                        }else {
                                            log.debug("[ControlServer] [{}，L:{}，R:{}] DENIED", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                            context.writeAndFlush(new ControlEvent<>(ControlProtocolEventEnum.ERROR.getType(), new Error("Control Channel Not Permitted!")));
                                            context.close();
                                        }

                                        log.debug("[ControlServer] [{}，L:{}，R:{}] HANDSHAKE-COMPLETE", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                        return;
                                    }

                                    log.debug("[ControlServer] [{}，L:{}，R:{}] REJECTED-NOT-ENCRYPTED", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                    context.writeAndFlush(new ControlEvent<>(ControlProtocolEventEnum.ERROR.getType(), new Error("Control Channel Not Encrypted Yet!")));
                                    context.handleAbnormalEvent(event);

                                    return;
                                }

                                if (!context.isPermit()) {
                                    log.debug("[ControlServer] [{}，L:{}，R:{}] DENIED", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                    context.writeAndFlush(new ControlEvent<>(ControlProtocolEventEnum.ERROR.getType(), new Error("Control Channel Not Permitted!")));
                                    context.close();

                                    return;
                                }

                                if (ControlProtocolEventEnum.PING.equals(event.getType())) {
                                    log.debug("[ControlServer] [{}，L:{}，R:{}] PING-PONG", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                    long pingTime = (long) event.getBody().get("pingTime");
                                    context.writeAndFlush(new ControlEvent<>(ControlProtocolEventEnum.PONG.getType(), Map.of("pingTime", pingTime)));
                                }else {
                                    // 5.进行消息处理
                                    log.debug("[ControlServer] [{}，L:{}，R:{}] DELEGATE-ON-MESSAGE", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                    controlServerListener.onEvent(context, event);
                                }
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                String id = UUID.randomUUID().toString();
                                ProxyClientInfo proxyClientInfo = new ProxyClientInfo((InetSocketAddress) ctx.channel().remoteAddress());

                                ControlContext context = createControlContext(proxyClientInfo, ctx.channel());
                                ctx.channel().attr(ControlContext.KEY).set(context);

                                log.debug("[ControlServer] [{}，L:{}，R:{}] ACTIVE", id, ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                // 2.接收完成，分配id完成，但还未分发，id顺便在permit时分发
                                controlServerListener.afterAccept(context);
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                ControlContext context = ctx.channel().attr(ControlContext.KEY).get();
                                log.debug("[ControlServer] [{}，L:{}，R:{}] INACTIVE",
                                        context != null ? context.getControlId() : null,
                                        ctx.channel().localAddress(),
                                        ctx.channel().remoteAddress());
                                if (context != null) {
                                    controlServerListener.onClose(context);
                                    context.close();
                                }
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ControlContext context = ctx.channel().attr(ControlContext.KEY).get();
                                log.debug("[ControlServer] [{}，L:{}，R:{}] EXCEPTION",
                                        context != null ? context.getControlId() : null,
                                        ctx.channel().localAddress(),
                                        ctx.channel().remoteAddress(), cause);
                                if (context != null) controlServerListener.caughtException(context, cause);
                                else ctx.close();
                            }
                        });
                    }
                });

            log.debug("[ControlServer] Binding to port {}", opts.getPort());
            ChannelFuture bindFuture = bootstrap.bind(opts.getPort());
            bindFuture.addListener(f -> {
                if (f.isSuccess()) {
                    status = ServerStatus.RUNNING;
                }
            });
            return bindFuture;
        }

        return DefaultEventLoopGroup.failFuture(new Exception("Server is not in INIT state"));
    }

    @Override
    public Future<?> shutdown() {
        if (status == ServerStatus.RUNNING || status == ServerStatus.INIT) {
            status = ServerStatus.STOPPING;
            Promise<?> promise = DefaultEventLoopGroup.newPromise();
            Thread.ofVirtual().start(() -> {

                controlContextMap.forEach((id, context) -> {
                    try {
                        context.close().get();
                    } catch (Exception e) {
                        promise.setFailure(e);
                    }
                });

                promise.setSuccess(null);
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
            controlContextMap.forEach((id, context) -> {
                try {
                    context.close().get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            status = ServerStatus.SHUTDOWN;
        }
    }
    
    public ControlContext createControlContext(ProxyClientInfo proxyClientInfo, Channel channel) {
        ControlContext newControlContext = new ControlContext(proxyClientInfo, channel);
        decodeControlContext(newControlContext);
        controlContextMap.put(newControlContext.getControlId(), newControlContext);
        return newControlContext;
    }
    
    public void decodeControlContext(ControlContext controlContext) {
        controlContext.setCloseHook(ctx -> {
            controlContextMap.remove(controlContext.getControlId());
        });
    }
}
