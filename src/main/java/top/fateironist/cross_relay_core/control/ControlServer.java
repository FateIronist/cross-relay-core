package top.fateironist.cross_relay_core.control;

import com.fasterxml.jackson.core.type.TypeReference;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;
import top.fateironist.cross_relay_core.Server;
import top.fateironist.cross_relay_core.control.handler.EventEncryptHandler;
import top.fateironist.cross_relay_core.control.handler.IdleEventHandler;
import top.fateironist.cross_relay_core.control.handler.JsonDecoder;
import top.fateironist.cross_relay_core.control.handler.JsonEncoder;
import top.fateironist.cross_relay_core.control.listener.ControlServerListener;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.control.ControlEvent;
import top.fateironist.cross_relay_core.model.control.ControlEventEnum;
import top.fateironist.cross_relay_core.model.control.Error;
import top.fateironist.cross_relay_core.model.info.ProxyClientInfo;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;
import top.fateironist.cross_relay_core.model.options.ControlServerStartOptions;
import top.fateironist.cross_relay_core.util.EncryptUtil;
import top.fateironist.cross_relay_core.util.JsonUtil;

import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import javax.crypto.SecretKey;
import java.net.InetSocketAddress;
import java.security.PublicKey;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ControlServer implements Server {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ControlServer.class);

    private final ServerBootstrap bootstrap;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;

    private final ProxyServerInfo proxyServerInfo;
    // 生命周期钩子
    private final ControlServerListener controlServerListener;

    public ControlServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup, ProxyServerInfo proxyServerInfo, ControlServerListener controlServerListener) {
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.proxyServerInfo = proxyServerInfo;
        this.controlServerListener = controlServerListener;
        this.bootstrap = new ServerBootstrap();
    }

    public ChannelFuture start(ControlServerStartOptions options) {
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
                                if (controlServerListener.beforeConnect(childChannel)) {
                                    logger.debug("[ControlServer] ACCEPT {} -> {}", childChannel.remoteAddress(), childChannel.localAddress());
                                    ctx.fireChannelRead(msg);
                                } else {
                                    childChannel.unsafe().close(childChannel.voidPromise());
                                    logger.debug("[ControlServer] REJECT {} (beforeConnect returned false)", childChannel.remoteAddress());
                                }
                            }
                        });
                    }
                })
                .option(ChannelOption.SO_BACKLOG, options.maxConnections)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new IdleStateHandler(options.pingTimeout, 0, 0, TimeUnit.MILLISECONDS));
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
                                logger.debug("[ControlServer] [{}({})] RECEIVED event: type={}", context.getControlChannelId(), ctx.channel().remoteAddress(), event.getType());

                                // 3.进行密钥互换
                                if (!context.isEncrypted()) {
                                    if (event.getType() == ControlEventEnum.SESSION_PUBLIC_KEY) {
                                        logger.debug("[ControlServer] [{}({})] HANDSHAKE step 1: received client RSA public key", context.getControlChannelId(), ctx.channel().remoteAddress());
                                        String pubKey = (String) event.getBody().get("publicKey");
                                        PublicKey publicKey = EncryptUtil.base64ToPublicKey(pubKey);

                                        SecretKey secretKey = EncryptUtil.generateAESKey();
                                        String secretKeyStr = EncryptUtil.aesKeyToString(secretKey);
                                        secretKeyStr = EncryptUtil.rsaEncrypt(secretKeyStr, publicKey);

                                        ControlEvent<Map<String, Object>> handShake = new ControlEvent<>(ControlEventEnum.SESSION_SECRET_KEY, Map.of("secretKey", secretKeyStr));
                                        context.writeAndFlush(handShake);

                                        ctx.channel().attr(EventEncryptHandler.SESSION_PUBLIC_KEY).set(publicKey);
                                        ctx.channel().attr(EventEncryptHandler.SESSION_SECRET_KEY).set(secretKey);

                                        logger.debug("[ControlServer] [{}({})] HANDSHAKE step 2: sent SESSION_SECRET_KEY (AES key encrypted with client RSA public key)", context.getControlChannelId(), ctx.channel().remoteAddress());
                                    } else if (event.getType() == ControlEventEnum.SESSION_SECRET_ACK) {
                                        ctx.channel().attr(EventEncryptHandler.SESSION_PUBLIC_KEY).set(null);

                                        // 补全客户端代理信息，尤其是credentials
                                        ProxyClientInfo proxyClientInfo = JsonUtil.OBJECT_MAPPER.convertValue(event.getBody(), ProxyClientInfo.class);
                                        context.getProxyClientInfo().setAdditional(proxyClientInfo);

                                        context.setEncrypted(true);

                                        // 4.进行权限验证，验证通过则允许正式连接
                                        if (controlServerListener.beforePermit(context, event)) {
                                            context.setPermit(true);
                                            logger.debug("[ControlServer] [{}({})] PERMIT: connection permitted by listener", context.getControlChannelId(), ctx.channel().remoteAddress());
                                            context.writeAndFlush(new ControlEvent<>(ControlEventEnum.CONNECTION_PERMIT, Map.of("controlChannelId", context.getControlChannelId(), "proxyServerInfo", proxyServerInfo)));
                                        }else {
                                            logger.debug("[ControlServer] [{}({})] DENIED: connection denied by listener, closing", context.getControlChannelId(), ctx.channel().remoteAddress());
                                            context.writeAndFlush(new ControlEvent<>(ControlEventEnum.ERROR, new Error("Control Channel Not Permitted!")));
                                            context.close();
                                        }

                                        logger.debug("[ControlServer] [{}({})] HANDSHAKE step 3: encryption established, RSA public key removed, AES key active", context.getControlChannelId(), ctx.channel().remoteAddress());
                                    } else {
                                        logger.debug("[ControlServer] [{}({})] REJECTED: channel not encrypted yet, event type={}", context.getControlChannelId(), ctx.channel().remoteAddress(), event.getType());
                                        context.writeAndFlush(new ControlEvent<>(ControlEventEnum.ERROR, new Error("Control Channel Not Encrypted Yet!")));
                                    }

                                    return;
                                }

                                if (!context.isPermit()) {
                                    logger.debug("[ControlServer] [{}({})] DENIED: connection denied by listener, closing", context.getControlChannelId(), ctx.channel().remoteAddress());
                                    context.writeAndFlush(new ControlEvent<>(ControlEventEnum.ERROR, new Error("Control Channel Not Permitted!")));
                                    context.close();

                                    return;
                                }

                                if (event.getType() == ControlEventEnum.PING) {
                                    logger.debug("[ControlServer] [{}({})] PING -> PONG", context.getControlChannelId(), ctx.channel().remoteAddress());
                                    long pingTime = (long) event.getBody().get("pingTime");
                                    context.writeAndFlush(new ControlEvent<>(ControlEventEnum.PONG, Map.of("pingTime", pingTime)));
                                }else {
                                    // 5.进行消息处理
                                    logger.debug("[ControlServer] [{}({})] DELEGATE onMessage: type={}", context.getControlChannelId(), ctx.channel().remoteAddress(), event.getType());
                                    controlServerListener.onMessage(context, event);
                                }
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                String id = UUID.randomUUID().toString();
                                ProxyClientInfo proxyClientInfo = new ProxyClientInfo((InetSocketAddress) ctx.channel().remoteAddress());

                                ControlContext context = new ControlContext(id, proxyClientInfo, ctx.channel());
                                ctx.channel().attr(ControlContext.KEY).set(context);

                                logger.debug("[ControlServer] [{}({})] ACTIVE", id, ctx.channel().remoteAddress());
                                // 2.接收完成，分配id完成，但还未分发，id顺便在permit时分发
                                controlServerListener.afterAccept(context);
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                ControlContext context = ctx.channel().attr(ControlContext.KEY).get();
                                logger.debug("[ControlServer] [{}({})] INACTIVE: encrypted={}, permit={}",
                                        context != null ? context.getControlChannelId() : null,
                                        ctx.channel().remoteAddress(),
                                        context != null ? context.isEncrypted() : "unknown",
                                        context != null ? context.isPermit() : "unknown");
                                if (context != null) controlServerListener.onClose(context);
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ControlContext context = ctx.channel().attr(ControlContext.KEY).get();
                                logger.debug("[ControlServer] [{}({})] EXCEPTION: {}",
                                        context != null ? context.getControlChannelId() : null,
                                        ctx.channel().remoteAddress(),
                                        cause.getMessage(), cause);
                                if (context != null) controlServerListener.caughtException(context, cause);
                                else ctx.close();
                            }
                        });
                    }
                });

        logger.debug("[ControlServer] Binding to port {}", options.port);
        return bootstrap.bind(options.port);
    }

    @Override
    public Future<?> shutdown() {
        return bossGroup.shutdownGracefully().addListener(f -> workerGroup.shutdownGracefully());
    }

    @Override
    public void shutdownNow() {
        bossGroup.shutdownNow();
        workerGroup.shutdownNow();
    }
}
