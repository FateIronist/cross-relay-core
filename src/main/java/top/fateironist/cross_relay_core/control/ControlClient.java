package top.fateironist.cross_relay_core.control;

import com.fasterxml.jackson.core.type.TypeReference;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.ScheduledFuture;
import top.fateironist.cross_relay_core.Client;
import top.fateironist.cross_relay_core.control.handler.EventEncryptHandler;
import top.fateironist.cross_relay_core.control.handler.IdleEventHandler;
import top.fateironist.cross_relay_core.control.handler.JsonDecoder;
import top.fateironist.cross_relay_core.control.handler.JsonEncoder;
import top.fateironist.cross_relay_core.control.listener.ControlClientListener;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.control.ControlEvent;
import top.fateironist.cross_relay_core.model.control.ControlEventEnum;
import top.fateironist.cross_relay_core.model.info.ProxyClientInfo;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;
import top.fateironist.cross_relay_core.model.options.ControlClientConnectOptions;
import top.fateironist.cross_relay_core.util.EncryptUtil;
import top.fateironist.cross_relay_core.util.JsonUtil;

import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import javax.crypto.SecretKey;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ControlClient implements Client {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ControlClient.class);

    private final Bootstrap bootstrap;
    private final EventLoopGroup workerGroup;

    private final ProxyClientInfo proxyClientInfo;

    private final ControlClientListener controlClientListener;

    public ControlClient(EventLoopGroup workerGroup, ProxyClientInfo proxyClientInfo, ControlClientListener controlClientListener) {
        this.workerGroup = workerGroup;
        this.proxyClientInfo = proxyClientInfo;
        this.controlClientListener = controlClientListener;
        this.bootstrap = new Bootstrap();
    }

    public Future<Void> connect(ControlClientConnectOptions options) {
        bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new IdleStateHandler(options.pingTimeout, 0, 0, TimeUnit.MILLISECONDS));
                        pipeline.addLast(new IdleEventHandler(controlClientListener::onTimeOut));
                        pipeline.addLast(new LengthFieldBasedFrameDecoder(65535, 0, 4, 0, 4));
                        pipeline.addLast(new LengthFieldPrepender(4));
                        pipeline.addLast(new EventEncryptHandler());
                        pipeline.addLast(new JsonEncoder());
                        pipeline.addLast(new JsonDecoder<>(new TypeReference<ControlEvent<Map<String, Object>>>() {}));
                        pipeline.addLast(new SimpleChannelInboundHandler<ControlEvent<Map<String, Object>>>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ControlEvent<Map<String, Object>> event) {
                                ControlContext context = ctx.channel().attr(ControlContext.KEY).get();
                                String remoteAddress = String.valueOf(ctx.channel().remoteAddress());
                                logger.debug("[ControlClient] [{}({})] RECEIVED event: type={}", context.getControlChannelId(), remoteAddress, event.getType());

                                if (!context.isEncrypted()) {
                                    // 2.收到对称密钥
                                    if (event.getType() == ControlEventEnum.SESSION_SECRET_KEY) {
                                        logger.debug("[ControlClient] [{}({})] HANDSHAKE step 2: received server AES key, switching to AES encryption", context.getControlChannelId(), remoteAddress);
                                        PrivateKey privateKey = ctx.channel().attr(EventEncryptHandler.SESSION_PRIVATE_KEY).get();

                                        String secretKeyStr = (String) event.getBody().get("secretKey");
                                        secretKeyStr = EncryptUtil.rsaDecrypt(secretKeyStr, privateKey);
                                        SecretKey secretKey = EncryptUtil.stringToAESKey(secretKeyStr);


                                        ctx.channel().attr(EventEncryptHandler.SESSION_SECRET_KEY).set(secretKey);
                                        ctx.channel().attr(EventEncryptHandler.SESSION_PRIVATE_KEY).set(null);

                                        context.setEncrypted(true);
                                        logger.debug("[ControlClient] [{}({})] HANDSHAKE complete: encryption established, AES key active", context.getControlChannelId(), remoteAddress);

                                        // 3.返回ACK附带客户端信息
                                        ControlEvent<ProxyClientInfo> response = new ControlEvent<>(ControlEventEnum.SESSION_SECRET_ACK, proxyClientInfo);
                                        context.writeAndFlush(response);
                                        logger.debug("[ControlClient] [{}({})] HANDSHAKE step 3: sent SESSION_SECRET_ACK with client proxy info", context.getControlChannelId(), remoteAddress);

                                        // 定时Ping
                                        ScheduledFuture<?> scheduledFuture = workerGroup.scheduleAtFixedRate(() -> {
                                            if (context.isPermit()) {
                                                logger.debug("[ControlClient] [{}({})] PING -> server", context.getControlChannelId(), remoteAddress);
                                                ctx.channel().writeAndFlush(new ControlEvent<>(ControlEventEnum.PING, Map.of("pingTime", System.currentTimeMillis())));
                                            }
                                        }, options.pingInterval, options.pingInterval, TimeUnit.MILLISECONDS);
                                        context.setPingScheduler(scheduledFuture);
                                    }

                                    return;
                                }

                                if (!context.isPermit()) {
                                    // 4.代理请求被允许
                                    if (event.getType() == ControlEventEnum.CONNECTION_PERMIT) {
                                        context.setPermit(true);

                                        String controlChannelId = (String) event.getBody().get("controlChannelId");
                                        ProxyServerInfo proxyServerInfo = JsonUtil.OBJECT_MAPPER.convertValue(event.getBody().get("proxyServerInfo"), ProxyServerInfo.class);

                                        context.setControlChannelId(controlChannelId);
                                        context.getProxyServerInfo().setAdditional(proxyServerInfo);

                                        controlClientListener.afterPermit(context);
                                        logger.debug("[ControlClient] [{}({})] PERMITTED: connection established", context.getControlChannelId(), remoteAddress);
                                    }else {
                                        controlClientListener.onDeny(context);
                                        context.close();
                                        logger.debug("[ControlClient] [{}({})] DENY: connection deny", context.getControlChannelId(), remoteAddress);
                                    }
                                    return;
                                }

                                if (event.getType() == ControlEventEnum.PONG) {
                                    ProxyServerInfo proxyServerInfo = context.getProxyServerInfo();
                                    long lastPingTime = (long) event.getBody().get("pingTime");
                                    proxyServerInfo.receivePong(lastPingTime);
                                    logger.debug("[ControlClient] [{}({})] PONG <- server, latency={}ms", context.getControlChannelId(), remoteAddress, proxyServerInfo.getLatency());
                                }else {
                                    logger.debug("[ControlClient] [{}({})] DELEGATE onMessage: type={}", context.getControlChannelId(), remoteAddress, event.getType());
                                    controlClientListener.onMessage(context, event);
                                }
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                logger.debug("[ControlClient] [{}({})] ACTIVE: connected, generating RSA key pair", (Object) null, ctx.channel().remoteAddress());
                                // 1.发送公钥
                                KeyPair keyPair = EncryptUtil.generateRSAKeyPair();
                                ctx.channel().attr(EventEncryptHandler.SESSION_PRIVATE_KEY).set(keyPair.getPrivate());
                                String publicKey = EncryptUtil.publicKeyToBase64(keyPair.getPublic());

                                ControlEvent<Map<String, Object>> event = new ControlEvent<>(ControlEventEnum.SESSION_PUBLIC_KEY, Map.of("publicKey", publicKey));

                                ControlContext context = new ControlContext(null, new ProxyServerInfo(), ctx.channel());
                                ctx.channel().attr(ControlContext.KEY).set(context);
                                context.writeAndFlush(event);
                                logger.debug("[ControlClient] [{}({})] HANDSHAKE step 1: sent SESSION_PUBLIC_KEY (RSA public key)", context.getControlChannelId(), ctx.channel().remoteAddress());
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                ControlContext context = ctx.channel().attr(ControlContext.KEY).get();
                                logger.debug("[ControlClient] [{}({})] INACTIVE: encrypted={}",
                                        context != null ? context.getControlChannelId() : null,
                                        ctx.channel().remoteAddress(),
                                        context != null ? context.isEncrypted() : "unknown");
                                if (context != null) controlClientListener.onClose(context);
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ControlContext context = ctx.channel().attr(ControlContext.KEY).get();
                                logger.debug("[ControlClient] [{}({})] EXCEPTION: {}",
                                        context != null ? context.getControlChannelId() : null,
                                        ctx.channel().remoteAddress(),
                                        cause.getMessage(), cause);
                                if (context != null) controlClientListener.caughtException(context, cause);
                                else ctx.close();
                            }
                        });
                    }
                });

        logger.debug("[ControlClient] Connecting to {}", options.address);
        return bootstrap.connect(options.address);
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
