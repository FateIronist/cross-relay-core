package top.fateironist.cross_relay_core.control;

import com.fasterxml.jackson.core.type.TypeReference;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;
import lombok.extern.slf4j.Slf4j;
import top.fateironist.cross_relay_core.Client;
import top.fateironist.cross_relay_core.control.handler.EventEncryptHandler;
import top.fateironist.cross_relay_core.control.handler.IdleEventHandler;
import top.fateironist.cross_relay_core.control.handler.JsonDecoder;
import top.fateironist.cross_relay_core.control.handler.JsonEncoder;
import top.fateironist.cross_relay_core.control.listener.ControlClientListener;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.control.ControlEvent;
import top.fateironist.cross_relay_core.model.control.ControlProtocolEventEnum;
import top.fateironist.cross_relay_core.model.info.ProxyClientInfo;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;
import top.fateironist.cross_relay_core.model.args.control.ControlClientConnectAbstractArgs;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.util.EncryptUtil;
import top.fateironist.cross_relay_core.util.JsonUtil;

import javax.crypto.SecretKey;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * 只负责构建加密通道和传递身份信息，其余如注册代理、业务事件发送由上层业务层在ControlClientListener通过ControlManager实现
 */
@Slf4j
public class ControlClient implements Client {
    private final Bootstrap bootstrap;
    private final EventLoopGroup workerGroup;

    private final ProxyClientInfo proxyClientInfo;
    private final ProxyServerInfo proxyServerInfo;

    private final ControlClientListener controlClientListener;

    public ControlClient(EventLoopGroup workerGroup, ProxyClientInfo proxyClientInfo, ProxyServerInfo proxyServerInfo, ControlClientListener controlClientListener) {
        this.workerGroup = workerGroup;
        this.proxyClientInfo = proxyClientInfo;
        this.proxyServerInfo = proxyServerInfo;
        this.controlClientListener = controlClientListener;
        this.bootstrap = new Bootstrap();
    }

    @Override
    public Future<Void> connect(AbstractArgs arg) {
        ControlClientConnectAbstractArgs args = (ControlClientConnectAbstractArgs) arg;
        var opts = args.getOptions();

        if (proxyServerInfo.getAddress() == null || proxyServerInfo.getAddress().getServerControlAddress() == null) {

            if (proxyServerInfo.getAddress().getServerInfoServerAddress() != null) {
                proxyServerInfo.getMetaDataFromServerInfoServer(3, 1000);
            }

            if (!proxyServerInfo.getAddress().isSufficient()) {
                Promise<Void> promise = workerGroup.next().newPromise();
                promise.setFailure(new Exception("Proxy server control address is null"));
                return promise;
            }

        }

        bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new IdleStateHandler(opts.getPingTimeout(), 0, 0, TimeUnit.MILLISECONDS));
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
                                log.debug("[ControlClient] [{}，L:{}，R:{}] RECEIVED data", context.getControlChannelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                if (!context.isEncrypted()) {
                                    // 2.收到对称密钥
                                    if (ControlProtocolEventEnum.SESSION_SECRET_KEY.equals(event.getType())) {
                                        log.debug("[ControlClient] [{}，L:{}，R:{}] SECRET-KEY-RECEIVED", context.getControlChannelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                        PrivateKey privateKey = ctx.channel().attr(EventEncryptHandler.SESSION_PRIVATE_KEY).get();

                                        String secretKeyStr = (String) event.getBody().get("secretKey");
                                        secretKeyStr = EncryptUtil.rsaDecrypt(secretKeyStr, privateKey);
                                        SecretKey secretKey = EncryptUtil.stringToAESKey(secretKeyStr);


                                        ctx.channel().attr(EventEncryptHandler.SESSION_SECRET_KEY).set(secretKey);
                                        ctx.channel().attr(EventEncryptHandler.SESSION_PRIVATE_KEY).set(null);

                                        context.setEncrypted(true);
                                        log.debug("[ControlClient] [{}，L:{}，R:{}] ENCRYPTED", context.getControlChannelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                        // 3.返回ACK附带客户端信息
                                        ControlEvent<ProxyClientInfo> response = new ControlEvent<>(ControlProtocolEventEnum.SESSION_SECRET_ACK.getType(), proxyClientInfo);
                                        context.writeAndFlush(response);
                                        log.debug("[ControlClient] [{}，L:{}，R:{}] SESSION-ACK-SENT", context.getControlChannelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                        // 定时Ping
                                        ScheduledFuture<?> scheduledFuture = workerGroup.scheduleAtFixedRate(() -> {
                                            if (context.isPermit()) {
                                                log.debug("[ControlClient] [{}，L:{}，R:{}] PING-SENT", context.getControlChannelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                                ctx.channel().writeAndFlush(new ControlEvent<>(ControlProtocolEventEnum.PING.getType(), Map.of("pingTime", System.currentTimeMillis())));
                                            }
                                        }, opts.getPingInterval(), opts.getPingInterval(), TimeUnit.MILLISECONDS);
                                        context.setPingScheduler(scheduledFuture);

                                        return;
                                    }

                                    // not encrypted yet
                                    log.debug("[ControlClient] [{}，L:{}，R:{}] NO ENCRYPTED YET", context.getControlChannelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                    context.handleAbnormalEvent(event);

                                    return;
                                }

                                if (!context.isPermit()) {
                                    // 4.代理请求被允许
                                    if (ControlProtocolEventEnum.CONNECTION_PERMIT.equals(event.getType())) {
                                        context.setPermit(true);

                                        String controlChannelId = (String) event.getBody().get("controlChannelId");
                                        ProxyServerInfo proxyServerInfo = JsonUtil.OBJECT_MAPPER.convertValue(event.getBody().get("proxyServerInfo"), ProxyServerInfo.class);

                                        context.setControlChannelId(controlChannelId);
                                        context.getProxyServerInfo().setAdditional(proxyServerInfo);

                                        controlClientListener.afterPermit(context);
                                        log.debug("[ControlClient] [{}，L:{}，R:{}] PERMITTED", context.getControlChannelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                        log.debug("[ControlClient] [{}，L:{}，R:{}] HANDSHAKE-COMPLETE", context.getControlChannelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                        return;
                                    }

                                    controlClientListener.onDeny(context);
                                    context.close();
                                    log.debug("[ControlClient] [{}，L:{}，R:{}] DENIED", context.getControlChannelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                    return;
                                }

                                if (ControlProtocolEventEnum.PONG.equals(event.getType())) {
                                    ProxyServerInfo proxyServerInfo = context.getProxyServerInfo();
                                    long lastPingTime = (long) event.getBody().get("pingTime");
                                    proxyServerInfo.receivePong(lastPingTime);
                                    log.debug("[ControlClient] [{}，L:{}，R:{}] PONG-RECEIVED", context.getControlChannelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                }else {
                                    log.debug("[ControlClient] [{}，L:{}，R:{}] DELEGATE-ON-MESSAGE", context.getControlChannelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                    controlClientListener.onEvent(context, event);
                                }
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                ControlContext context = new ControlContext(null, new ProxyServerInfo(), ctx.channel());
                                ctx.channel().attr(ControlContext.KEY).set(context);
                                log.debug("[ControlClient] [{}，L:{}，R:{}] ACTIVE", null, ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                // 1.发送公钥
                                KeyPair keyPair = EncryptUtil.generateRSAKeyPair();
                                ctx.channel().attr(EventEncryptHandler.SESSION_PRIVATE_KEY).set(keyPair.getPrivate());
                                String publicKey = EncryptUtil.publicKeyToBase64(keyPair.getPublic());

                                ControlEvent<Map<String, Object>> event = new ControlEvent<>(ControlProtocolEventEnum.SESSION_PUBLIC_KEY.getType(), Map.of("publicKey", publicKey));

                                context.writeAndFlush(event);
                                log.debug("[ControlClient] [{}，L:{}，R:{}] PUBLIC-KEY-SENT", context.getControlChannelId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                ControlContext context = ctx.channel().attr(ControlContext.KEY).get();
                                log.debug("[ControlClient] [{}，L:{}，R:{}] INACTIVE",
                                        context != null ? context.getControlChannelId() : null,
                                        ctx.channel().localAddress(),
                                        ctx.channel().remoteAddress());
                                if (context != null) {
                                    controlClientListener.onClose(context);
                                }
                                close();
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ControlContext context = ctx.channel().attr(ControlContext.KEY).get();
                                log.debug("[ControlClient] [{}，L:{}，R:{}] EXCEPTION",
                                        context != null ? context.getControlChannelId() : null,
                                        ctx.channel().localAddress(),
                                        ctx.channel().remoteAddress(), cause);
                                if (context != null) controlClientListener.caughtException(context, cause);
                                else ctx.close();
                            }
                        });
                    }
                });

        log.debug("[ControlClient] Connecting to {}", proxyServerInfo.getAddress().getServerControlAddress());
        return bootstrap.connect(proxyServerInfo.getAddress().getServerControlAddress());
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
