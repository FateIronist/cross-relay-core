package top.fateironist.cross_relay_core.control;

import com.fasterxml.jackson.core.type.TypeReference;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import top.fateironist.constack.Container;
import top.fateironist.constack.Promise;
import top.fateironist.cross_relay_core.Client;
import top.fateironist.cross_relay_core.FutureBridge;
import top.fateironist.cross_relay_core.control.handler.EventEncryptHandler;
import top.fateironist.cross_relay_core.control.handler.IdleEventHandler;
import top.fateironist.cross_relay_core.control.handler.JsonDecoder;
import top.fateironist.cross_relay_core.control.handler.JsonEncoder;
import top.fateironist.cross_relay_core.control.listener.ControlClientListener;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.control.ControlClientConnectAbstractArgs;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.control.ControlProtocolEventEnum;
import top.fateironist.cross_relay_core.model.control.event.ControlEvent;
import top.fateironist.cross_relay_core.model.info.ProxyClientInfo;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;
import top.fateironist.cross_relay_core.util.EncryptUtil;
import top.fateironist.cross_relay_core.util.JsonUtil;

import javax.crypto.SecretKey;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * 控制客户端：顶层容器，load = TCP 连接建立即激活；
 * 加密握手（收到 CONNECTION_PERMIT）由 ControlContext 子容器生命周期承载，connect Future 以 permit 为完成条件。
 * Listener 为纯通知钩子，不参与状态机与资源回收。
 */
@Slf4j
public class ControlClient extends Container implements Client {
    private final EventLoopGroup workerGroup;

    // 这里的依赖是用来发送身份认证信息
    private final ProxyClientInfo proxyClientInfo;

    private final ControlClientListener controlClientListener;

    /** 当前控制连接子容器；数据面经其沿父子链向下传递身份与连接信息。
     *  volatile 仅保证引用发布的可见性，ControlContext 内部状态由 Netty 事件循环串行访问 */
    @Getter
    @SuppressWarnings("java:S3077")
    private volatile ControlContext controlContext;

    /** TCP 连接建立信号：load 的 start Future，连接成功即完成，顶层容器随之激活并补全排队中的子容器请求 */
    private volatile Promise<Object> establishedPromise;

    /** 握手完成信号：收到 CONNECTION_PERMIT 才完成，经 connect() 桥接给调用方 */
    private volatile Promise<Object> connectPromise;

    public ControlClient(EventLoopGroup workerGroup, ProxyClientInfo proxyClientInfo, ControlClientListener controlClientListener) {
        super();
        this.workerGroup = workerGroup;
        this.proxyClientInfo = proxyClientInfo;
        this.controlClientListener = controlClientListener;
    }

    @Override
    public Future<Void> connect(AbstractArgs arg) {
        java.util.concurrent.Future<Object> established = load(arg);
        // 建连失败时握手信号一并失败，避免调用方永久等待
        Thread.startVirtualThread(() -> {
            try {
                established.get();
            } catch (Exception e) {
                Promise<Object> pending = connectPromise;
                if (pending != null) {
                    pending.setFailure(e);
                }
            }
        });
        return FutureBridge.bridge(connectPromise, null);
    }

    @Override
    protected java.util.concurrent.Future<Object> start(Object... args) {
        ControlClientConnectAbstractArgs connectArgs = (ControlClientConnectAbstractArgs) args[0];
        var opts = connectArgs.getOptions();

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new IdleStateHandler(opts.getPingTimeout(), 0, 0, TimeUnit.MILLISECONDS));
                        pipeline.addLast(new IdleEventHandler(context -> {
                            // ping 超时：直接销毁容器并回收资源；Listener 仅作通知，不承担卸载逻辑
                            if (context != null) {
                                context.disposal();
                            }
                            controlClientListener.onTimeOut(context);
                        }));
                        pipeline.addLast(new LengthFieldBasedFrameDecoder(65535, 0, 4, 0, 4));
                        pipeline.addLast(new LengthFieldPrepender(4));
                        pipeline.addLast(new EventEncryptHandler());
                        pipeline.addLast(new JsonEncoder());
                        pipeline.addLast(new JsonDecoder<>(new TypeReference<ControlEvent<Map<String, Object>>>() {}));
                        pipeline.addLast(new SimpleChannelInboundHandler<ControlEvent<Map<String, Object>>>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ControlEvent<Map<String, Object>> event) {
                                ControlContext context = controlContext;
                                log.debug("[ControlClient] [{}，L:{}，R:{}] RECEIVED data", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                if (!context.isEncrypted()) {
                                    // 2.收到对称密钥
                                    if (ControlProtocolEventEnum.SESSION_SECRET_KEY.matches(event.getType())) {
                                        log.debug("[ControlClient] [{}，L:{}，R:{}] SECRET-KEY-RECEIVED", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                        PrivateKey privateKey = ctx.channel().attr(EventEncryptHandler.SESSION_PRIVATE_KEY).get();

                                        String secretKeyStr = (String) event.getBody().get("secretKey");
                                        secretKeyStr = EncryptUtil.rsaDecrypt(secretKeyStr, privateKey);
                                        SecretKey secretKey = EncryptUtil.stringToAESKey(secretKeyStr);

                                        ctx.channel().attr(EventEncryptHandler.SESSION_SECRET_KEY).set(secretKey);
                                        ctx.channel().attr(EventEncryptHandler.SESSION_PRIVATE_KEY).set(null);

                                        context.setEncrypted(true);
                                        log.debug("[ControlClient] [{}，L:{}，R:{}] ENCRYPTED", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                        // 3.返回ACK附带客户端信息
                                        ControlEvent<ProxyClientInfo> response = new ControlEvent<>(ControlProtocolEventEnum.SESSION_SECRET_ACK.getType(), proxyClientInfo);
                                        context.writeAndFlush(response);
                                        log.debug("[ControlClient] [{}，L:{}，R:{}] SESSION-ACK-SENT", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                        // 定时心跳：任务句柄归 ControlContext 所有，销毁时取消
                                        context.schedulePing(opts.getPingInterval());

                                        return;
                                    }

                                    // not encrypted yet
                                    log.debug("[ControlClient] [{}，L:{}，R:{}] NO ENCRYPTED YET", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                    context.handleAbnormalEvent(event);

                                    return;
                                }

                                if (!context.isPermit()) {
                                    // 4.代理请求被允许
                                    if (ControlProtocolEventEnum.CONNECTION_PERMIT.matches(event.getType())) {
                                        context.setPermit(true);

                                        String controlId = (String) event.getBody().get("controlId");
                                        ProxyServerInfo serverInfo = JsonUtil.OBJECT_MAPPER.convertValue(event.getBody().get("proxyServerInfo"), ProxyServerInfo.class);

                                        context.setControlId(controlId);
                                        context.getProxyServerInfo().setAdditional(serverInfo);

                                        // 握手完成：ControlContext 与顶层容器同时激活
                                        context.completeHandshake();
                                        connectPromise.setSuccess(null);

                                        controlClientListener.afterPermit(context);
                                        log.debug("[ControlClient] [{}，L:{}，R:{}] PERMITTED", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                        log.debug("[ControlClient] [{}，L:{}，R:{}] HANDSHAKE-COMPLETE", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                        return;
                                    }

                                    controlClientListener.onDeny(context);
                                    SecurityException denied = new SecurityException("Control Channel Not Permitted");
                                    context.failHandshake(denied);
                                    connectPromise.setFailure(denied);
                                    context.close();
                                    log.debug("[ControlClient] [{}，L:{}，R:{}] DENIED", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                    return;
                                }

                                if (ControlProtocolEventEnum.PONG.matches(event.getType())) {
                                    ProxyServerInfo proxyServerInfo = context.getProxyServerInfo();
                                    long lastPingTime = (long) event.getBody().get("pingTime");
                                    proxyServerInfo.receivePong(lastPingTime);
                                    log.debug("[ControlClient] [{}，L:{}，R:{}] PONG-RECEIVED", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                } else {
                                    log.debug("[ControlClient] [{}，L:{}，R:{}] DELEGATE-ON-MESSAGE", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                    controlClientListener.onEvent(context, event);
                                }
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                // 创建控制连接子容器并启动握手生命周期：
                                // 顶层容器尚在 LOADING 时 child() 请求排队，待 establishedPromise 完成（顶层激活）后补全
                                Thread.startVirtualThread(() -> {
                                    try {
                                        ControlContext context = createControlContext(null, new ProxyServerInfo(), ctx.channel());
                                        controlContext = context;
                                        ctx.channel().attr(ControlContext.KEY).set(context);
                                        context.load();
                                        log.debug("[ControlClient] [{}，L:{}，R:{}] ACTIVE", null, ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                        // 1.发送公钥
                                        KeyPair keyPair = EncryptUtil.generateRSAKeyPair();
                                        ctx.channel().attr(EventEncryptHandler.SESSION_PRIVATE_KEY).set(keyPair.getPrivate());
                                        String publicKey = EncryptUtil.publicKeyToBase64(keyPair.getPublic());

                                        ControlEvent<Map<String, Object>> event = new ControlEvent<>(ControlProtocolEventEnum.SESSION_PUBLIC_KEY.getType(), Map.of("publicKey", publicKey));

                                        context.writeAndFlush(event);
                                        log.debug("[ControlClient] [{}，L:{}，R:{}] PUBLIC-KEY-SENT", controlContext.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                    } catch (Exception e) {
                                        connectPromise.setFailure(e);
                                    }
                                });
                                // TCP 连接建立：顶层容器激活，排队中的子容器请求随之补全
                                establishedPromise.setSuccess(null);
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                ControlContext context = controlContext;
                                log.debug("[ControlClient] [{}，L:{}，R:{}] INACTIVE",
                                        context != null ? context.getControlId() : null,
                                        ctx.channel().localAddress(),
                                        ctx.channel().remoteAddress());
                                if (context != null) {
                                    controlClientListener.onClose(context);
                                }
                                // 握手未完成即断开：connect 以失败完成，避免调用方永久等待
                                Promise<Object> pending = connectPromise;
                                if (pending != null) {
                                    pending.setFailure(new IllegalStateException("control channel inactive before handshake complete"));
                                }
                                // 通道已断开，销毁顶层容器并级联回收控制连接
                                disposal();
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ControlContext context = controlContext;
                                log.debug("[ControlClient] [{}，L:{}，R:{}] EXCEPTION",
                                        context != null ? context.getControlId() : null,
                                        ctx.channel().localAddress(),
                                        ctx.channel().remoteAddress(), cause);
                                if (context != null) controlClientListener.caughtException(context, cause);
                                else ctx.close();
                            }
                        });
                    }
                });

        log.debug("[ControlClient] Connecting to {}", connectArgs.getServerControlAddress());
        establishedPromise = new Promise<>();
        connectPromise = new Promise<>();
        Promise<Object> established = establishedPromise;
        Promise<Object> handshake = connectPromise;
        bootstrap.connect(connectArgs.getServerControlAddress()).addListener(f -> {
            if (!f.isSuccess()) {
                established.setFailure(f.cause());
                handshake.setFailure(f.cause());
            }
            // 连接建立由 channelActive 完成 establishedPromise；握手（CONNECTION_PERMIT）完成 connectPromise
        });
        return establishedPromise;
    }

    @Override
    public Future<?> close() {
        ControlContext context = controlContext;
        if (context != null) {
            return context.close();
        }
        return disposal();
    }

    @Override
    public void closeNow() {
        try {
            close().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (java.util.concurrent.ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 创建控制连接子容器，容器销毁时回收动作由容器自身 Effect 完成。
     */
    private ControlContext createControlContext(String controlId, ProxyServerInfo proxyServerInfo, Channel channel) {
        try {
            return (ControlContext) child(controlId, proxyServerInfo, channel).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Container createChild(Container parent, Object... args) {
        String controlId = (String) args[0];
        ProxyServerInfo proxyServerInfo = (ProxyServerInfo) args[1];
        Channel channel = (Channel) args[2];
        return new ControlContext(parent, controlId, null, proxyServerInfo, channel);
    }
}
