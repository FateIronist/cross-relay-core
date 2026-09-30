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
import lombok.extern.slf4j.Slf4j;
import top.fateironist.cross_relay_core.Client;
import top.fateironist.cross_relay_core.ClientStatus;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * control 通道客户端：只负责加密通道构建、身份信息传递与控制事件（REQUIRE_CHANNEL / TUNNEL_CLOSE / PROXY_CLOSE 等）的收发。
 * 一切核心功能留在核心库、绝不委托给上层：通道建立编排在 ProxyContext / TunnelContext 体系内闭环；
 * listener 仅承担通知与业务 event 处理两种角色，不承担核心逻辑
 */
@Slf4j
public class ControlClient implements Client {
    // 全局共享的 worker 线程组，既承载 channel 也用于调度 ping 定时任务
    private final EventLoopGroup workerGroup;

    // 生命周期状态：INIT -> OPEN -> CLOSING -> CLOSED
    public volatile ClientStatus status = ClientStatus.INIT;

    // 这里的依赖是用来发送身份认证信息
    private ProxyClientInfo proxyClientInfo;

    // 生命周期钩子
    private final ControlClientListener controlClientListener;
    // 当前唯一控制连接的会话上下文，channelActive 时创建
    private ControlContext controlContext;

    public ControlClient(EventLoopGroup workerGroup, ProxyClientInfo proxyClientInfo, ControlClientListener controlClientListener) {
        this.workerGroup = workerGroup;
        this.proxyClientInfo = proxyClientInfo;
        this.controlClientListener = controlClientListener;
    }

    /**
     * 装配 pipeline 并发起对服务端控制端口的 TCP 连接，仅在 INIT 状态下有效（重复调用返回空 Future）
     * pipeline 依次为：读空闲检测 -> 长度域编解码 -> 加解密 -> JSON 编解码 -> 握手状态机；握手四阶段（公钥上行、AES 密钥接收、ACK 附身份、permit）全部由该状态机驱动
     * 连接成功后状态置为 OPEN，握手完成点则是收到 CONNECTION_PERMIT 时调用的 afterPermit 钩子
     */
    @Override
    public Future<Void> connect(AbstractArgs arg) {
        if (status == ClientStatus.INIT) {
            ControlClientConnectAbstractArgs args = (ControlClientConnectAbstractArgs) arg;
            var opts = args.getOptions();

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
                            pipeline.addLast(new IdleEventHandler(controlClientListener::onTimeOut));
                            pipeline.addLast(new LengthFieldBasedFrameDecoder(65535, 0, 4, 0, 4));
                            pipeline.addLast(new LengthFieldPrepender(4));
                            pipeline.addLast(new EventEncryptHandler());
                            pipeline.addLast(new JsonEncoder());
                            pipeline.addLast(new JsonDecoder<>(new TypeReference<ControlEvent<Map<String, Object>>>() {}));
                            pipeline.addLast(new SimpleChannelInboundHandler<ControlEvent<Map<String, Object>>>() {
                                /**
                                 * 客户端侧的握手状态机，按 encrypted/permit 两个状态位分三段处理
                                 * encrypted=false：只认 SESSION_SECRET_KEY（解出会话密钥、置加密、回 ACK 附身份、启动 ping 定时器），其余事件计入异常容忍
                                 * encrypted=true 且 permit=false：只认 CONNECTION_PERMIT（回填 controlId 与服务端信息、置 permit、回调 afterPermit），其余事件走 onDeny 并关闭连接
                                 * permit=true：PONG 用于计算 RTT，其余事件交给业务层的 onEvent
                                 */
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, ControlEvent<Map<String, Object>> event) {
                                    ControlContext context = controlContext;
                                    log.debug("[ControlClient] [{}，L:{}，R:{}] RECEIVED data", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                    if (!context.isEncrypted()) {
                                        // 2.收到对称密钥
                                        if (ControlProtocolEventEnum.SESSION_SECRET_KEY.equals(event.getType())) {
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

                                            // 定时Ping：定时器随加密完成即启动，但只在 permit 后才真正发包，避免握手未完成就产生心跳
                                            ScheduledFuture<?> scheduledFuture = workerGroup.scheduleAtFixedRate(() -> {
                                                if (context.isPermit()) {
                                                    log.debug("[ControlClient] [{}，L:{}，R:{}] PING-SENT", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                                    ctx.channel().writeAndFlush(new ControlEvent<>(ControlProtocolEventEnum.PING.getType(), Map.of("pingTime", System.currentTimeMillis())));
                                                }
                                            }, opts.getPingInterval(), opts.getPingInterval(), TimeUnit.MILLISECONDS);
                                            context.setPingScheduler(scheduledFuture);

                                            return;
                                        }

                                        // not encrypted yet
                                        log.debug("[ControlClient] [{}，L:{}，R:{}] NO ENCRYPTED YET", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                        context.handleAbnormalEvent(event);

                                        return;
                                    }

                                    if (!context.isPermit()) {
                                        // 4.代理请求被允许
                                        if (ControlProtocolEventEnum.CONNECTION_PERMIT.equals(event.getType())) {
                                            context.setPermit(true);

                                            String controlId = (String) event.getBody().get("controlId");
                                            ProxyServerInfo proxyServerInfo = JsonUtil.OBJECT_MAPPER.convertValue(event.getBody().get("proxyServerInfo"), ProxyServerInfo.class);

                                            context.setControlId(controlId);
                                            context.getProxyServerInfo().setAdditional(proxyServerInfo);

                                            controlClientListener.afterPermit(context);
                                            log.debug("[ControlClient] [{}，L:{}，R:{}] PERMITTED", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                            log.debug("[ControlClient] [{}，L:{}，R:{}] HANDSHAKE-COMPLETE", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                            return;
                                        }

                                        controlClientListener.onDeny(context);
                                        context.close();
                                        log.debug("[ControlClient] [{}，L:{}，R:{}] DENIED", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());

                                        return;
                                    }

                                    // 5.心跳应答：pingTime 由服务端原样回显，据此统计 RTT；其余事件均为业务事件
                                    if (ControlProtocolEventEnum.PONG.equals(event.getType())) {
                                        ProxyServerInfo proxyServerInfo = context.getProxyServerInfo();
                                        long lastPingTime = (long) event.getBody().get("pingTime");
                                        proxyServerInfo.receivePong(lastPingTime);
                                        log.debug("[ControlClient] [{}，L:{}，R:{}] PONG-RECEIVED", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                    }else {
                                        log.debug("[ControlClient] [{}，L:{}，R:{}] DELEGATE-ON-MESSAGE", context.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                        controlClientListener.onEvent(context, event);
                                    }
                                }

                                /**
                                 * 连接建立：创建会话上下文并挂到 channel attr（此时 controlId 仍为 null，由服务端在 permit 时回填），随后发起握手第 1 步——生成 RSA 密钥对上行公钥
                                 * 私钥只留在本端 channel attr，收到会话密钥后立刻清空
                                 */
                                @Override
                                public void channelActive(ChannelHandlerContext ctx) {
                                    controlContext = createControlContext(null, new ProxyServerInfo(), ctx.channel());
                                    ctx.channel().attr(ControlContext.KEY).set(controlContext);
                                    log.debug("[ControlClient] [{}，L:{}，R:{}] ACTIVE", null, ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                    // 1.发送公钥
                                    KeyPair keyPair = EncryptUtil.generateRSAKeyPair();
                                    ctx.channel().attr(EventEncryptHandler.SESSION_PRIVATE_KEY).set(keyPair.getPrivate());
                                    String publicKey = EncryptUtil.publicKeyToBase64(keyPair.getPublic());

                                    ControlEvent<Map<String, Object>> event = new ControlEvent<>(ControlProtocolEventEnum.SESSION_PUBLIC_KEY.getType(), Map.of("publicKey", publicKey));

                                    controlContext.writeAndFlush(event);
                                    log.debug("[ControlClient] [{}，L:{}，R:{}] PUBLIC-KEY-SENT", controlContext.getControlId(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
                                }

                                /**
                                 * 连接断开（无论是本端关闭还是对端断开）：先回调 onClose 钩子，再关闭本端上下文
                                 * context 可能尚未创建（连接建立前即失败），此时跳过钩子
                                 */
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
                                    close();
                                }

                                /**
                                 * 通道异常：有会话上下文时交给业务层钩子决定处理方式，否则直接关闭 channel
                                 */
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

            log.debug("[ControlClient] Connecting to {}", args.getServerControlAddress());
            return bootstrap.connect(args.getServerControlAddress()).addListener(f -> {
                if (f.isSuccess()) {
                    status = ClientStatus.OPEN;
                }
            });
        }

        return DefaultEventLoopGroup.emptyFuture(null);
    }

    /** 异步优雅关闭：置 CLOSING 后关闭会话上下文（内部会先通知对端 CLOSE 再关 channel），关闭完成后状态落到 CLOSED；非 OPEN/INIT 状态直接返回空 Future */
    @Override
    public Future<?> close() {
        if (status == ClientStatus.OPEN || status == ClientStatus.INIT) {
            status = ClientStatus.CLOSING;
            return controlContext.close().addListener(f -> status = ClientStatus.CLOSED);
        }
        return DefaultEventLoopGroup.emptyFuture();
    }

    /** 同步关闭，阻塞至 channel 关闭完成，中断等异常包装为 RuntimeException 抛出 */
    @Override
    public void closeNow() {
        if (status == ClientStatus.OPEN || status == ClientStatus.INIT) {
            status = ClientStatus.CLOSING;
            try {
                controlContext.close().get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            status = ClientStatus.CLOSED;
        }
    }


    /** 创建控制会话上下文，并在返回前经 decorateControlContext 留出子类定制入口 */
    public ControlContext createControlContext(String controlId, ProxyServerInfo proxyServerInfo, Channel channel) {
        ControlContext newControlContext = new ControlContext(controlId, proxyServerInfo, channel);
        decorateControlContext(newControlContext);
        return newControlContext;
    }

    /** 空实现的扩展点：子类可在此为新建的 ControlContext 附加自定义信息，默认不修改任何内容 */
    public void decorateControlContext(ControlContext controlContext) {
    }
}
