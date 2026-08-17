package top.fateironist.cross_relay_core.control;

import com.fasterxml.jackson.core.type.TypeReference;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;
import top.fateironist.cross_relay_core.Server;
import top.fateironist.cross_relay_core.control.handler.EventEncryptHandler;
import top.fateironist.cross_relay_core.control.handler.IdleEventHandler;
import top.fateironist.cross_relay_core.control.handler.JsonDecoder;
import top.fateironist.cross_relay_core.control.handler.JsonEncoder;
import top.fateironist.cross_relay_core.control.listener.ControlServerListener;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.control.ControlEvent;
import top.fateironist.cross_relay_core.model.control.ControlProtocolEventEnum;
import top.fateironist.cross_relay_core.model.control.Error;
import top.fateironist.cross_relay_core.model.info.ProxyClientInfo;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;
import top.fateironist.cross_relay_core.model.options.control.ControlServerStartOptions;
import top.fateironist.cross_relay_core.model.options.Options;
import top.fateironist.cross_relay_core.util.EncryptUtil;
import top.fateironist.cross_relay_core.util.JsonUtil;

import javax.crypto.SecretKey;
import java.net.InetSocketAddress;
import java.security.PublicKey;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * 只负责构建加密通道和传递身份信息，其余如注册代理、业务事件发送由上层业务层在ControlClientListener通过ControlManager实现
 */
@Slf4j
public class ControlServer implements Server {
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

    @Override
    public ChannelFuture start(Options option) {
        ControlServerStartOptions options = (ControlServerStartOptions) option;
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
                                log.debug("[ControlServer] [{}({})] RECEIVED event: type={}", context.getControlChannelId(), ctx.channel().remoteAddress(), event.getType());

                                // 3.进行密钥互换
                                if (!context.isEncrypted()) {
                                    if (ControlProtocolEventEnum.SESSION_PUBLIC_KEY.equals(event.getType())) {
                                        log.debug("[ControlServer] [{}({})] HANDSHAKE step 1: received client RSA public key", context.getControlChannelId(), ctx.channel().remoteAddress());
                                        String pubKey = (String) event.getBody().get("publicKey");
                                        PublicKey publicKey = EncryptUtil.base64ToPublicKey(pubKey);

                                        SecretKey secretKey = EncryptUtil.generateAESKey();
                                        String secretKeyStr = EncryptUtil.aesKeyToString(secretKey);
                                        secretKeyStr = EncryptUtil.rsaEncrypt(secretKeyStr, publicKey);

                                        ControlEvent<Map<String, Object>> handShake = new ControlEvent<>(ControlProtocolEventEnum.SESSION_SECRET_KEY.getType(), Map.of("secretKey", secretKeyStr));
                                        context.writeAndFlush(handShake);

                                        ctx.channel().attr(EventEncryptHandler.SESSION_PUBLIC_KEY).set(publicKey);
                                        ctx.channel().attr(EventEncryptHandler.SESSION_SECRET_KEY).set(secretKey);

                                        log.debug("[ControlServer] [{}({})] HANDSHAKE step 2: sent SESSION_SECRET_KEY (AES key encrypted with client RSA public key)", context.getControlChannelId(), ctx.channel().remoteAddress());
                                        return;
                                    }

                                    if (ControlProtocolEventEnum.SESSION_SECRET_ACK.equals(event.getType())) {
                                        ctx.channel().attr(EventEncryptHandler.SESSION_PUBLIC_KEY).set(null);

                                        // 补全客户端代理信息，尤其是credentials
                                        ProxyClientInfo proxyClientInfo = JsonUtil.OBJECT_MAPPER.convertValue(event.getBody(), ProxyClientInfo.class);
                                        context.getProxyClientInfo().setAdditional(proxyClientInfo);

                                        context.setEncrypted(true);

                                        // 4.进行权限验证，验证通过则允许正式连接
                                        if (controlServerListener.beforePermit(context, event)) {
                                            context.setPermit(true);
                                            log.debug("[ControlServer] [{}({})] PERMIT: connection permitted by listener", context.getControlChannelId(), ctx.channel().remoteAddress());
                                            context.writeAndFlush(new ControlEvent<>(ControlProtocolEventEnum.CONNECTION_PERMIT.getType(), Map.of("controlChannelId", context.getControlChannelId(), "proxyServerInfo", proxyServerInfo)));
                                        }else {
                                            log.debug("[ControlServer] [{}({})] DENIED: connection denied by listener, closing", context.getControlChannelId(), ctx.channel().remoteAddress());
                                            context.writeAndFlush(new ControlEvent<>(ControlProtocolEventEnum.ERROR.getType(), new Error("Control Channel Not Permitted!")));
                                            context.close();
                                        }

                                        log.debug("[ControlServer] [{}({})] HANDSHAKE step 3: encryption established, RSA public key removed, AES key active", context.getControlChannelId(), ctx.channel().remoteAddress());
                                        return;
                                    }

                                    log.debug("[ControlServer] [{}({})] REJECTED: channel not encrypted yet, event type={}", context.getControlChannelId(), ctx.channel().remoteAddress(), event.getType());
                                    context.writeAndFlush(new ControlEvent<>(ControlProtocolEventEnum.ERROR.getType(), new Error("Control Channel Not Encrypted Yet!")));
                                    context.handleAbnormalEvent(event);

                                    return;
                                }

                                if (!context.isPermit()) {
                                    log.debug("[ControlServer] [{}({})] DENIED: connection denied by listener, closing", context.getControlChannelId(), ctx.channel().remoteAddress());
                                    context.writeAndFlush(new ControlEvent<>(ControlProtocolEventEnum.ERROR.getType(), new Error("Control Channel Not Permitted!")));
                                    context.close();

                                    return;
                                }

                                if (ControlProtocolEventEnum.PING.equals(event.getType())) {
                                    log.debug("[ControlServer] [{}({})] PING -> PONG", context.getControlChannelId(), ctx.channel().remoteAddress());
                                    long pingTime = (long) event.getBody().get("pingTime");
                                    context.writeAndFlush(new ControlEvent<>(ControlProtocolEventEnum.PONG.getType(), Map.of("pingTime", pingTime)));
                                }else {
                                    // 5.进行消息处理
                                    log.debug("[ControlServer] [{}({})] DELEGATE onMessage: type={}", context.getControlChannelId(), ctx.channel().remoteAddress(), event.getType());
                                    controlServerListener.onEvent(context, event);
                                }
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                String id = UUID.randomUUID().toString();
                                ProxyClientInfo proxyClientInfo = new ProxyClientInfo((InetSocketAddress) ctx.channel().remoteAddress());

                                ControlContext context = new ControlContext(id, proxyClientInfo, ctx.channel());
                                ctx.channel().attr(ControlContext.KEY).set(context);

                                log.debug("[ControlServer] [{}({})] ACTIVE", id, ctx.channel().remoteAddress());
                                // 2.接收完成，分配id完成，但还未分发，id顺便在permit时分发
                                controlServerListener.afterAccept(context);
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                ControlContext context = ctx.channel().attr(ControlContext.KEY).get();
                                log.debug("[ControlServer] [{}({})] INACTIVE: encrypted={}, permit={}",
                                        context != null ? context.getControlChannelId() : null,
                                        ctx.channel().remoteAddress(),
                                        context != null ? context.isEncrypted() : "unknown",
                                        context != null ? context.isPermit() : "unknown");
                                if (context != null) controlServerListener.onClose(context);
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ControlContext context = ctx.channel().attr(ControlContext.KEY).get();
                                log.debug("[ControlServer] [{}({})] EXCEPTION: {}",
                                        context != null ? context.getControlChannelId() : null,
                                        ctx.channel().remoteAddress(),
                                        cause.getMessage(), cause);
                                if (context != null) controlServerListener.caughtException(context, cause);
                                else ctx.close();
                            }
                        });
                    }
                });

        log.debug("[ControlServer] Binding to port {}", options.port);
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
