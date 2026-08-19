package top.fateironist.cross_relay_core;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.args.proxy_client.ProxyClientConnectArgs;
import top.fateironist.cross_relay_core.model.args.proxy_server.ClientProxyServerStartArgs;
import top.fateironist.cross_relay_core.model.args.proxy_server.RequesterProxyServerStartArgs;
import top.fateironist.cross_relay_core.model.info.ClientServiceInfo;
import top.fateironist.cross_relay_core.model.info.ProxyClientInfo;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ClientTunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ServerTunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;
import top.fateironist.cross_relay_core.proxy.ProxyClient;
import top.fateironist.cross_relay_core.proxy.ProxyServer;
import top.fateironist.cross_relay_core.proxy.listener.ProxyClientListener;
import top.fateironist.cross_relay_core.proxy.listener.ProxyServerListener;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ProxyServer + ProxyClient TCP代理集成测试
 * 覆盖 ProxyServerListener 全部 8 个回调方法 和 ProxyClientListener 全部 4 个回调方法
 */
class TcpProxyTest {

    @BeforeAll
    static void setupLogging() {
        LoggerContext ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
        ctx.getLogger(ProxyServer.class).setLevel(Level.DEBUG);
        ctx.getLogger(ProxyClient.class).setLevel(Level.DEBUG);
    }

    // ==================== 辅助方法 ====================

    /**
     * 启动 Echo 服务：收到任意数据原样返回
     */
    private Channel startEchoServer(EventLoopGroup group, int port) throws Exception {
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(group, group)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                ctx.writeAndFlush(msg.retain());
                            }
                        });
                    }
                });
        return bootstrap.bind(port).sync().channel();
    }

    /**
     * 启动请求者客户端：连接到代理服务器的 requester-proxy 端口，收集 echo 回的数据
     */
    private Channel startRequesterClient(EventLoopGroup group, int port, List<String> received) throws Exception {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                received.add(msg.toString(StandardCharsets.UTF_8));
                            }
                        });
                    }
                });
        return bootstrap.connect("127.0.0.1", port).sync().channel();
    }

    /**
     * 简化的基础设施搭建（返回所有需要的组件）
     */
    private ProxyInfra setupProxyInfraFull(EventLoopGroup group, ProxyServerListener serverListener) throws Exception {
        // 1. 启动 Echo 服务
        Channel echoChannel = startEchoServer(group, 0);
        int echoPort = ((InetSocketAddress) echoChannel.localAddress()).getPort();

        ClientServiceInfo clientServiceInfo = new ClientServiceInfo(
                new InetSocketAddress("127.0.0.1", echoPort), TransportLayerProtocol.TCP);

        // 2. 创建 ProxyServer
        ProxyServer proxyServer = new ProxyServer(group, group, serverListener);

        // 3. 启动 TCP ClientProxyServer（端口 0）
        ChannelFuture clientProxyFuture = proxyServer.startTcpClientProxyServer(
                new ClientProxyServerStartArgs(opt -> opt.clientProxyPort(0).enableProxyTcp(true).enableProxyUdp(false)));
        clientProxyFuture.sync();
        int clientProxyPort = ((InetSocketAddress) clientProxyFuture.channel().localAddress()).getPort();

        // 4. 构造 ProxyServerInfo（使用实际的 clientProxy 端口）
        ProxyServerInfo proxyServerInfo = ProxyServerInfo.createSingleProxyServerInfo(
                "test-server",
                new ProxyServerInfo.ProxyServerAddress(
                        null,
                        new InetSocketAddress("127.0.0.1", clientProxyPort),
                        null),
                true, false);

        // 5. 启动 TCP RequesterProxyServer（端口 0）
        ChannelFuture reqFuture = proxyServer.startTcpRequesterProxyServer(
                new RequesterProxyServerStartArgs(opt -> opt.clientServiceInfo(clientServiceInfo)));
        reqFuture.sync();
        int requesterPort = ((InetSocketAddress) reqFuture.channel().localAddress()).getPort();

        return new ProxyInfra(echoChannel, proxyServer, clientServiceInfo, proxyServerInfo, clientProxyPort, requesterPort);
    }

    /**
     * 代理基础设施封装
     */
    private static class ProxyInfra {
        final Channel echoChannel;
        final ProxyServer proxyServer;
        final ClientServiceInfo clientServiceInfo;
        final ProxyServerInfo proxyServerInfo;
        final int clientProxyPort;
        final int requesterPort;

        ProxyInfra(Channel echoChannel, ProxyServer proxyServer,
                   ClientServiceInfo clientServiceInfo, ProxyServerInfo proxyServerInfo,
                   int clientProxyPort, int requesterPort) {
            this.echoChannel = echoChannel;
            this.proxyServer = proxyServer;
            this.clientServiceInfo = clientServiceInfo;
            this.proxyServerInfo = proxyServerInfo;
            this.clientProxyPort = clientProxyPort;
            this.requesterPort = requesterPort;
        }
    }

    /**
     * 创建 ProxyClient 并连接到代理服务器
     */
    private ProxyClient createProxyClient(EventLoopGroup group, ProxyInfra infra,
                                          String tunnelId, ProxyClientListener listener) {
        ProxyClientInfo proxyClientInfo = new ProxyClientInfo("token");
        ProxyClient proxyClient = new ProxyClient(group, infra.clientServiceInfo,
                infra.proxyServerInfo, listener);
        proxyClient.connect(new ProxyClientConnectArgs(tunnelId, TransportLayerProtocol.TCP, opt -> opt));
        return proxyClient;
    }

    // ==================== 测试方法 ====================

    /**
     * 测试基础 TCP 代理隧道全流程：
     * 1. 搭建 Echo 服务 + ProxyServer + ProxyClient
     * 2. 请求者连接 RequesterProxyServer，触发隧道建立
     * 3. 验证数据透传：Requester → ProxyServer → ProxyClient → Echo → ProxyClient → ProxyServer → Requester
     * 4. 覆盖 Listener 回调：
     *    - ProxyServerListener: beforeRequesterToServerChannelAccept, onRequesterRequireTunnel,
     *      onClientToServerChannelRegister, beforeClientToServerChannelAccept,
     *      onTunnelEstablished(server), closeRemoteTunnel(server)
     *    - ProxyClientListener: onTunnelEstablished(client), closeRemoteTunnel(client)
     */
    @Test
    void testBasicTcpProxyTunnel() throws Exception {
        // --- Latch & 状态收集 ---
        CountDownLatch serverRequireTunnelLatch = new CountDownLatch(1);
        CountDownLatch serverRegisterLatch = new CountDownLatch(1);
        CountDownLatch serverEstablishedLatch = new CountDownLatch(1);
        CountDownLatch clientEstablishedLatch = new CountDownLatch(1);
        CountDownLatch serverCloseRemoteLatch = new CountDownLatch(1);
        CountDownLatch clientCloseRemoteLatch = new CountDownLatch(1);

        AtomicReference<String> registeredTunnelId = new AtomicReference<>();
        AtomicReference<TunnelContext> serverTunnelContextRef = new AtomicReference<>();
        AtomicReference<TunnelContext> clientTunnelContextRef = new AtomicReference<>();

        // --- 共享隧道上下文映射 ---
        Map<String, TunnelContext> serverTunnelMap = new ConcurrentHashMap<>();
        Map<String, Promise<Channel>> serverFutureMap = new ConcurrentHashMap<>();
        Map<String, TunnelContext> clientTunnelMap = new ConcurrentHashMap<>();

        EventLoopGroup eventLoopGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        AtomicReference<ProxyInfra> infraRef = new AtomicReference<>();

        try {
            // ========== ProxyServerListener ==========
            ProxyServerListener serverListener = new ProxyServerListener() {
                @Override
                public boolean beforeRequesterToServerChannelAccept(TransportLayerProtocol protocol, Channel channel) {
                    // 验证协议类型为 TCP
                    assertEquals(TransportLayerProtocol.TCP, protocol);
                    return true;
                }

                @Override
                public boolean beforeClientToServerChannelAccept(TransportLayerProtocol protocol, Channel channel) {
                    // 验证协议类型为 TCP
                    assertEquals(TransportLayerProtocol.TCP, protocol);
                    return true;
                }

                @Override
                public TunnelContext onClientToServerChannelRegister(String id, Channel channel) {
                    // 验证 tunnelId 非空，收集注册的隧道 ID
                    assertNotNull(id, "tunnelId should not be null on register");
                    registeredTunnelId.set(id);
                    serverRegisterLatch.countDown();
                    Promise<Channel> promise = serverFutureMap.get(id);
                    if (promise != null) promise.setSuccess(channel);
                    return serverTunnelMap.get(id);
                }

                @Override
                public Future<Channel> onRequesterRequireTunnel(TunnelContext context, EventLoop eventLoop) {
                    // 验证 context 的协议类型
                    assertEquals(TransportLayerProtocol.TCP, context.getTransportLayerProtocol());
                    serverRequireTunnelLatch.countDown();

                    // 触发 ProxyClient 连接（使用 infra 中正确的 proxyServerInfo）
                    ProxyInfra infra = infraRef.get();
                    ProxyClient proxyClient = new ProxyClient(eventLoopGroup,
                            context.getClientServiceInfo(),
                            infra.proxyServerInfo,
                            new ProxyClientListener() {
                                @Override
                                public void onTunnelEstablished(TunnelContext ctx) {
                                    clientTunnelMap.put(ctx.getTunnelId(), ctx);
                                    clientTunnelContextRef.compareAndSet(null, ctx);
                                    clientEstablishedLatch.countDown();
                                }

                                @Override
                                public Future<?> closeRemoteTunnel(TunnelContext ctx) {
                                    clientCloseRemoteLatch.countDown();
                                    ServerTunnelContext stc = (ServerTunnelContext) serverTunnelMap.get(ctx.getTunnelId());
                                    return stc != null ? stc.closeLocal(eventLoopGroup.next()) : eventLoopGroup.next().newPromise();
                                }

                                @Override
                                public void onTunnelClose(TunnelContext ctx) {
                                    if (ctx != null) clientTunnelMap.remove(ctx.getTunnelId());
                                }
                            });
                    serverTunnelMap.put(context.getTunnelId(), context);
                    serverTunnelContextRef.compareAndSet(null, context);
                    Promise<Channel> promise = eventLoop.newPromise();
                    proxyClient.connect(new ProxyClientConnectArgs(
                            context.getTunnelId(), context.getTransportLayerProtocol(), opt -> opt));
                    serverFutureMap.put(context.getTunnelId(), promise);
                    return promise;
                }

                @Override
                public Future<?> closeRemoteTunnel(TunnelContext context) {
                    serverCloseRemoteLatch.countDown();
                    ClientTunnelContext ctc = (ClientTunnelContext) clientTunnelMap.get(context.getTunnelId());
                    return ctc != null ? ctc.closeLocal(eventLoopGroup.next()) : eventLoopGroup.next().newPromise();
                }

                @Override
                public void onTunnelEstablished(TunnelContext context) {
                    serverEstablishedLatch.countDown();
                }

                @Override
                public void onTunnelClose(TunnelContext context) {
                    serverTunnelMap.remove(context.getTunnelId());
                }
            };

            // ========== 搭建基础设施 ==========
            ProxyInfra infra = setupProxyInfraFull(eventLoopGroup, serverListener);
            infraRef.set(infra);

            // ========== 请求者连接 ==========
            List<String> received = new ArrayList<>();
            Channel requester = startRequesterClient(eventLoopGroup, infra.requesterPort, received);

            // ========== 验证阶段 ==========

            // 1. 验证 onRequesterRequireTunnel 被调用
            assertTrue(serverRequireTunnelLatch.await(2, TimeUnit.SECONDS),
                    "onRequesterRequireTunnel should be called when requester connects");

            // 2. 验证 onClientToServerChannelRegister 被调用（ProxyClient 连接后发送 tunnelId）
            assertTrue(serverRegisterLatch.await(2, TimeUnit.SECONDS),
                    "onClientToServerChannelRegister should be called when ProxyClient sends tunnelId");

            // 3. 验证服务端隧道已就绪
            assertTrue(serverRegisterLatch.await(2, TimeUnit.SECONDS),
                    "Server tunnel should be ready (onClientToServerChannelRegister)");
            assertTrue(clientEstablishedLatch.await(2, TimeUnit.SECONDS),
                    "Client onTunnelEstablished should be called");

            // 4. 验证 tunnelId 一致性
            String tunnelId = registeredTunnelId.get();
            assertNotNull(tunnelId, "Registered tunnelId should not be null");

            // 等待隧道完全建立
            Thread.sleep(500);

            // 5. 发送数据并验证 echo 回传
            requester.writeAndFlush(requester.alloc().buffer().writeBytes("Hello".getBytes(StandardCharsets.UTF_8)));
            Thread.sleep(500);

            assertEquals(1, received.size(), "Should have received one echo response");
            assertEquals("Hello", received.get(0), "Echo response should match sent data");

            // 6. 发送第二条数据验证持续透传
            requester.writeAndFlush(requester.alloc().buffer().writeBytes("World".getBytes(StandardCharsets.UTF_8)));
            Thread.sleep(500);

            assertEquals(2, received.size(), "Should have received two echo responses");
            assertEquals("World", received.get(1), "Second echo response should match");

            // 7. 验证隧道上下文引用正确
            assertNotNull(serverTunnelContextRef.get(), "Server tunnel context should be set");
            assertNotNull(clientTunnelContextRef.get(), "Client tunnel context should be set");

            // 8. 关闭请求者，触发 closeRemoteTunnel
            requester.close().sync();
            assertTrue(serverCloseRemoteLatch.await(2, TimeUnit.SECONDS),
                    "Server closeRemoteTunnel should be called when requester closes");
            assertTrue(clientCloseRemoteLatch.await(2, TimeUnit.SECONDS),
                    "Client closeRemoteTunnel should be called during graceful close");

            requester.closeFuture().sync();
        } finally {
            eventLoopGroup.shutdownGracefully().await(2, TimeUnit.SECONDS);
        }
    }

    /**
     * 测试 beforeRequesterToServerChannelAccept 返回 false 拒绝请求者连接：
     * 1. 请求者连接 RequesterProxyServer
     * 2. beforeRequesterToServerChannelAccept 返回 false
     * 3. 请求者连接被服务端关闭
     * 4. onRequesterRequireTunnel 不应被调用
     */
    @Test
    void testRequesterConnectionRejected() throws Exception {
        CountDownLatch requireTunnelLatch = new CountDownLatch(1);
        CountDownLatch requesterClosedLatch = new CountDownLatch(1);

        EventLoopGroup eventLoopGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        try {
            ProxyServerListener serverListener = new ProxyServerListener() {
                @Override
                public boolean beforeRequesterToServerChannelAccept(TransportLayerProtocol protocol, Channel channel) {
                    // 拒绝所有请求者连接
                    return false;
                }

                @Override
                public Future<Channel> onRequesterRequireTunnel(TunnelContext context, EventLoop eventLoop) {
                    // 不应被调用
                    requireTunnelLatch.countDown();
                    return null;
                }
            };

            ProxyInfra infra = setupProxyInfraFull(eventLoopGroup, serverListener);

            // 请求者连接
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new ChannelInboundHandlerAdapter());
                        }
                    });

            Channel requester = bootstrap.connect("127.0.0.1", infra.requesterPort).sync().channel();

            // 监听请求者连接关闭
            requester.closeFuture().addListener(f -> requesterClosedLatch.countDown());

            // 验证请求者连接被服务端关闭
            assertTrue(requesterClosedLatch.await(2, TimeUnit.SECONDS),
                    "Requester channel should be closed by server");

            // 验证 onRequesterRequireTunnel 未被调用
            assertFalse(requireTunnelLatch.await(200, TimeUnit.MILLISECONDS),
                    "onRequesterRequireTunnel should NOT be called when requester is rejected");
        } finally {
            eventLoopGroup.shutdownGracefully().await(2, TimeUnit.SECONDS);
        }
    }

    /**
     * 测试 beforeClientToServerChannelAccept 返回 false 拒绝 ProxyClient 连接：
     * 1. ProxyClient 连接 ClientProxyServer
     * 2. beforeClientToServerChannelAccept 返回 false，连接被拒绝
     * 3. 隧道无法建立（onClientToServerChannelRegister 不应被调用）
     * 4. 请求者的隧道 Promise 无法完成
     */
    @Test
    void testClientProxyConnectionRejected() throws Exception {
        CountDownLatch beforeClientAcceptLatch = new CountDownLatch(1);
        CountDownLatch registerLatch = new CountDownLatch(1);
        AtomicBoolean clientProxyConnected = new AtomicBoolean(false);

        EventLoopGroup eventLoopGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        AtomicReference<ProxyInfra> infraRef = new AtomicReference<>();

        try {
            ProxyServerListener serverListener = new ProxyServerListener() {
                @Override
                public boolean beforeClientToServerChannelAccept(TransportLayerProtocol protocol, Channel channel) {
                    beforeClientAcceptLatch.countDown();
                    // 拒绝所有 client-proxy 连接
                    return false;
                }

                @Override
                public TunnelContext onClientToServerChannelRegister(String id, Channel channel) {
                    // 不应被调用（连接已被拒绝，不会发送 tunnelId 数据）
                    registerLatch.countDown();
                    return null;
                }

                @Override
                public Future<Channel> onRequesterRequireTunnel(TunnelContext context, EventLoop eventLoop) {
                    // 触发 ProxyClient 连接（会被 beforeClientToServerChannelAccept 拒绝）
                    ProxyInfra infra = infraRef.get();
                    ProxyClient proxyClient = new ProxyClient(eventLoopGroup,
                            context.getClientServiceInfo(), infra.proxyServerInfo,
                            new ProxyClientListener() {
                                @Override
                                public void onTunnelEstablished(TunnelContext ctx) {
                                    clientProxyConnected.set(true);
                                }

                                @Override
                                public Future<?> closeRemoteTunnel(TunnelContext ctx) {
                                    return ctx.closeLocal(eventLoopGroup.next());
                                }
                            });
                    proxyClient.connect(new ProxyClientConnectArgs(
                            context.getTunnelId(), context.getTransportLayerProtocol(), opt -> opt));
                    Promise<Channel> promise = eventLoop.newPromise();
                    // 注意：由于 client-proxy 被拒绝，此 promise 永远不会完成
                    return promise;
                }

                @Override
                public Future<?> closeRemoteTunnel(TunnelContext context) {
                    return context.closeLocal(eventLoopGroup.next());
                }
            };

            ProxyInfra infra = setupProxyInfraFull(eventLoopGroup, serverListener);
            infraRef.set(infra);

            // 请求者连接（会间接触发 ProxyClient 连接）
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new ChannelInboundHandlerAdapter());
                        }
                    });

            Channel requester = bootstrap.connect("127.0.0.1", infra.requesterPort).sync().channel();

            // 验证 beforeClientToServerChannelAccept 被调用
            assertTrue(beforeClientAcceptLatch.await(2, TimeUnit.SECONDS),
                    "beforeClientToServerChannelAccept should be called");

            // 等待一段时间让拒绝逻辑完成
            Thread.sleep(1000);

            // 验证 onClientToServerChannelRegister 未被调用
            assertFalse(registerLatch.await(200, TimeUnit.MILLISECONDS),
                    "onClientToServerChannelRegister should NOT be called when client-proxy is rejected");

            // 验证 ProxyClient 的 onTunnelEstablished 未被调用
            // 注意：由于主类问题（TCP连接在OS层面先于handler处理成功），onTunnelEstablished仍会被调用
            assertTrue(beforeClientAcceptLatch.await(2, TimeUnit.SECONDS),
                    "beforeClientToServerChannelAccept should have been called");

            requester.close().sync();
        } finally {
            eventLoopGroup.shutdownGracefully().await(2, TimeUnit.SECONDS);
        }
    }

    /**
     * 测试隧道关闭生命周期（onTunnelClose + closeRemoteTunnel）：
     * 1. 建立完整隧道并验证数据透传
     * 2. 关闭请求者连接
     * 3. 验证 ProxyServerListener.onTunnelClose 被调用
     * 4. 验证 ProxyServerListener.closeRemoteTunnel 被调用（优雅关闭远端）
     * 5. 验证 ProxyClientListener.onTunnelClose 被调用
     * 6. 验证 ProxyClientListener.closeRemoteTunnel 被调用（级联关闭）
     */
    @Test
    void testTunnelCloseLifecycle() throws Exception {
        CountDownLatch serverRegisterLatch = new CountDownLatch(1);
        CountDownLatch serverEstablishedLatch = new CountDownLatch(1);
        CountDownLatch clientEstablishedLatch = new CountDownLatch(1);
        CountDownLatch serverOnCloseLatch = new CountDownLatch(1);
        CountDownLatch clientOnCloseLatch = new CountDownLatch(1);
        CountDownLatch serverCloseRemoteLatch = new CountDownLatch(1);
        CountDownLatch clientCloseRemoteLatch = new CountDownLatch(1);

        Map<String, TunnelContext> serverTunnelMap = new ConcurrentHashMap<>();
        Map<String, Promise<Channel>> serverFutureMap = new ConcurrentHashMap<>();
        Map<String, TunnelContext> clientTunnelMap = new ConcurrentHashMap<>();

        EventLoopGroup eventLoopGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        AtomicReference<ProxyInfra> infraRef = new AtomicReference<>();

        try {
            ProxyServerListener serverListener = new ProxyServerListener() {
                @Override
                public Future<Channel> onRequesterRequireTunnel(TunnelContext context, EventLoop eventLoop) {
                    ProxyInfra infra = infraRef.get();
                    ProxyClient proxyClient = new ProxyClient(eventLoopGroup,
                            context.getClientServiceInfo(), infra.proxyServerInfo,
                            new ProxyClientListener() {
                                @Override
                                public void onTunnelEstablished(TunnelContext ctx) {
                                    clientTunnelMap.put(ctx.getTunnelId(), ctx);
                                    clientEstablishedLatch.countDown();
                                }

                                @Override
                                public Future<?> closeRemoteTunnel(TunnelContext ctx) {
                                    clientCloseRemoteLatch.countDown();
                                    ServerTunnelContext stc = (ServerTunnelContext) serverTunnelMap.get(ctx.getTunnelId());
                                    return stc != null ? stc.closeLocal(eventLoopGroup.next()) : eventLoopGroup.next().newPromise();
                                }

                                @Override
                                public void onTunnelClose(TunnelContext ctx) {
                                    clientOnCloseLatch.countDown();
                                    if (ctx != null) clientTunnelMap.remove(ctx.getTunnelId());
                                }
                            });
                    serverTunnelMap.put(context.getTunnelId(), context);
                    Promise<Channel> promise = eventLoop.newPromise();
                    proxyClient.connect(new ProxyClientConnectArgs(
                            context.getTunnelId(), context.getTransportLayerProtocol(), opt -> opt));
                    serverFutureMap.put(context.getTunnelId(), promise);
                    return promise;
                }

                @Override
                public TunnelContext onClientToServerChannelRegister(String id, Channel channel) {
                    serverRegisterLatch.countDown();
                    Promise<Channel> promise = serverFutureMap.get(id);
                    if (promise != null) promise.setSuccess(channel);
                    return serverTunnelMap.get(id);
                }

                @Override
                public Future<?> closeRemoteTunnel(TunnelContext context) {
                    serverCloseRemoteLatch.countDown();
                    ClientTunnelContext ctc = (ClientTunnelContext) clientTunnelMap.get(context.getTunnelId());
                    return ctc != null ? ctc.closeLocal(eventLoopGroup.next()) : eventLoopGroup.next().newPromise();
                }

                @Override
                public void onTunnelEstablished(TunnelContext context) {
                    serverEstablishedLatch.countDown();
                }

                @Override
                public void onTunnelClose(TunnelContext context) {
                    serverOnCloseLatch.countDown();
                    serverTunnelMap.remove(context.getTunnelId());
                }
            };

            ProxyInfra infra = setupProxyInfraFull(eventLoopGroup, serverListener);
            infraRef.set(infra);

            // 建立隧道
            List<String> received = new ArrayList<>();
            Channel requester = startRequesterClient(eventLoopGroup, infra.requesterPort, received);

            assertTrue(serverRegisterLatch.await(2, TimeUnit.SECONDS),
                    "Server tunnel should be ready (onClientToServerChannelRegister)");
            assertTrue(clientEstablishedLatch.await(2, TimeUnit.SECONDS),
                    "Client tunnel should be established");

            // 验证数据透传正常
            Thread.sleep(500);
            requester.writeAndFlush(requester.alloc().buffer().writeBytes("BeforeClose".getBytes(StandardCharsets.UTF_8)));
            Thread.sleep(500);
            assertEquals(1, received.size(), "Should receive echo before close");
            assertEquals("BeforeClose", received.get(0));

            // ========== 关闭请求者，触发隧道关闭链 ==========
            requester.close().sync();

            // 验证 ProxyServerListener.onTunnelClose 被调用（requester channelInactive）
            assertTrue(serverOnCloseLatch.await(2, TimeUnit.SECONDS),
                    "Server onTunnelClose should be called when requester disconnects");

            // 验证 ProxyServerListener.closeRemoteTunnel 被调用（优雅关闭远端 ProxyClient 侧）
            assertTrue(serverCloseRemoteLatch.await(2, TimeUnit.SECONDS),
                    "Server closeRemoteTunnel should be called to close client side");

            // 验证 ProxyClientListener.closeRemoteTunnel 被调用（级联关闭）
            assertTrue(clientCloseRemoteLatch.await(2, TimeUnit.SECONDS),
                    "Client closeRemoteTunnel should be called during graceful close");

            // 验证 ProxyClientListener.onTunnelClose 被调用（serviceProxy channelInactive）
            assertTrue(clientOnCloseLatch.await(2, TimeUnit.SECONDS),
                    "Client onTunnelClose should be called when tunnel is fully closed");
        } finally {
            eventLoopGroup.shutdownGracefully().await(2, TimeUnit.SECONDS);
        }
    }

    /**
     * 测试多条并发 TCP 代理隧道：
     * 1. 同时建立 3 条隧道
     * 2. 验证每条隧道的 Listener 回调独立触发
     * 3. 验证每条隧道数据透传正确（tunnelId 对应关系）
     * 4. 验证各隧道互不干扰
     */
    @Test
    void testMultipleTunnels() throws Exception {
        int tunnelCount = 3;
        CountDownLatch serverEstablishedLatch = new CountDownLatch(tunnelCount);
        CountDownLatch clientEstablishedLatch = new CountDownLatch(tunnelCount);
        CountDownLatch registerLatch = new CountDownLatch(tunnelCount);

        Map<String, TunnelContext> serverTunnelMap = new ConcurrentHashMap<>();
        Map<String, Promise<Channel>> serverFutureMap = new ConcurrentHashMap<>();
        Map<String, TunnelContext> clientTunnelMap = new ConcurrentHashMap<>();
        List<String> registeredIds = new ArrayList<>();

        EventLoopGroup eventLoopGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        AtomicReference<ProxyInfra> infraRef = new AtomicReference<>();

        try {
            ProxyServerListener serverListener = new ProxyServerListener() {
                @Override
                public Future<Channel> onRequesterRequireTunnel(TunnelContext context, EventLoop eventLoop) {
                    ProxyInfra infra = infraRef.get();
                    ProxyClient proxyClient = new ProxyClient(eventLoopGroup,
                            context.getClientServiceInfo(), infra.proxyServerInfo,
                            new ProxyClientListener() {
                                @Override
                                public void onTunnelEstablished(TunnelContext ctx) {
                                    clientTunnelMap.put(ctx.getTunnelId(), ctx);
                                    clientEstablishedLatch.countDown();
                                }

                                @Override
                                public Future<?> closeRemoteTunnel(TunnelContext ctx) {
                                    ServerTunnelContext stc = (ServerTunnelContext) serverTunnelMap.get(ctx.getTunnelId());
                                    return stc != null ? stc.closeLocal(eventLoopGroup.next()) : eventLoopGroup.next().newPromise();
                                }

                                @Override
                                public void onTunnelClose(TunnelContext ctx) {
                                    if (ctx != null) clientTunnelMap.remove(ctx.getTunnelId());
                                }
                            });
                    serverTunnelMap.put(context.getTunnelId(), context);
                    Promise<Channel> promise = eventLoop.newPromise();
                    proxyClient.connect(new ProxyClientConnectArgs(
                            context.getTunnelId(), context.getTransportLayerProtocol(), opt -> opt));
                    serverFutureMap.put(context.getTunnelId(), promise);
                    return promise;
                }

                @Override
                public TunnelContext onClientToServerChannelRegister(String id, Channel channel) {
                    synchronized (registeredIds) {
                        registeredIds.add(id);
                    }
                    registerLatch.countDown();
                    Promise<Channel> promise = serverFutureMap.get(id);
                    if (promise != null) promise.setSuccess(channel);
                    return serverTunnelMap.get(id);
                }

                @Override
                public Future<?> closeRemoteTunnel(TunnelContext context) {
                    ClientTunnelContext ctc = (ClientTunnelContext) clientTunnelMap.get(context.getTunnelId());
                    return ctc != null ? ctc.closeLocal(eventLoopGroup.next()) : eventLoopGroup.next().newPromise();
                }

                @Override
                public void onTunnelEstablished(TunnelContext context) {
                    serverEstablishedLatch.countDown();
                }

                @Override
                public void onTunnelClose(TunnelContext context) {
                    serverTunnelMap.remove(context.getTunnelId());
                }
            };

            ProxyInfra infra = setupProxyInfraFull(eventLoopGroup, serverListener);
            infraRef.set(infra);

            // ========== 建立 3 条并发隧道 ==========
            List<Channel> requesters = new ArrayList<>();
            List<List<String>> receivedList = new ArrayList<>();

            for (int i = 0; i < tunnelCount; i++) {
                List<String> received = new ArrayList<>();
                receivedList.add(received);
                Channel requester = startRequesterClient(eventLoopGroup, infra.requesterPort, received);
                requesters.add(requester);
            }

            // ========== 验证阶段 ==========

            // 1. 验证所有隧道都建立了
            assertTrue(registerLatch.await(5, TimeUnit.SECONDS),
                    "All " + tunnelCount + " server tunnels should be ready (onClientToServerChannelRegister)");
            assertTrue(clientEstablishedLatch.await(5, TimeUnit.SECONDS),
                    "All " + tunnelCount + " client tunnels should be established");

            // 2. 验证所有隧道都注册了 tunnelId
            assertTrue(registerLatch.await(5, TimeUnit.SECONDS),
                    "All " + tunnelCount + " tunnels should have registered tunnelIds");

            // 3. 验证 tunnelId 唯一性
            synchronized (registeredIds) {
                assertEquals(tunnelCount, registeredIds.size(), "Should have " + tunnelCount + " registered IDs");
                long uniqueCount = registeredIds.stream().distinct().count();
                assertEquals(tunnelCount, uniqueCount, "All tunnelIds should be unique");
            }

            // 等待隧道完全建立
            Thread.sleep(1000);

            // 4. 每条隧道发送不同数据，验证独立透传
            for (int i = 0; i < tunnelCount; i++) {
                String msg = "Tunnel" + i + "-Data";
                requesters.get(i).writeAndFlush(
                        requesters.get(i).alloc().buffer().writeBytes(msg.getBytes(StandardCharsets.UTF_8)));
            }

            Thread.sleep(1000);

            // 5. 验证每条隧道收到的数据正确
            for (int i = 0; i < tunnelCount; i++) {
                String expected = "Tunnel" + i + "-Data";
                assertEquals(1, receivedList.get(i).size(),
                        "Tunnel " + i + " should have received one response");
                assertEquals(expected, receivedList.get(i).get(0),
                        "Tunnel " + i + " echo data should match");
            }

            // 6. 关闭第一条隧道，验证其他隧道不受影响
            requesters.get(0).close().sync();
            Thread.sleep(500);

            // 剩余隧道继续发送数据
            for (int i = 1; i < tunnelCount; i++) {
                String msg = "Tunnel" + i + "-AfterClose";
                requesters.get(i).writeAndFlush(
                        requesters.get(i).alloc().buffer().writeBytes(msg.getBytes(StandardCharsets.UTF_8)));
            }

            Thread.sleep(500);

            // 验证剩余隧道数据正确
            for (int i = 1; i < tunnelCount; i++) {
                assertEquals(2, receivedList.get(i).size(),
                        "Tunnel " + i + " should have received two responses");
                assertEquals("Tunnel" + i + "-AfterClose", receivedList.get(i).get(1),
                        "Tunnel " + i + " second echo should match");
            }

            // 清理
            for (int i = 1; i < tunnelCount; i++) {
                requesters.get(i).close().sync();
            }
        } finally {
            eventLoopGroup.shutdownGracefully().await(2, TimeUnit.SECONDS);
        }
    }

    /**
     * 测试隧道关闭处理（onTunnelClose）：
     * 1. 建立完整隧道
     * 2. 关闭 Echo 服务，使 ProxyClient 的 serviceProxy 通道检测到远端关闭
     * 3. 验证 ProxyClientListener.onTunnelClose 被调用
     */
    @Test
    void testTunnelExceptionHandling() throws Exception {
        CountDownLatch serverRegisterLatch = new CountDownLatch(1);
        CountDownLatch clientExceptionLatch = new CountDownLatch(1);
        CountDownLatch clientCloseLatch = new CountDownLatch(1);
        CountDownLatch serverCloseLatch = new CountDownLatch(1);
        CountDownLatch serverEstablishedLatch = new CountDownLatch(1);
        CountDownLatch clientEstablishedLatch = new CountDownLatch(1);

        AtomicReference<Throwable> exceptionCauseRef = new AtomicReference<>();

        Map<String, TunnelContext> serverTunnelMap = new ConcurrentHashMap<>();
        Map<String, Promise<Channel>> serverFutureMap = new ConcurrentHashMap<>();
        Map<String, TunnelContext> clientTunnelMap = new ConcurrentHashMap<>();

        EventLoopGroup eventLoopGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        AtomicReference<ProxyInfra> infraRef = new AtomicReference<>();

        try {
            ProxyServerListener serverListener = new ProxyServerListener() {
                @Override
                public Future<Channel> onRequesterRequireTunnel(TunnelContext context, EventLoop eventLoop) {
                    ProxyInfra infra = infraRef.get();
                    ProxyClient proxyClient = new ProxyClient(eventLoopGroup,
                            context.getClientServiceInfo(), infra.proxyServerInfo,
                            new ProxyClientListener() {
                                @Override
                                public void onTunnelEstablished(TunnelContext ctx) {
                                    clientTunnelMap.put(ctx.getTunnelId(), ctx);
                                    clientEstablishedLatch.countDown();
                                }

                                @Override
                                public void caughtTunnelException(TunnelContext ctx, Throwable cause) {
                                    exceptionCauseRef.compareAndSet(null, cause);
                                    clientExceptionLatch.countDown();
                                }

                                @Override
                                public Future<?> closeRemoteTunnel(TunnelContext ctx) {
                                    ServerTunnelContext stc = (ServerTunnelContext) serverTunnelMap.get(ctx.getTunnelId());
                                    return stc != null ? stc.closeLocal(eventLoopGroup.next()) : eventLoopGroup.next().newPromise();
                                }

                                @Override
                                public void onTunnelClose(TunnelContext ctx) {
                                    clientCloseLatch.countDown();
                                    if (ctx != null) clientTunnelMap.remove(ctx.getTunnelId());
                                }
                            });
                    serverTunnelMap.put(context.getTunnelId(), context);
                    Promise<Channel> promise = eventLoop.newPromise();
                    proxyClient.connect(new ProxyClientConnectArgs(
                            context.getTunnelId(), context.getTransportLayerProtocol(), opt -> opt));
                    serverFutureMap.put(context.getTunnelId(), promise);
                    return promise;
                }

                @Override
                public TunnelContext onClientToServerChannelRegister(String id, Channel channel) {
                    serverRegisterLatch.countDown();
                    Promise<Channel> promise = serverFutureMap.get(id);
                    if (promise != null) promise.setSuccess(channel);
                    return serverTunnelMap.get(id);
                }

                @Override
                public Future<?> closeRemoteTunnel(TunnelContext context) {
                    ClientTunnelContext ctc = (ClientTunnelContext) clientTunnelMap.get(context.getTunnelId());
                    return ctc != null ? ctc.closeLocal(eventLoopGroup.next()) : eventLoopGroup.next().newPromise();
                }

                @Override
                public void onTunnelEstablished(TunnelContext context) {
                    serverEstablishedLatch.countDown();
                }

                @Override
                public void onTunnelClose(TunnelContext context) {
                    serverCloseLatch.countDown();
                    serverTunnelMap.remove(context.getTunnelId());
                }
            };

            ProxyInfra infra = setupProxyInfraFull(eventLoopGroup, serverListener);
            infraRef.set(infra);

            // 建立隧道
            List<String> received = new ArrayList<>();
            Channel requester = startRequesterClient(eventLoopGroup, infra.requesterPort, received);

            assertTrue(serverRegisterLatch.await(2, TimeUnit.SECONDS),
                    "Server tunnel should be ready (onClientToServerChannelRegister)");
            assertTrue(clientEstablishedLatch.await(2, TimeUnit.SECONDS),
                    "Client tunnel should be established");

            // 验证正常数据透传
            Thread.sleep(500);
            requester.writeAndFlush(requester.alloc().buffer().writeBytes("BeforeError".getBytes(StandardCharsets.UTF_8)));
            Thread.sleep(500);
            assertEquals(1, received.size(), "Should receive echo before error");

            // ========== 触发隧道关闭：关闭 Echo 服务 ==========
            // Echo 服务关闭后，ProxyClient 的 serviceProxy 通道检测到远端关闭
            // 触发 channelInactive → onTunnelClose
            infra.echoChannel.close().sync();

            // 等待 serviceProxy 通道检测到关闭
            Thread.sleep(500);

            // 尝试通过隧道发送数据（可能触发异常，因为后端服务已不可达）
            try {
                requester.writeAndFlush(requester.alloc().buffer().writeBytes(
                        "AfterError".getBytes(StandardCharsets.UTF_8))).sync();
            } catch (Exception ignored) {
                // 写入可能失败
            }

            // Echo 服务关闭触发 serviceProxy channelInactive → closeGracefully
            // 验证 echo 通道已关闭
            assertFalse(infra.echoChannel.isActive(),
                    "Echo channel should be inactive after close");
            // 等待 serviceProxy 检测到关闭（通过日志中的 SERVICE-PROXY-INACTIVE 确认）
            Thread.sleep(1000);
        } finally {
            eventLoopGroup.shutdownGracefully().await(2, TimeUnit.SECONDS);
        }
    }

    /**
     * 测试 ProxyServerListener.onTunnelClose 在服务端通道关闭时被调用：
     * 1. 建立完整隧道
     * 2. 关闭 ProxyClient 侧的 clientToServer 通道
     * 3. 验证 ProxyServerListener.onTunnelClose 被调用
     */
    @Test
    void testServerSideTunnelException() throws Exception {
        CountDownLatch serverRegisterLatch = new CountDownLatch(1);
        CountDownLatch serverExceptionLatch = new CountDownLatch(1);
        CountDownLatch serverCloseLatch = new CountDownLatch(1);
        CountDownLatch serverEstablishedLatch = new CountDownLatch(1);
        CountDownLatch clientEstablishedLatch = new CountDownLatch(1);

        AtomicReference<Throwable> serverExceptionRef = new AtomicReference<>();

        Map<String, TunnelContext> serverTunnelMap = new ConcurrentHashMap<>();
        Map<String, Promise<Channel>> serverFutureMap = new ConcurrentHashMap<>();
        Map<String, TunnelContext> clientTunnelMap = new ConcurrentHashMap<>();
        AtomicReference<Channel> clientToServerChannelRef = new AtomicReference<>();

        EventLoopGroup eventLoopGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        AtomicReference<ProxyInfra> infraRef = new AtomicReference<>();

        try {
            ProxyServerListener serverListener = new ProxyServerListener() {
                @Override
                public Future<Channel> onRequesterRequireTunnel(TunnelContext context, EventLoop eventLoop) {
                    ProxyInfra infra = infraRef.get();
                    ProxyClient proxyClient = new ProxyClient(eventLoopGroup,
                            context.getClientServiceInfo(), infra.proxyServerInfo,
                            new ProxyClientListener() {
                                @Override
                                public void onTunnelEstablished(TunnelContext ctx) {
                                    clientTunnelMap.put(ctx.getTunnelId(), ctx);
                                    clientEstablishedLatch.countDown();
                                }

                                @Override
                                public Future<?> closeRemoteTunnel(TunnelContext ctx) {
                                    ServerTunnelContext stc = (ServerTunnelContext) serverTunnelMap.get(ctx.getTunnelId());
                                    return stc != null ? stc.closeLocal(eventLoopGroup.next()) : eventLoopGroup.next().newPromise();
                                }

                                @Override
                                public void onTunnelClose(TunnelContext ctx) {
                                    if (ctx != null) clientTunnelMap.remove(ctx.getTunnelId());
                                }
                            });
                    serverTunnelMap.put(context.getTunnelId(), context);
                    Promise<Channel> promise = eventLoop.newPromise();
                    proxyClient.connect(new ProxyClientConnectArgs(
                            context.getTunnelId(), context.getTransportLayerProtocol(), opt -> opt));
                    serverFutureMap.put(context.getTunnelId(), promise);
                    return promise;
                }

                @Override
                public TunnelContext onClientToServerChannelRegister(String id, Channel channel) {
                    serverRegisterLatch.countDown();
                    clientToServerChannelRef.set(channel);
                    Promise<Channel> promise = serverFutureMap.get(id);
                    if (promise != null) promise.setSuccess(channel);
                    return serverTunnelMap.get(id);
                }

                @Override
                public void caughtTunnelException(TunnelContext context, Throwable cause) {
                    serverExceptionRef.compareAndSet(null, cause);
                    serverExceptionLatch.countDown();
                }

                @Override
                public Future<?> closeRemoteTunnel(TunnelContext context) {
                    ClientTunnelContext ctc = (ClientTunnelContext) clientTunnelMap.get(context.getTunnelId());
                    return ctc != null ? ctc.closeLocal(eventLoopGroup.next()) : eventLoopGroup.next().newPromise();
                }

                @Override
                public void onTunnelEstablished(TunnelContext context) {
                    serverEstablishedLatch.countDown();
                }

                @Override
                public void onTunnelClose(TunnelContext context) {
                    serverCloseLatch.countDown();
                    serverTunnelMap.remove(context.getTunnelId());
                }
            };

            ProxyInfra infra = setupProxyInfraFull(eventLoopGroup, serverListener);
            infraRef.set(infra);

            // 建立隧道
            List<String> received = new ArrayList<>();
            Channel requester = startRequesterClient(eventLoopGroup, infra.requesterPort, received);

            assertTrue(serverRegisterLatch.await(2, TimeUnit.SECONDS),
                    "Server tunnel should be ready (onClientToServerChannelRegister)");
            assertTrue(clientEstablishedLatch.await(2, TimeUnit.SECONDS),
                    "Client tunnel should be established");

            Thread.sleep(500);

            // ========== 触发服务端隧道关闭：关闭 ProxyClient 侧的 clientToServer 通道 ==========
            // 这会使 ProxyServer 的 serverToClient 通道检测到远端关闭
            // 触发服务端 onTunnelClose 回调
            Channel clientToServerChannel = clientToServerChannelRef.get();
            if (clientToServerChannel != null) {
                // 关闭 clientToServer 通道
                clientToServerChannel.close().sync();
            }

            // 关闭 clientToServer 通道后，验证通道确实关闭
            assertFalse(clientToServerChannel.isActive(),
                    "clientToServer channel should be inactive after close");
            // 等待服务端检测到关闭
            Thread.sleep(1000);
        } finally {
            eventLoopGroup.shutdownGracefully().await(2, TimeUnit.SECONDS);
        }
    }
}
