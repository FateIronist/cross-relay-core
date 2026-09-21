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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import top.fateironist.cross_relay_core.control.ControlClient;
import top.fateironist.cross_relay_core.control.ControlServer;
import top.fateironist.cross_relay_core.control.listener.ControlClientListener;
import top.fateironist.cross_relay_core.control.listener.ControlServerListener;
import top.fateironist.cross_relay_core.model.DeploymentMode;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.args.control.ControlClientConnectAbstractArgs;
import top.fateironist.cross_relay_core.model.args.control.ControlServerStartArgs;
import top.fateironist.cross_relay_core.model.args.proxy_client.ProxyClientConnectArgs;
import top.fateironist.cross_relay_core.model.args.proxy_server.ClientProxyServerStartArgs;
import top.fateironist.cross_relay_core.model.args.proxy_server.RequesterProxyServerStartArgs;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.control.ProxyControlEventEnum;
import top.fateironist.cross_relay_core.model.control.event.CommonInfo;
import top.fateironist.cross_relay_core.model.info.ClientServiceInfo;
import top.fateironist.cross_relay_core.model.info.ProxyClientInfo;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ClientTcpTunnelContext;
import top.fateironist.cross_relay_core.proxy.ProxyTcpClient;
import top.fateironist.cross_relay_core.proxy.ProxyTcpServer;
import top.fateironist.cross_relay_core.proxy.listener.ProxyClientListener;
import top.fateironist.cross_relay_core.proxy.listener.ProxyServerListener;
import top.fateironist.cross_relay_core.util.JsonUtil;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ControlServer + ControlClient + ProxyTcpServer + ProxyTcpClient TCP 代理集成测试。
 *
 * <p>编排链路：requester 接入 → 服务端经控制通道下发 REQUIRE_CHANNEL →
 * 客户端事件处理器创建对端隧道 → 注册包匹配 tunnelId 后双端齐备 → 隧道激活转发数据。
 * 隧道关闭经控制通道 TUNNEL_CLOSE 通知对端，两侧容器销毁级联回收 channel。
 */
class TcpProxyTest {

    @BeforeAll
    static void setupLogging() {
        LoggerContext ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
        ctx.getLogger(ProxyTcpServer.class).setLevel(Level.DEBUG);
        ctx.getLogger(ProxyTcpClient.class).setLevel(Level.DEBUG);
        ctx.getLogger(ControlServer.class).setLevel(Level.DEBUG);
        ctx.getLogger(ControlClient.class).setLevel(Level.DEBUG);
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
     * 测试基础设施：控制通道 + client-proxy 共享端口 + requester-proxy 端口 + echo 服务
     */
    private static class ProxyInfra {
        final Channel echoChannel;
        final ControlServer controlServer;
        final ControlServerListener controlServerListener;
        final ControlClient controlClient;
        final ProxyTcpServer proxyTcpServer;
        final ProxyTcpClient proxyTcpClient;
        final int clientProxyPort;
        final int requesterPort;

        ProxyInfra(Channel echoChannel, ControlServer controlServer, ControlServerListener controlServerListener,
                   ControlClient controlClient,
                   ProxyTcpServer proxyTcpServer, ProxyTcpClient proxyTcpClient,
                   int clientProxyPort, int requesterPort) {
            this.echoChannel = echoChannel;
            this.controlServer = controlServer;
            this.controlServerListener = controlServerListener;
            this.controlClient = controlClient;
            this.proxyTcpServer = proxyTcpServer;
            this.proxyTcpClient = proxyTcpClient;
            this.clientProxyPort = clientProxyPort;
            this.requesterPort = requesterPort;
        }
    }

    /**
     * 搭建完整基础设施：
     * 1. echo 服务
     * 2. ControlServer + ControlClient 控制通道握手
     * 3. ProxyTcpServer 绑定 client-proxy 共享端口
     * 4. 客户端监听 REQUIRE_CHANNEL（建隧道）与 TUNNEL_CLOSE（关隧道）控制事件
     * 5. ProxyTcpServer 绑定 requester-proxy 端口
     */
    private ProxyInfra setupInfra(EventLoopGroup group, ProxyServerListener serverListener,
                                  ProxyClientListener clientListener,
                                  Map<String, ClientTcpTunnelContext> clientTunnelMap,
                                  List<String> requiredTunnelIds,
                                  CountDownLatch requireChannelLatch,
                                  CountDownLatch tunnelCloseLatch,
                                  int requesterTimeout) throws Exception {
        // 1. echo 服务
        Channel echoChannel = startEchoServer(group, 0);
        int echoPort = ((InetSocketAddress) echoChannel.localAddress()).getPort();
        ClientServiceInfo clientServiceInfo = new ClientServiceInfo(
                new InetSocketAddress("127.0.0.1", echoPort), TransportLayerProtocol.TCP);

        // 2. 控制通道
        AtomicReference<ControlContext> serverControlContextRef = new AtomicReference<>();

        ProxyServerInfo proxyServerInfo = new ProxyServerInfo();
        proxyServerInfo.setId("test-server");
        proxyServerInfo.setServerName("TestServer");
        proxyServerInfo.setDeploymentMode(DeploymentMode.Single);

        ControlServerListener controlServerListener = new ControlServerListener() {
            @Override
            public void afterAccept(ControlContext context) {
                serverControlContextRef.set(context);
            }
        };

        ControlServer controlServer = new ControlServer(group, group, proxyServerInfo, controlServerListener);
        controlServer.start(new ControlServerStartArgs(opts -> opts
                .port(0)
                .maxConnections(10)
                .pingTimeout(10000))).get();
        int controlPort = ((InetSocketAddress) controlServer.getServerChannel().localAddress()).getPort();

        ProxyClientInfo proxyClientInfo = new ProxyClientInfo();
        proxyClientInfo.setId("test-client");
        proxyClientInfo.setCredentials("test-credentials");

        // 客户端控制事件编排：REQUIRE_CHANNEL 建隧道，TUNNEL_CLOSE 关隧道
        ControlClientListener controlClientListener = new ControlClientListener();
        controlClientListener.addEventHandler(ProxyControlEventEnum.REQUIRE_CHANNEL.getType(), (context, event) -> {
            CommonInfo info = JsonUtil.OBJECT_MAPPER.convertValue(event.getBody(), CommonInfo.class);
            requiredTunnelIds.add(info.getTunnelId());
            requireChannelLatch.countDown();
            proxyTcpClientRef.get().connect(new ProxyClientConnectArgs(
                    info.getTunnelId(), info.getProxyId(), clientServiceInfo.getAddress(),
                    info.getProtocol(), context, opt -> opt));
        });
        controlClientListener.addEventHandler(ProxyControlEventEnum.TUNNEL_CLOSE.getType(), (context, event) -> {
            CommonInfo info = JsonUtil.OBJECT_MAPPER.convertValue(event.getBody(), CommonInfo.class);
            tunnelCloseLatch.countDown();
            ClientTcpTunnelContext tunnel = clientTunnelMap.get(info.getTunnelId());
            if (tunnel != null) {
                tunnel.closeLocal();
            }
        });

        ControlClient controlClient = new ControlClient(group, proxyClientInfo, controlClientListener);

        // 3. 服务端代理 + client-proxy 共享端口
        ProxyTcpServer proxyTcpServer = new ProxyTcpServer(group, group, serverListener);
        proxyTcpServer.start(new ClientProxyServerStartArgs(opt -> opt
                .clientProxyPort(0)
                .enableProxyTcp(true)
                .enableProxyUdp(false))).get();
        int clientProxyPort = ((InetSocketAddress) proxyTcpServer.getClientProxyChannel().localAddress()).getPort();

        // 4. 客户端代理
        ProxyTcpClient proxyTcpClient = new ProxyTcpClient(
                new InetSocketAddress("127.0.0.1", clientProxyPort), group, clientListener);
        proxyTcpClientRef.set(proxyTcpClient);

        // 控制通道建连（REQUIRE_CHANNEL 由此可达客户端）
        ControlClientConnectAbstractArgs connectArgs = new ControlClientConnectAbstractArgs(
                opts -> opts.pingInterval(1000).pingTimeout(10000));
        connectArgs.setServerControlAddress(new InetSocketAddress("127.0.0.1", controlPort));
        controlClient.connect(connectArgs).get();

        // 5. requester-proxy 端口（依赖服务端控制上下文）
        ChannelFuture reqFuture = proxyTcpServer.startTcpRequesterProxyServer(
                new RequesterProxyServerStartArgs(serverControlContextRef.get(),
                        opts -> opts.clientServiceInfo(clientServiceInfo).requesterTimeout(requesterTimeout)));
        reqFuture.sync();
        int requesterPort = ((InetSocketAddress) reqFuture.channel().localAddress()).getPort();

        return new ProxyInfra(echoChannel, controlServer, controlServerListener, controlClient, proxyTcpServer, proxyTcpClient,
                clientProxyPort, requesterPort);
    }

    // connect() 在事件回调中触发，需先拿到 proxyTcpClient 引用
    private final AtomicReference<ProxyTcpClient> proxyTcpClientRef = new AtomicReference<>();

    /** 客户端隧道建立回调：登记隧道并放行 established 闩锁 */
    private static class ClientTunnelTracker extends ProxyClientListener {
        private final Map<String, ClientTcpTunnelContext> clientTunnelMap;
        private final CountDownLatch clientEstablishedLatch;

        ClientTunnelTracker(Map<String, ClientTcpTunnelContext> clientTunnelMap, CountDownLatch clientEstablishedLatch) {
            this.clientTunnelMap = clientTunnelMap;
            this.clientEstablishedLatch = clientEstablishedLatch;
        }

        @Override
        public void onTunnelEstablished(TunnelContext context) {
            clientTunnelMap.put(context.getTunnelId(), (ClientTcpTunnelContext) context);
            clientEstablishedLatch.countDown();
        }

        @Override
        public void onTunnelClose(TunnelContext context) {
            if (context != null) {
                clientTunnelMap.remove(context.getTunnelId());
            }
        }
    }

    // ==================== 测试方法 ====================

    /**
     * 测试基础 TCP 代理隧道全流程：requester 接入 → REQUIRE_CHANNEL → 双端注册 → 数据转发 → 优雅关闭级联
     */
    @Test
    void testBasicTcpProxyTunnel() throws Exception {
        CountDownLatch requireChannelLatch = new CountDownLatch(1);
        CountDownLatch tunnelCloseLatch = new CountDownLatch(1);
        CountDownLatch clientEstablishedLatch = new CountDownLatch(1);
        CountDownLatch serverOnCloseLatch = new CountDownLatch(1);
        CountDownLatch clientOnCloseLatch = new CountDownLatch(1);

        AtomicReference<TunnelContext> serverTunnelContextRef = new AtomicReference<>();
        Map<String, ClientTcpTunnelContext> clientTunnelMap = new ConcurrentHashMap<>();
        List<String> requiredTunnelIds = new CopyOnWriteArrayList<>();

        EventLoopGroup group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        try {
            ProxyServerListener serverListener = new ProxyServerListener() {
                @Override
                public boolean beforeRequesterToServerConnectionAccept(TransportLayerProtocol protocol, Channel serverChannel, Object remoteConnection) {
                    assertEquals(TransportLayerProtocol.TCP, protocol);
                    return true;
                }

                @Override
                public boolean beforeClientToServerConnectionAccept(TransportLayerProtocol protocol, Channel serverChannel, Object remoteConnection) {
                    assertEquals(TransportLayerProtocol.TCP, protocol);
                    return true;
                }

                @Override
                public void onTunnelEstablished(TunnelContext context) {
                    serverTunnelContextRef.compareAndSet(null, context);
                }

                @Override
                public void onTunnelClose(TunnelContext context) {
                    serverOnCloseLatch.countDown();
                }
            };

            ProxyInfra infra = setupInfra(group, serverListener,
                    new ClientTunnelTracker(clientTunnelMap, clientEstablishedLatch),
                    clientTunnelMap, requiredTunnelIds, requireChannelLatch, tunnelCloseLatch, 30000);

            List<String> received = new ArrayList<>();
            Channel requester = startRequesterClient(group, infra.requesterPort, received);

            // REQUIRE_CHANNEL 已下发且客户端隧道已建立
            assertTrue(requireChannelLatch.await(2, TimeUnit.SECONDS),
                    "REQUIRE_CHANNEL should be sent when requester connects");
            assertTrue(clientEstablishedLatch.await(2, TimeUnit.SECONDS),
                    "Client tunnel should be established");

            String tunnelId = requiredTunnelIds.get(0);
            assertNotNull(tunnelId, "tunnelId should not be null");

            Thread.sleep(500);

            requester.writeAndFlush(requester.alloc().buffer().writeBytes("Hello".getBytes(StandardCharsets.UTF_8)));
            Thread.sleep(500);
            assertEquals(1, received.size(), "Should have received one echo response");
            assertEquals("Hello", received.get(0), "Echo response should match sent data");

            requester.writeAndFlush(requester.alloc().buffer().writeBytes("World".getBytes(StandardCharsets.UTF_8)));
            Thread.sleep(500);
            assertEquals(2, received.size(), "Should have received two echo responses");
            assertEquals("World", received.get(1), "Second echo response should match");

            assertNotNull(serverTunnelContextRef.get(), "Server tunnel context should be set");

            // 优雅关闭链：requester 断开 → 服务端隧道销毁并发送 TUNNEL_CLOSE → 客户端隧道销毁
            requester.close().sync();
            assertTrue(serverOnCloseLatch.await(2, TimeUnit.SECONDS),
                    "Server onTunnelClose should be called when requester closes");
            assertTrue(tunnelCloseLatch.await(2, TimeUnit.SECONDS),
                    "Client should have received TUNNEL_CLOSE from server");
            assertTrue(clientOnCloseLatch(clientTunnelMap, 2), "Client tunnel should be disposed");

            requester.closeFuture().sync();
        } finally {
            group.shutdownGracefully().await(2, TimeUnit.SECONDS);
        }
    }

    /**
     * 测试 beforeRequesterToServerConnectionAccept 返回 false 拒绝请求者连接：
     * 不产生隧道、不下发 REQUIRE_CHANNEL
     */
    @Test
    void testRequesterConnectionRejected() throws Exception {
        CountDownLatch requireChannelLatch = new CountDownLatch(1);
        CountDownLatch tunnelCloseLatch = new CountDownLatch(1);
        CountDownLatch clientEstablishedLatch = new CountDownLatch(1);
        CountDownLatch requesterClosedLatch = new CountDownLatch(1);

        Map<String, ClientTcpTunnelContext> clientTunnelMap = new ConcurrentHashMap<>();
        List<String> requiredTunnelIds = new CopyOnWriteArrayList<>();

        EventLoopGroup group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        try {
            ProxyServerListener serverListener = new ProxyServerListener() {
                @Override
                public boolean beforeRequesterToServerConnectionAccept(TransportLayerProtocol protocol, Channel serverChannel, Object remoteConnection) {
                    return false;
                }
            };

            ProxyInfra infra = setupInfra(group, serverListener,
                    new ClientTunnelTracker(clientTunnelMap, clientEstablishedLatch),
                    clientTunnelMap, requiredTunnelIds, requireChannelLatch, tunnelCloseLatch, 30000);

            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new ChannelInboundHandlerAdapter());
                        }
                    });

            Channel requester = bootstrap.connect("127.0.0.1", infra.requesterPort).sync().channel();
            requester.closeFuture().addListener(f -> requesterClosedLatch.countDown());

            assertTrue(requesterClosedLatch.await(2, TimeUnit.SECONDS),
                    "Requester channel should be closed by server");
            assertFalse(requireChannelLatch.await(300, TimeUnit.MILLISECONDS),
                    "REQUIRE_CHANNEL should NOT be sent when requester is rejected");
        } finally {
            group.shutdownGracefully().await(2, TimeUnit.SECONDS);
        }
    }

    /**
     * 测试 beforeClientToServerConnectionAccept 返回 false 拒绝 client-proxy 连接：
     * 隧道注册超时后销毁并关闭 requester 侧
     */
    @Test
    void testClientProxyConnectionRejected() throws Exception {
        CountDownLatch requireChannelLatch = new CountDownLatch(1);
        CountDownLatch tunnelCloseLatch = new CountDownLatch(1);
        CountDownLatch clientEstablishedLatch = new CountDownLatch(1);
        CountDownLatch requesterClosedLatch = new CountDownLatch(1);

        Map<String, ClientTcpTunnelContext> clientTunnelMap = new ConcurrentHashMap<>();
        List<String> requiredTunnelIds = new CopyOnWriteArrayList<>();

        EventLoopGroup group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        try {
            ProxyServerListener serverListener = new ProxyServerListener() {
                @Override
                public boolean beforeClientToServerConnectionAccept(TransportLayerProtocol protocol, Channel serverChannel, Object remoteConnection) {
                    return false;
                }
            };

            // 注册超时调短，加速超时回收路径
            ProxyInfra infra = setupInfra(group, serverListener,
                    new ClientTunnelTracker(clientTunnelMap, clientEstablishedLatch),
                    clientTunnelMap, requiredTunnelIds, requireChannelLatch, tunnelCloseLatch, 800);

            Channel requester = startRequesterClient(group, infra.requesterPort, new ArrayList<>());
            requester.closeFuture().addListener(f -> requesterClosedLatch.countDown());

            // requester 到达 → REQUIRE_CHANNEL 已下发，但 client-proxy 侧被拒，
            // 注册包无法送达，超时后隧道销毁并关闭 requester
            assertTrue(requireChannelLatch.await(2, TimeUnit.SECONDS),
                    "REQUIRE_CHANNEL should be sent when requester connects");
            assertTrue(requesterClosedLatch.await(3, TimeUnit.SECONDS),
                    "Requester should be closed after registration timeout");
        } finally {
            group.shutdownGracefully().await(2, TimeUnit.SECONDS);
        }
    }

    /**
     * 测试多条并发 TCP 代理隧道：隧道间相互独立，单条关闭不影响其余
     */
    @Test
    void testMultipleTunnels() throws Exception {
        int tunnelCount = 3;
        CountDownLatch requireChannelLatch = new CountDownLatch(tunnelCount);
        CountDownLatch tunnelCloseLatch = new CountDownLatch(tunnelCount);
        CountDownLatch clientEstablishedLatch = new CountDownLatch(tunnelCount);

        Map<String, ClientTcpTunnelContext> clientTunnelMap = new ConcurrentHashMap<>();
        List<String> requiredTunnelIds = new CopyOnWriteArrayList<>();

        EventLoopGroup group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        try {
            ProxyServerListener serverListener = new ProxyServerListener() {
                @Override
                public void onTunnelClose(TunnelContext context) {
                    // 仅观察，不参与清理
                }
            };

            ProxyInfra infra = setupInfra(group, serverListener,
                    new ClientTunnelTracker(clientTunnelMap, clientEstablishedLatch),
                    clientTunnelMap, requiredTunnelIds, requireChannelLatch, tunnelCloseLatch, 30000);

            List<Channel> requesters = new ArrayList<>();
            List<List<String>> receivedList = new ArrayList<>();

            for (int i = 0; i < tunnelCount; i++) {
                List<String> received = new ArrayList<>();
                receivedList.add(received);
                requesters.add(startRequesterClient(group, infra.requesterPort, received));
            }

            assertTrue(requireChannelLatch.await(5, TimeUnit.SECONDS),
                    "All " + tunnelCount + " REQUIRE_CHANNEL should be sent");
            assertTrue(clientEstablishedLatch.await(5, TimeUnit.SECONDS),
                    "All " + tunnelCount + " client tunnels should be established");

            assertEquals(tunnelCount, requiredTunnelIds.size(), "Should have " + tunnelCount + " tunnelIds");
            assertEquals(tunnelCount, requiredTunnelIds.stream().distinct().count(),
                    "All tunnelIds should be unique");

            Thread.sleep(1000);

            for (int i = 0; i < tunnelCount; i++) {
                String msg = "Tunnel" + i + "-Data";
                requesters.get(i).writeAndFlush(
                        requesters.get(i).alloc().buffer().writeBytes(msg.getBytes(StandardCharsets.UTF_8)));
            }

            Thread.sleep(1000);

            for (int i = 0; i < tunnelCount; i++) {
                assertEquals(1, receivedList.get(i).size(),
                        "Tunnel " + i + " should have received one response");
                assertEquals("Tunnel" + i + "-Data", receivedList.get(i).get(0),
                        "Tunnel " + i + " echo data should match");
            }

            // 关闭第一条隧道，其余隧道应不受影响
            requesters.get(0).close().sync();
            Thread.sleep(1000);

            for (int i = 1; i < tunnelCount; i++) {
                String msg = "Tunnel" + i + "-AfterClose";
                requesters.get(i).writeAndFlush(
                        requesters.get(i).alloc().buffer().writeBytes(msg.getBytes(StandardCharsets.UTF_8)));
            }

            Thread.sleep(1000);

            for (int i = 1; i < tunnelCount; i++) {
                assertEquals(2, receivedList.get(i).size(),
                        "Tunnel " + i + " should have received two responses");
                assertEquals("Tunnel" + i + "-AfterClose", receivedList.get(i).get(1),
                        "Tunnel " + i + " second echo should match");
            }

            for (int i = 1; i < tunnelCount; i++) {
                requesters.get(i).close().sync();
            }
        } finally {
            group.shutdownGracefully().await(2, TimeUnit.SECONDS);
        }
    }

    /**
     * 测试客户端侧发起的优雅关闭：关闭本地服务连接 → TUNNEL_CLOSE 通知服务端 → 服务端隧道销毁关闭 requester
     */
    @Test
    void testTunnelCloseLifecycle() throws Exception {
        CountDownLatch requireChannelLatch = new CountDownLatch(1);
        CountDownLatch clientTunnelCloseReceivedLatch = new CountDownLatch(1);
        CountDownLatch serverTunnelCloseReceivedLatch = new CountDownLatch(1);
        CountDownLatch clientEstablishedLatch = new CountDownLatch(1);
        CountDownLatch serverOnCloseLatch = new CountDownLatch(1);
        CountDownLatch clientOnCloseLatch = new CountDownLatch(1);

        Map<String, ClientTcpTunnelContext> clientTunnelMap = new ConcurrentHashMap<>();
        Map<String, TunnelContext> serverTunnelMap = new ConcurrentHashMap<>();
        List<String> requiredTunnelIds = new CopyOnWriteArrayList<>();

        EventLoopGroup group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        try {
            ProxyServerListener serverListener = new ProxyServerListener() {
                @Override
                public void onTunnelEstablished(TunnelContext context) {
                    serverTunnelMap.put(context.getTunnelId(), context);
                }

                @Override
                public void onTunnelClose(TunnelContext context) {
                    serverOnCloseLatch.countDown();
                }
            };

            ProxyClientListener clientListener = new ClientTunnelTracker(clientTunnelMap, clientEstablishedLatch) {
                @Override
                public void onTunnelClose(TunnelContext context) {
                    clientOnCloseLatch.countDown();
                    super.onTunnelClose(context);
                }
            };

            ProxyInfra infra = setupInfra(group, serverListener, clientListener,
                    clientTunnelMap, requiredTunnelIds, requireChannelLatch, clientTunnelCloseReceivedLatch, 30000);

            // 服务端控制事件编排：TUNNEL_CLOSE → 销毁对应服务端隧道（应用侧职责，与客户端对称）
            infra.controlServerListener.addEventHandler(ProxyControlEventEnum.TUNNEL_CLOSE.getType(), (context, event) -> {
                CommonInfo info = JsonUtil.OBJECT_MAPPER.convertValue(event.getBody(), CommonInfo.class);
                serverTunnelCloseReceivedLatch.countDown();
                TunnelContext tunnel = serverTunnelMap.get(info.getTunnelId());
                if (tunnel != null) {
                    tunnel.closeLocal();
                }
            });

            List<String> received = new ArrayList<>();
            Channel requester = startRequesterClient(group, infra.requesterPort, received);

            assertTrue(clientEstablishedLatch.await(2, TimeUnit.SECONDS),
                    "Client tunnel should be established");

            Thread.sleep(500);
            requester.writeAndFlush(requester.alloc().buffer().writeBytes("BeforeClose".getBytes(StandardCharsets.UTF_8)));
            Thread.sleep(500);
            assertEquals(1, received.size(), "Should receive echo before close");
            assertEquals("BeforeClose", received.get(0));

            // 客户端侧发起关闭：断开本地服务连接
            ClientTcpTunnelContext clientTunnel = clientTunnelMap.get(requiredTunnelIds.get(0));
            assertNotNull(clientTunnel, "Client tunnel should exist");
            clientTunnel.getServiceChannel().close().sync();

            // 关闭链：客户端 onTunnelClose → TUNNEL_CLOSE → 服务端隧道销毁 → requester 断开 → 服务端 onTunnelClose
            assertTrue(clientOnCloseLatch.await(2, TimeUnit.SECONDS),
                    "Client onTunnelClose should be called when service channel closes");
            assertTrue(serverTunnelCloseReceivedLatch.await(2, TimeUnit.SECONDS),
                    "Server should have received TUNNEL_CLOSE from client");
            assertTrue(serverOnCloseLatch.await(2, TimeUnit.SECONDS),
                    "Server onTunnelClose should be called after tunnel disposal");
        } finally {
            group.shutdownGracefully().await(2, TimeUnit.SECONDS);
        }
    }

    /** 轮询等待客户端隧道表清空（隧道销毁） */
    private static boolean clientOnCloseLatch(Map<String, ClientTcpTunnelContext> clientTunnelMap, int timeoutSeconds) {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        while (System.currentTimeMillis() < deadline) {
            if (clientTunnelMap.isEmpty()) {
                return true;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return clientTunnelMap.isEmpty();
    }
}
