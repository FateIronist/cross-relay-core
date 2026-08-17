package top.fateironist.cross_relay_core;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import top.fateironist.cross_relay_core.control.ControlClient;
import top.fateironist.cross_relay_core.control.ControlServer;
import top.fateironist.cross_relay_core.control.listener.ControlClientListener;
import top.fateironist.cross_relay_core.control.listener.ControlServerListener;
import top.fateironist.cross_relay_core.model.DeploymentMode;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.control.ControlEvent;
import top.fateironist.cross_relay_core.model.control.ControlProtocolEventEnum;
import top.fateironist.cross_relay_core.model.info.ProxyClientInfo;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;
import top.fateironist.cross_relay_core.model.options.control.ControlClientConnectOptions;
import top.fateironist.cross_relay_core.model.options.control.ControlServerStartOptions;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ControlServer + ControlClient 集成测试
 * 测试完整的控制通道生命周期：连接建立 → RSA+AES密钥交换 → 加密通信 → PING/PONG
 */
class ControlTest {

    @BeforeAll
    static void setupLogging() {
        LoggerContext ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
        ctx.getLogger(ControlClient.class).setLevel(Level.DEBUG);
        ctx.getLogger(ControlServer.class).setLevel(Level.DEBUG);
    }

    /**
     * 测试完整的控制通道连接和加密握手流程：
     * 1. 启动 ControlServer
     * 2. ControlClient 连接
     * 3. RSA公钥交换 → AES密钥协商
     * 4. 验证加密通道建立成功（controlChannelId、proxyServerInfo）
     * 5. 验证消息收发（CLIENT_INFO → SERVER_INFO）
     * 6. 验证 PING/PONG 心跳（latency >= 0）
     */
    @Test
    void testControlConnectionAndHandshake() throws Exception {
        CountDownLatch serverAfterAcceptLatch = new CountDownLatch(1);
        CountDownLatch serverPermitLatch = new CountDownLatch(1);
        CountDownLatch clientPermitLatch = new CountDownLatch(1);
        CountDownLatch clientEncryptedLatch = new CountDownLatch(1);
        CountDownLatch serverMessageLatch = new CountDownLatch(1);
        CountDownLatch clientMessageLatch = new CountDownLatch(1);

        AtomicReference<ControlContext> serverContextRef = new AtomicReference<>();
        AtomicReference<ControlContext> clientContextRef = new AtomicReference<>();

        EventLoopGroup serverBossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        EventLoopGroup serverWorkerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        EventLoopGroup clientWorkerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        try {
            // ========== 启动 ControlServer ==========
            ProxyServerInfo proxyServerInfo = new ProxyServerInfo();
            proxyServerInfo.setId("test-server");
            proxyServerInfo.setServerName("TestServer");
            proxyServerInfo.setDeploymentMode(DeploymentMode.Single);

            ControlServerListener serverListener = new ControlServerListener() {
                @Override
                public void afterAccept(ControlContext context) {
                    serverAfterAcceptLatch.countDown();
                }

                @Override
                public boolean beforePermit(ControlContext context, ControlEvent<Map<String, Object>> event) {
                    serverPermitLatch.countDown();
                    return true;
                }
            };
            serverListener.addEventHandler(ControlProtocolEventEnum.CLIENT_INFO.getType(), (context, event) -> {
                serverContextRef.set(context);
                serverMessageLatch.countDown();
                context.writeAndFlush(new ControlEvent<>(ControlProtocolEventEnum.SERVER_INFO.getType(), Map.of("status", "ok")));
            });

            ControlServer controlServer = new ControlServer(serverBossGroup, serverWorkerGroup, proxyServerInfo, serverListener);
            ControlServerStartOptions serverOptions = ControlServerStartOptions.builder()
                    .port(0)
                    .maxConnections(10)
                    .pingTimeout(1000)
                    .build();

            ChannelFuture serverFuture = controlServer.start(serverOptions);
            serverFuture.sync();
            int actualPort = ((InetSocketAddress) serverFuture.channel().localAddress()).getPort();

            // ========== 启动 ControlClient ==========
            ProxyClientInfo proxyClientInfo = new ProxyClientInfo();
            proxyClientInfo.setId("test-client");
            proxyClientInfo.setCredentials("test-credentials");

            ControlClientListener clientListener = new ControlClientListener() {
                @Override
                public void afterPermit(ControlContext context) {
                    clientContextRef.set(context);
                    clientPermitLatch.countDown();
                    clientEncryptedLatch.countDown();
                }
            };
            clientListener.addEventHandler(ControlProtocolEventEnum.SERVER_INFO.getType(), (context, event) -> {
                clientMessageLatch.countDown();
            });

            ControlClient controlClient = new ControlClient(clientWorkerGroup, proxyClientInfo, clientListener);
            ControlClientConnectOptions clientOptions = ControlClientConnectOptions.builder()
                    .address(new InetSocketAddress("127.0.0.1", actualPort))
                    .pingInterval(50)
                    .pingTimeout(1000)
                    .build();

            controlClient.connect(clientOptions).get();

            // ========== 验证阶段 ==========

            // 1. 验证 server accept 了连接
            assertTrue(serverAfterAcceptLatch.await(1, TimeUnit.SECONDS),
                    "Server should have accepted the connection");

            // 2. 验证加密握手完成（用Latch替代Thread.sleep）
            assertTrue(clientEncryptedLatch.await(1, TimeUnit.SECONDS),
                    "Client should have completed encryption handshake");

            // 3. 验证 server 端 permit 阶段被触发
            assertTrue(serverPermitLatch.await(1, TimeUnit.SECONDS),
                    "Server should have reached permit phase");

            // 4. 验证 client 端 permit
            assertTrue(clientPermitLatch.await(1, TimeUnit.SECONDS),
                    "Client should have been permitted");

            // 5. 验证 controlChannelId 和 proxyServerInfo 已设置
            ControlContext clientCtx = clientContextRef.get();
            assertNotNull(clientCtx, "Client context should exist");
            assertNotNull(clientCtx.getControlChannelId(), "Client controlChannelId should be set after permit");
            assertNotNull(clientCtx.getProxyServerInfo().getServerName(),
                    "Client proxyServerInfo should be populated from server");

            // 6. 验证消息收发：client → server → client
            clientCtx.writeAndFlush(new ControlEvent<>(ControlProtocolEventEnum.CLIENT_INFO.getType(), Map.of("info", "hello")));
            assertTrue(serverMessageLatch.await(1, TimeUnit.SECONDS),
                    "Server should have received CLIENT_INFO");
            assertTrue(clientMessageLatch.await(1, TimeUnit.SECONDS),
                    "Client should have received SERVER_INFO reply");

            // 7. 验证 PING/PONG（pingInterval=50ms，轮询等待latency被设置）
            long deadline = System.currentTimeMillis() + 1000;
            while (clientCtx.getProxyServerInfo().getLatency() == null && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertNotNull(clientCtx.getProxyServerInfo().getLatency(),
                    "Latency should be recorded after PONG");
            assertTrue(clientCtx.getProxyServerInfo().getLatency() >= 0,
                    "Latency should be non-negative");

            // ========== 清理 ==========
            controlClient.close();
            controlServer.shutdown();
        } finally {
            shutdownGroup(clientWorkerGroup);
        }
    }

    /**
     * 测试服务端拒绝连接（beforePermit 返回 false）：
     * 1. 握手完成后 beforePermit 拒绝
     * 2. 客户端收到 deny 回调
     * 3. 连接被关闭
     */
    @Test
    void testConnectionDeniedByServer() throws Exception {
        CountDownLatch clientDenyLatch = new CountDownLatch(1);
        CountDownLatch clientCloseLatch = new CountDownLatch(1);

        EventLoopGroup serverBossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        EventLoopGroup serverWorkerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        EventLoopGroup clientWorkerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        try {
            ProxyServerInfo proxyServerInfo = new ProxyServerInfo();
            proxyServerInfo.setId("test-server-deny");

            ControlServerListener serverListener = new ControlServerListener() {
                @Override
                public boolean beforePermit(ControlContext context, ControlEvent<Map<String, Object>> event) {
                    return false;
                }
            };

            ControlServer controlServer = new ControlServer(serverBossGroup, serverWorkerGroup, proxyServerInfo, serverListener);
            ControlServerStartOptions serverOptions = ControlServerStartOptions.builder()
                    .port(0)
                    .maxConnections(10)
                    .pingTimeout(1000)
                    .build();

            ChannelFuture serverFuture = controlServer.start(serverOptions);
            serverFuture.sync();
            int actualPort = ((InetSocketAddress) serverFuture.channel().localAddress()).getPort();

            ProxyClientInfo proxyClientInfo = new ProxyClientInfo();
            proxyClientInfo.setId("test-client-deny");

            ControlClientListener clientListener = new ControlClientListener() {
                @Override
                public void onDeny(ControlContext context) {
                    clientDenyLatch.countDown();
                }

                @Override
                public void onClose(ControlContext context) {
                    clientCloseLatch.countDown();
                }
            };

            ControlClient controlClient = new ControlClient(clientWorkerGroup, proxyClientInfo, clientListener);
            ControlClientConnectOptions clientOptions = ControlClientConnectOptions.builder()
                    .address(new InetSocketAddress("127.0.0.1", actualPort))
                    .pingInterval(100)
                    .pingTimeout(1000)
                    .build();

            controlClient.connect(clientOptions).get();

            // 验证客户端被拒绝
            assertTrue(clientDenyLatch.await(1, TimeUnit.SECONDS),
                    "Client should have been denied by server");
            assertTrue(clientCloseLatch.await(1, TimeUnit.SECONDS),
                    "Client connection should have been closed after deny");

            controlServer.shutdown();
        } finally {
            shutdownGroup(clientWorkerGroup);
        }
    }

    /**
     * 测试 beforeConnect 拒绝连接（IP封禁等场景）：
     * 1. TCP连接建立后被服务端拒绝
     * 2. afterAccept 不应被调用
     * 3. 客户端连接应被关闭
     */
    @Test
    void testBeforeConnectRejection() throws Exception {
        CountDownLatch afterAcceptLatch = new CountDownLatch(1);
        CountDownLatch clientCloseLatch = new CountDownLatch(1);

        EventLoopGroup serverBossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        EventLoopGroup serverWorkerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        EventLoopGroup clientWorkerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        try {
            ProxyServerInfo proxyServerInfo = new ProxyServerInfo();
            proxyServerInfo.setId("test-server-reject");

            ControlServerListener serverListener = new ControlServerListener() {
                @Override
                public boolean beforeAccept(Channel channel) {
                    return false;
                }

                @Override
                public void afterAccept(ControlContext context) {
                    afterAcceptLatch.countDown();
                }
            };

            ControlServer controlServer = new ControlServer(serverBossGroup, serverWorkerGroup, proxyServerInfo, serverListener);
            ControlServerStartOptions serverOptions = ControlServerStartOptions.builder()
                    .port(0)
                    .maxConnections(10)
                    .pingTimeout(1000)
                    .build();

            ChannelFuture serverFuture = controlServer.start(serverOptions);
            serverFuture.sync();
            int actualPort = ((InetSocketAddress) serverFuture.channel().localAddress()).getPort();

            ProxyClientInfo proxyClientInfo = new ProxyClientInfo();
            proxyClientInfo.setId("test-client-reject");
            ControlClientListener clientListener = new ControlClientListener() {
                @Override
                public void onClose(ControlContext context) {
                    clientCloseLatch.countDown();
                }
            };

            ControlClient controlClient = new ControlClient(clientWorkerGroup, proxyClientInfo, clientListener);
            ControlClientConnectOptions clientOptions = ControlClientConnectOptions.builder()
                    .address(new InetSocketAddress("127.0.0.1", actualPort))
                    .pingInterval(100)
                    .pingTimeout(1000)
                    .build();

            CompletableFuture.runAsync(() -> {
                try {
                    controlClient.connect(clientOptions).get();
                } catch (Exception ignored) {
                }
            });

            // afterAccept 不应被调用（连接在 beforeConnect 阶段被拒绝）
            assertFalse(afterAcceptLatch.await(500, TimeUnit.MILLISECONDS),
                    "afterAccept should NOT have been called when beforeConnect returns false");

            // 客户端连接应被服务端关闭
            assertTrue(clientCloseLatch.await(1, TimeUnit.SECONDS),
                    "Client should have been closed by server");

            controlServer.shutdown();
        } finally {
            shutdownGroup(clientWorkerGroup);
        }
    }

    private static void shutdownGroup(EventLoopGroup group) throws Exception {
        group.shutdownGracefully().await(1, TimeUnit.SECONDS);
    }
}
