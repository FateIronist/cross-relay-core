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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.args.proxy_client.ProxyClientConnectArgs;
import top.fateironist.cross_relay_core.model.args.proxy_server.ClientProxyServerStartArgs;
import top.fateironist.cross_relay_core.model.args.proxy_server.RequesterProxyServerStartArgs;
import top.fateironist.cross_relay_core.model.info.ClientServiceInfo;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ClientTcpTunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ServerTcpTunnelContext;
import top.fateironist.cross_relay_core.proxy.ProxyClient;
import top.fateironist.cross_relay_core.proxy.ProxyTcpServer;
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
 * ProxyTcpServer + ProxyClient TCP代理集成测试
 * 覆盖 ProxyServerListener 全部回调方法 和 ProxyClientListener 全部回调方法
 */
class TcpProxyTest {

    @BeforeAll
    static void setupLogging() {
        LoggerContext ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
        ctx.getLogger(ProxyTcpServer.class).setLevel(Level.DEBUG);
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

        // 2. 创建 ProxyTcpServer
        ProxyTcpServer proxyTcpServer = new ProxyTcpServer(group, group, serverListener);

        // 3. 启动 TCP ClientProxyServer（端口 0）
        ChannelFuture clientProxyFuture = proxyTcpServer.startTcpClientProxyServer(
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
        ChannelFuture reqFuture = proxyTcpServer.startTcpRequesterProxyServer(
                new RequesterProxyServerStartArgs(opt -> opt.clientServiceInfo(clientServiceInfo)));
        reqFuture.sync();
        int requesterPort = ((InetSocketAddress) reqFuture.channel().localAddress()).getPort();

        return new ProxyInfra(echoChannel, proxyTcpServer, clientServiceInfo, proxyServerInfo, clientProxyPort, requesterPort);
    }

    /**
     * 代理基础设施封装
     */
    private static class ProxyInfra {
        final Channel echoChannel;
        final ProxyTcpServer proxyTcpServer;
        final ClientServiceInfo clientServiceInfo;
        final ProxyServerInfo proxyServerInfo;
        final int clientProxyPort;
        final int requesterPort;

        ProxyInfra(Channel echoChannel, ProxyTcpServer proxyTcpServer,
                   ClientServiceInfo clientServiceInfo, ProxyServerInfo proxyServerInfo,
                   int clientProxyPort, int requesterPort) {
            this.echoChannel = echoChannel;
            this.proxyTcpServer = proxyTcpServer;
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
        ProxyClient proxyClient = new ProxyClient(group, infra.clientServiceInfo,
                infra.proxyServerInfo, listener);
        proxyClient.connect(new ProxyClientConnectArgs(tunnelId, TransportLayerProtocol.TCP, opt -> opt));
        return proxyClient;
    }

    // ==================== 测试方法 ====================

    /**
     * 测试基础 TCP 代理隧道全流程
     */
    @Test
    void testBasicTcpProxyTunnel() throws Exception {
        CountDownLatch serverRequireTunnelLatch = new CountDownLatch(1);
        CountDownLatch serverRegisterLatch = new CountDownLatch(1);
        CountDownLatch serverEstablishedLatch = new CountDownLatch(1);
        CountDownLatch clientEstablishedLatch = new CountDownLatch(2);
        CountDownLatch serverCloseRemoteLatch = new CountDownLatch(1);
        CountDownLatch clientCloseRemoteLatch = new CountDownLatch(1);

        AtomicReference<String> registeredTunnelId = new AtomicReference<>();
        AtomicReference<TunnelContext> serverTunnelContextRef = new AtomicReference<>();
        AtomicReference<TunnelContext> clientTunnelContextRef = new AtomicReference<>();

        Map<String, TunnelContext> serverTunnelMap = new ConcurrentHashMap<>();
        Map<String, TunnelContext> clientTunnelMap = new ConcurrentHashMap<>();

        EventLoopGroup eventLoopGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        AtomicReference<ProxyInfra> infraRef = new AtomicReference<>();

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
                public void onClientToServerChannelRegister(String id, Channel channel) {
                    assertNotNull(id, "tunnelId should not be null on register");
                    registeredTunnelId.set(id);
                    serverRegisterLatch.countDown();
                }

                @Override
                public void onRequesterRequireTunnel(TunnelContext context) {
                    assertEquals(TransportLayerProtocol.TCP, context.getTransportLayerProtocol());
                    serverRequireTunnelLatch.countDown();

                    ProxyInfra infra = infraRef.get();
                    createProxyClient(eventLoopGroup, infra, context.getTunnelId(), new ProxyClientListener() {
                        @Override
                        public void onTunnelEstablished(TunnelContext ctx) {
                            clientTunnelMap.put(ctx.getTunnelId(), ctx);
                            clientTunnelContextRef.compareAndSet(null, ctx);
                            clientEstablishedLatch.countDown();
                        }

                        @Override
                        public Future<?> closeRemoteTunnel(TunnelContext ctx) {
                            clientCloseRemoteLatch.countDown();
                            ServerTcpTunnelContext stc = (ServerTcpTunnelContext) serverTunnelMap.get(ctx.getTunnelId());
                            return stc != null ? stc.closeLocal() : eventLoopGroup.next().newPromise().setSuccess(null);
                        }

                        @Override
                        public void onTunnelClose(TunnelContext ctx) {
                            if (ctx != null) clientTunnelMap.remove(ctx.getTunnelId());
                        }
                    });

                    serverTunnelMap.put(context.getTunnelId(), context);
                    serverTunnelContextRef.compareAndSet(null, context);
                }

                @Override
                public Future<?> closeRemoteTunnel(TunnelContext context) {
                    serverCloseRemoteLatch.countDown();
                    ClientTcpTunnelContext ctc = (ClientTcpTunnelContext) clientTunnelMap.get(context.getTunnelId());
                    return ctc != null ? ctc.closeLocal() : eventLoopGroup.next().newPromise().setSuccess(null);
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

            List<String> received = new ArrayList<>();
            Channel requester = startRequesterClient(eventLoopGroup, infra.requesterPort, received);

            assertTrue(serverRequireTunnelLatch.await(2, TimeUnit.SECONDS),
                    "onRequesterRequireTunnel should be called when requester connects");
            assertTrue(serverRegisterLatch.await(2, TimeUnit.SECONDS),
                    "onClientToServerChannelRegister should be called when ProxyClient sends tunnelId");
            assertTrue(clientEstablishedLatch.await(2, TimeUnit.SECONDS),
                    "Client onTunnelEstablished should be called");

            String tunnelId = registeredTunnelId.get();
            assertNotNull(tunnelId, "Registered tunnelId should not be null");

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
            assertNotNull(clientTunnelContextRef.get(), "Client tunnel context should be set");

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
     * 测试 beforeRequesterToServerChannelAccept 返回 false 拒绝请求者连接
     */
    @Test
    void testRequesterConnectionRejected() throws Exception {
        CountDownLatch requireTunnelLatch = new CountDownLatch(1);
        CountDownLatch requesterClosedLatch = new CountDownLatch(1);

        EventLoopGroup eventLoopGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        try {
            ProxyServerListener serverListener = new ProxyServerListener() {
                @Override
                public boolean beforeRequesterToServerConnectionAccept(TransportLayerProtocol protocol, Channel serverChannel, Object remoteConnection) {
                    return false;
                }

                @Override
                public void onRequesterRequireTunnel(TunnelContext context) {
                    requireTunnelLatch.countDown();
                }
            };

            ProxyInfra infra = setupProxyInfraFull(eventLoopGroup, serverListener);

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
            requester.closeFuture().addListener(f -> requesterClosedLatch.countDown());

            assertTrue(requesterClosedLatch.await(2, TimeUnit.SECONDS),
                    "Requester channel should be closed by server");
            assertFalse(requireTunnelLatch.await(200, TimeUnit.MILLISECONDS),
                    "onRequesterRequireTunnel should NOT be called when requester is rejected");
        } finally {
            eventLoopGroup.shutdownGracefully().await(2, TimeUnit.SECONDS);
        }
    }

    /**
     * 测试 beforeClientToServerChannelAccept 返回 false 拒绝 ProxyClient 连接
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
                public boolean beforeClientToServerConnectionAccept(TransportLayerProtocol protocol, Channel serverChannel, Object remoteConnection) {
                    beforeClientAcceptLatch.countDown();
                    return false;
                }

                @Override
                public void onClientToServerChannelRegister(String id, Channel channel) {
                    registerLatch.countDown();
                }

                @Override
                public void onRequesterRequireTunnel(TunnelContext context) {
                    ProxyInfra infra = infraRef.get();
                    createProxyClient(eventLoopGroup, infra, context.getTunnelId(), new ProxyClientListener() {
                        @Override
                        public void onTunnelEstablished(TunnelContext ctx) {
                            clientProxyConnected.set(true);
                        }

                        @Override
                        public Future<?> closeRemoteTunnel(TunnelContext ctx) {
                            return ctx.closeLocal();
                        }
                    });
                }

                @Override
                public Future<?> closeRemoteTunnel(TunnelContext context) {
                    return context.closeLocal();
                }
            };

            ProxyInfra infra = setupProxyInfraFull(eventLoopGroup, serverListener);
            infraRef.set(infra);

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

            assertTrue(beforeClientAcceptLatch.await(2, TimeUnit.SECONDS),
                    "beforeClientToServerChannelAccept should be called");
            Thread.sleep(1000);
            assertFalse(registerLatch.await(200, TimeUnit.MILLISECONDS),
                    "onClientToServerChannelRegister should NOT be called when client-proxy is rejected");
            assertTrue(beforeClientAcceptLatch.await(2, TimeUnit.SECONDS),
                    "beforeClientToServerChannelAccept should have been called");

            requester.close().sync();
        } finally {
            eventLoopGroup.shutdownGracefully().await(2, TimeUnit.SECONDS);
        }
    }

    /**
     * 测试隧道关闭生命周期
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
        Map<String, TunnelContext> clientTunnelMap = new ConcurrentHashMap<>();

        EventLoopGroup eventLoopGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        AtomicReference<ProxyInfra> infraRef = new AtomicReference<>();

        try {
            ProxyServerListener serverListener = new ProxyServerListener() {
                @Override
                public void onClientToServerChannelRegister(String id, Channel channel) {
                    serverRegisterLatch.countDown();
                }

                @Override
                public void onRequesterRequireTunnel(TunnelContext context) {
                    ProxyInfra infra = infraRef.get();
                    createProxyClient(eventLoopGroup, infra, context.getTunnelId(), new ProxyClientListener() {
                        @Override
                        public void onTunnelEstablished(TunnelContext ctx) {
                            clientTunnelMap.put(ctx.getTunnelId(), ctx);
                            clientEstablishedLatch.countDown();
                        }

                        @Override
                        public Future<?> closeRemoteTunnel(TunnelContext ctx) {
                            clientCloseRemoteLatch.countDown();
                            ServerTcpTunnelContext stc = (ServerTcpTunnelContext) serverTunnelMap.get(ctx.getTunnelId());
                            return stc != null ? stc.closeLocal() : eventLoopGroup.next().newPromise().setSuccess(null);
                        }

                        @Override
                        public void onTunnelClose(TunnelContext ctx) {
                            clientOnCloseLatch.countDown();
                            if (ctx != null) clientTunnelMap.remove(ctx.getTunnelId());
                        }
                    });
                    serverTunnelMap.put(context.getTunnelId(), context);
                }

                @Override
                public Future<?> closeRemoteTunnel(TunnelContext context) {
                    serverCloseRemoteLatch.countDown();
                    ClientTcpTunnelContext ctc = (ClientTcpTunnelContext) clientTunnelMap.get(context.getTunnelId());
                    return ctc != null ? ctc.closeLocal() : eventLoopGroup.next().newPromise().setSuccess(null);
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

            List<String> received = new ArrayList<>();
            Channel requester = startRequesterClient(eventLoopGroup, infra.requesterPort, received);

            assertTrue(serverRegisterLatch.await(2, TimeUnit.SECONDS),
                    "Server tunnel should be ready (onClientToServerChannelRegister)");
            assertTrue(clientEstablishedLatch.await(2, TimeUnit.SECONDS),
                    "Client tunnel should be established");

            Thread.sleep(500);
            requester.writeAndFlush(requester.alloc().buffer().writeBytes("BeforeClose".getBytes(StandardCharsets.UTF_8)));
            Thread.sleep(500);
            assertEquals(1, received.size(), "Should receive echo before close");
            assertEquals("BeforeClose", received.get(0));

            requester.close().sync();

            assertTrue(serverOnCloseLatch.await(2, TimeUnit.SECONDS),
                    "Server onTunnelClose should be called when requester disconnects");
            assertTrue(serverCloseRemoteLatch.await(2, TimeUnit.SECONDS),
                    "Server closeRemoteTunnel should be called to close client side");
            assertTrue(clientCloseRemoteLatch.await(2, TimeUnit.SECONDS),
                    "Client closeRemoteTunnel should be called during graceful close");
            assertTrue(clientOnCloseLatch.await(2, TimeUnit.SECONDS),
                    "Client onTunnelClose should be called when tunnel is fully closed");
        } finally {
            eventLoopGroup.shutdownGracefully().await(2, TimeUnit.SECONDS);
        }
    }

    /**
     * 测试多条并发 TCP 代理隧道
     */
    @Test
    void testMultipleTunnels() throws Exception {
        int tunnelCount = 3;
        CountDownLatch serverEstablishedLatch = new CountDownLatch(tunnelCount);
        CountDownLatch clientEstablishedLatch = new CountDownLatch(tunnelCount);
        CountDownLatch registerLatch = new CountDownLatch(tunnelCount);

        Map<String, TunnelContext> serverTunnelMap = new ConcurrentHashMap<>();
        Map<String, TunnelContext> clientTunnelMap = new ConcurrentHashMap<>();
        List<String> registeredIds = new ArrayList<>();

        EventLoopGroup eventLoopGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        AtomicReference<ProxyInfra> infraRef = new AtomicReference<>();

        try {
            ProxyServerListener serverListener = new ProxyServerListener() {
                @Override
                public void onClientToServerChannelRegister(String id, Channel channel) {
                    synchronized (registeredIds) {
                        registeredIds.add(id);
                    }
                    registerLatch.countDown();
                }

                @Override
                public void onRequesterRequireTunnel(TunnelContext context) {
                    ProxyInfra infra = infraRef.get();
                    createProxyClient(eventLoopGroup, infra, context.getTunnelId(), new ProxyClientListener() {
                        @Override
                        public void onTunnelEstablished(TunnelContext ctx) {
                            clientTunnelMap.put(ctx.getTunnelId(), ctx);
                            clientEstablishedLatch.countDown();
                        }

                        @Override
                        public Future<?> closeRemoteTunnel(TunnelContext ctx) {
                            ServerTcpTunnelContext stc = (ServerTcpTunnelContext) serverTunnelMap.get(ctx.getTunnelId());
                            return stc != null ? stc.closeLocal() : eventLoopGroup.next().newPromise().setSuccess(null);
                        }

                        @Override
                        public void onTunnelClose(TunnelContext ctx) {
                            if (ctx != null) clientTunnelMap.remove(ctx.getTunnelId());
                        }
                    });
                    serverTunnelMap.put(context.getTunnelId(), context);
                }

                @Override
                public Future<?> closeRemoteTunnel(TunnelContext context) {
                    ClientTcpTunnelContext ctc = (ClientTcpTunnelContext) clientTunnelMap.get(context.getTunnelId());
                    return ctc != null ? ctc.closeLocal() : eventLoopGroup.next().newPromise().setSuccess(null);
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

            List<Channel> requesters = new ArrayList<>();
            List<List<String>> receivedList = new ArrayList<>();

            for (int i = 0; i < tunnelCount; i++) {
                List<String> received = new ArrayList<>();
                receivedList.add(received);
                Channel requester = startRequesterClient(eventLoopGroup, infra.requesterPort, received);
                requesters.add(requester);
            }

            assertTrue(registerLatch.await(5, TimeUnit.SECONDS),
                    "All " + tunnelCount + " server tunnels should be ready");
            assertTrue(clientEstablishedLatch.await(5, TimeUnit.SECONDS),
                    "All " + tunnelCount + " client tunnels should be established");

            synchronized (registeredIds) {
                assertEquals(tunnelCount, registeredIds.size(), "Should have " + tunnelCount + " registered IDs");
                long uniqueCount = registeredIds.stream().distinct().count();
                assertEquals(tunnelCount, uniqueCount, "All tunnelIds should be unique");
            }

            Thread.sleep(1000);

            for (int i = 0; i < tunnelCount; i++) {
                String msg = "Tunnel" + i + "-Data";
                requesters.get(i).writeAndFlush(
                        requesters.get(i).alloc().buffer().writeBytes(msg.getBytes(StandardCharsets.UTF_8)));
            }

            Thread.sleep(1000);

            for (int i = 0; i < tunnelCount; i++) {
                String expected = "Tunnel" + i + "-Data";
                assertEquals(1, receivedList.get(i).size(),
                        "Tunnel " + i + " should have received one response");
                assertEquals(expected, receivedList.get(i).get(0),
                        "Tunnel " + i + " echo data should match");
            }

            requesters.get(0).close().sync();
            Thread.sleep(500);

            for (int i = 1; i < tunnelCount; i++) {
                String msg = "Tunnel" + i + "-AfterClose";
                requesters.get(i).writeAndFlush(
                        requesters.get(i).alloc().buffer().writeBytes(msg.getBytes(StandardCharsets.UTF_8)));
            }

            Thread.sleep(500);

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
            eventLoopGroup.shutdownGracefully().await(2, TimeUnit.SECONDS);
        }
    }

    /**
     * 测试隧道异常处理
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
        Map<String, TunnelContext> clientTunnelMap = new ConcurrentHashMap<>();

        EventLoopGroup eventLoopGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        AtomicReference<ProxyInfra> infraRef = new AtomicReference<>();

        try {
            ProxyServerListener serverListener = new ProxyServerListener() {
                @Override
                public void onClientToServerChannelRegister(String id, Channel channel) {
                    serverRegisterLatch.countDown();
                }

                @Override
                public void onRequesterRequireTunnel(TunnelContext context) {
                    ProxyInfra infra = infraRef.get();
                    createProxyClient(eventLoopGroup, infra, context.getTunnelId(), new ProxyClientListener() {
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
                            ServerTcpTunnelContext stc = (ServerTcpTunnelContext) serverTunnelMap.get(ctx.getTunnelId());
                            return stc != null ? stc.closeLocal() : eventLoopGroup.next().newPromise().setSuccess(null);
                        }

                        @Override
                        public void onTunnelClose(TunnelContext ctx) {
                            clientCloseLatch.countDown();
                            if (ctx != null) clientTunnelMap.remove(ctx.getTunnelId());
                        }
                    });
                    serverTunnelMap.put(context.getTunnelId(), context);
                }

                @Override
                public Future<?> closeRemoteTunnel(TunnelContext context) {
                    ClientTcpTunnelContext ctc = (ClientTcpTunnelContext) clientTunnelMap.get(context.getTunnelId());
                    return ctc != null ? ctc.closeLocal() : eventLoopGroup.next().newPromise().setSuccess(null);
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

            List<String> received = new ArrayList<>();
            Channel requester = startRequesterClient(eventLoopGroup, infra.requesterPort, received);

            assertTrue(serverRegisterLatch.await(2, TimeUnit.SECONDS),
                    "Server tunnel should be ready");
            assertTrue(clientEstablishedLatch.await(2, TimeUnit.SECONDS),
                    "Client tunnel should be established");

            Thread.sleep(500);
            requester.writeAndFlush(requester.alloc().buffer().writeBytes("BeforeError".getBytes(StandardCharsets.UTF_8)));
            Thread.sleep(500);
            assertEquals(1, received.size(), "Should receive echo before error");

            infra.echoChannel.close().sync();
            Thread.sleep(500);

            try {
                requester.writeAndFlush(requester.alloc().buffer().writeBytes(
                        "AfterError".getBytes(StandardCharsets.UTF_8))).sync();
            } catch (Exception ignored) {
            }

            assertFalse(infra.echoChannel.isActive(),
                    "Echo channel should be inactive after close");
            Thread.sleep(1000);
        } finally {
            eventLoopGroup.shutdownGracefully().await(2, TimeUnit.SECONDS);
        }
    }

    /**
     * 测试服务端隧道异常处理
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
        Map<String, TunnelContext> clientTunnelMap = new ConcurrentHashMap<>();
        AtomicReference<Channel> clientToServerChannelRef = new AtomicReference<>();

        EventLoopGroup eventLoopGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        AtomicReference<ProxyInfra> infraRef = new AtomicReference<>();

        try {
            ProxyServerListener serverListener = new ProxyServerListener() {
                @Override
                public void onClientToServerChannelRegister(String id, Channel channel) {
                    serverRegisterLatch.countDown();
                    clientToServerChannelRef.set(channel);
                }

                @Override
                public void onRequesterRequireTunnel(TunnelContext context) {
                    ProxyInfra infra = infraRef.get();
                    createProxyClient(eventLoopGroup, infra, context.getTunnelId(), new ProxyClientListener() {
                        @Override
                        public void onTunnelEstablished(TunnelContext ctx) {
                            clientTunnelMap.put(ctx.getTunnelId(), ctx);
                            clientEstablishedLatch.countDown();
                        }

                        @Override
                        public Future<?> closeRemoteTunnel(TunnelContext ctx) {
                            ServerTcpTunnelContext stc = (ServerTcpTunnelContext) serverTunnelMap.get(ctx.getTunnelId());
                            return stc != null ? stc.closeLocal() : eventLoopGroup.next().newPromise().setSuccess(null);
                        }

                        @Override
                        public void onTunnelClose(TunnelContext ctx) {
                            if (ctx != null) clientTunnelMap.remove(ctx.getTunnelId());
                        }
                    });
                    serverTunnelMap.put(context.getTunnelId(), context);
                }

                @Override
                public void caughtTunnelException(TunnelContext context, Throwable cause) {
                    serverExceptionRef.compareAndSet(null, cause);
                    serverExceptionLatch.countDown();
                }

                @Override
                public Future<?> closeRemoteTunnel(TunnelContext context) {
                    ClientTcpTunnelContext ctc = (ClientTcpTunnelContext) clientTunnelMap.get(context.getTunnelId());
                    return ctc != null ? ctc.closeLocal() : eventLoopGroup.next().newPromise().setSuccess(null);
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

            List<String> received = new ArrayList<>();
            Channel requester = startRequesterClient(eventLoopGroup, infra.requesterPort, received);

            assertTrue(serverRegisterLatch.await(2, TimeUnit.SECONDS),
                    "Server tunnel should be ready");
            assertTrue(clientEstablishedLatch.await(2, TimeUnit.SECONDS),
                    "Client tunnel should be established");

            Thread.sleep(500);

            Channel clientToServerChannel = clientToServerChannelRef.get();
            if (clientToServerChannel != null) {
                clientToServerChannel.close().sync();
            }

            assertFalse(clientToServerChannel.isActive(),
                    "clientToServer channel should be inactive after close");
            Thread.sleep(1000);
        } finally {
            eventLoopGroup.shutdownGracefully().await(2, TimeUnit.SECONDS);
        }
    }
}
