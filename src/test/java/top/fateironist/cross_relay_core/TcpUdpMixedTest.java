package top.fateironist.cross_relay_core;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TCP + UDP 混合通信测试
 * TCP服务端监听8888端口，两个UDP客户端也使用8888端口通信
 *
 * 目的：验证同一端口号下TCP和UDP可以独立工作，互不干扰
 * TCP是面向连接的可靠传输，UDP是无连接的数据报传输，
 * 虽然端口号相同，但协议不同，操作系统会分别处理
 */
class TcpUdpMixedTest {

    @Test
    void testTcpAndUdpOnSamePort() throws Exception {
        CountDownLatch udpLatch = new CountDownLatch(2);
        java.util.List<String> udpReceived = java.util.Collections.synchronizedList(new java.util.ArrayList<>());

        // ========== 1. 启动TCP服务端，监听8888 ==========
        EventLoopGroup tcpBoss = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        EventLoopGroup tcpWorker = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        ServerBootstrap tcpBootstrap = new ServerBootstrap();
        tcpBootstrap.group(tcpBoss, tcpWorker)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new StringDecoder(StandardCharsets.UTF_8))
                                .addLast(new StringEncoder(StandardCharsets.UTF_8))
                                .addLast(new SimpleChannelInboundHandler<String>() {
                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx, String msg) {
                                        System.out.println("[TCP Server :8888] 收到: " + msg);
                                        ctx.writeAndFlush("TCP-Echo: " + msg);
                                    }

                                    @Override
                                    public void channelActive(ChannelHandlerContext ctx) {
                                        System.out.println("[TCP Server :8888] 新连接: " + ctx.channel().remoteAddress());
                                    }
                                });
                    }
                });
        Channel tcpServerChannel = tcpBootstrap.bind(8888).sync().channel();
        System.out.println(">>> TCP服务端已启动，监听端口: 8888");

        // ========== 2. 启动UDP A（作为"服务端"角色），绑定8888 ==========
        // 注意：UDP和TCP使用不同协议，端口号不冲突
        // 但这里UDP A绑定8888会和TCP服务端冲突吗？
        // 答案：会！TCP和UDP的端口号是独立的，但同一端口号可以被TCP和UDP同时绑定
        EventLoopGroup udpGroupA = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());

        Bootstrap udpBootstrapA = new Bootstrap();
        udpBootstrapA.group(udpGroupA)
                .channel(NioDatagramChannel.class)
                .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) {
                        String content = packet.content().toString(StandardCharsets.UTF_8);
                        System.out.println("[UDP-A :8888] 收到来自 " + packet.sender() + ": " + content);

                        // 回复
                        byte[] reply = ("UDP-A-Reply: " + content).getBytes(StandardCharsets.UTF_8);
                        ctx.writeAndFlush(new DatagramPacket(
                                ctx.alloc().buffer().writeBytes(reply),
                                packet.sender()
                        ));
                    }
                });
        Channel udpChannelA = udpBootstrapA.bind(8888).sync().channel();
        System.out.println(">>> UDP-A已启动，绑定端口: 8888");

        // ========== 3. 启动UDP B（作为"客户端"角色），随机端口 ==========
        EventLoopGroup udpGroupB = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());

        Bootstrap udpBootstrapB = new Bootstrap();
        udpBootstrapB.group(udpGroupB)
                .channel(NioDatagramChannel.class)
                .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) {
                        String reply = packet.content().toString(StandardCharsets.UTF_8);
                        System.out.println("[UDP-B] 收到回复: " + reply);
                        udpReceived.add(reply);
                        udpLatch.countDown();
                    }
                });
        Channel udpChannelB = udpBootstrapB.bind(0).sync().channel();
        System.out.println(">>> UDP-B已启动，绑定随机端口");

        // ========== 4. UDP B 向 UDP A 发送消息 ==========
        Thread.sleep(500); // 等待所有服务就绪
        for (int i = 1; i <= 2; i++) {
            byte[] data = ("UDP-Hello-" + i).getBytes(StandardCharsets.UTF_8);
            DatagramPacket packet = new DatagramPacket(
                    udpChannelB.alloc().buffer().writeBytes(data),
                    new InetSocketAddress("127.0.0.1", 8888)
            );
            udpChannelB.writeAndFlush(packet);
            System.out.println("[UDP-B] 发送: UDP-Hello-" + i);
        }

        // 等待UDP回复
        assertTrue(udpLatch.await(5, TimeUnit.SECONDS), "UDP应在5秒内收到回复");
        assertEquals(2, udpReceived.size());
        System.out.println(">>> UDP通信完成，收到 " + udpReceived.size() + " 条回复");

        // ========== 5. 清理 ==========
        udpChannelB.close().sync();
        udpChannelA.close().sync();
        tcpServerChannel.close().sync();

        udpGroupB.shutdownGracefully();
        udpGroupA.shutdownGracefully();
        tcpBoss.shutdownGracefully();
        tcpWorker.shutdownGracefully();

        System.out.println(">>> 所有服务已关闭");
    }
}
