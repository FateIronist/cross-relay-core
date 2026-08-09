package top.fateironist.cross_relay_core;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Netty UDP 服务端测试类
 * 演示：UDP无连接通信、DatagramPacket 处理
 *
 * UDP vs TCP 关键区别：
 * - UDP无连接，不需要 bossGroup/workerGroup 模型
 * - 数据以 DatagramPacket 为单位收发，不保证顺序和可靠性
 * - 单个 Channel 即可处理所有客户端通信（无需为每个客户端创建新Channel）
 * - 适合内网穿透场景：低延迟、无握手开销
 */
class UdpServerTest {

    /**
     * 测试1: UDP Echo服务端
     * 收到DatagramPacket后，将内容回复给发送方
     */
    @Test
    void testUdpEchoServer() throws Exception {
        // UDP只需一个EventLoopGroup（无连接，不需要boss/worker分离）
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(4, NioIoHandler.newFactory());

        try {
            Bootstrap bootstrap = new Bootstrap(); // 注意：UDP服务端也用 Bootstrap，不是 ServerBootstrap
            bootstrap.group(group)
                    .channel(NioDatagramChannel.class)    // UDP使用 NioDatagramChannel
                    .option(ChannelOption.SO_BROADCAST, true) // 支持广播
                    .handler(new SimpleChannelInboundHandler<DatagramPacket>() {

                        /**
                         * channelRead0 接收 DatagramPacket
                         * DatagramPacket 包含：发送方地址 + 数据内容
                         */
                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) {
                            // 提取数据内容
                            String content = packet.content().toString(StandardCharsets.UTF_8);
                            System.out.println("[UDP Server] 收到来自 " + packet.sender() + ": " + content);

                            // 构造回复：指定目标地址（UDP必须指定回复地址）
                            byte[] replyBytes = ("Echo: " + content).getBytes(StandardCharsets.UTF_8);
                            DatagramPacket reply = new DatagramPacket(
                                    ctx.alloc().buffer().writeBytes(replyBytes),
                                    packet.sender() // 回复给发送方
                            );
                            ctx.writeAndFlush(reply);
                        }

                        @Override
                        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                            System.err.println("[UDP Server] 异常: " + cause.getMessage());
                            // UDP无连接，异常不需要关闭Channel（所有客户端共用一个Channel）
                        }
                    });

            // 绑定UDP端口
            ChannelFuture future = bootstrap.bind(9001).sync();
            System.out.println("[UDP Server] 已启动，监听UDP端口: 9001");
            assertTrue(future.channel().isActive());

            // 运行10秒
            Thread.sleep(10_000);

            future.channel().close().sync();
        } finally {
            group.shutdownGracefully();
        }
    }

    /**
     * 测试2: UDP服务端 + 客户端完整通信测试
     * 演示完整的UDP请求-响应流程
     */
    @Test
    void testUdpServerClientCommunication() throws Exception {
        CountDownLatch latch = new CountDownLatch(3);
        java.util.List<String> received = java.util.Collections.synchronizedList(new java.util.ArrayList<>());

        // ========== 启动UDP服务端 ==========
        EventLoopGroup serverGroup = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
        Bootstrap serverBootstrap = new Bootstrap();
        serverBootstrap.group(serverGroup)
                .channel(NioDatagramChannel.class)
                .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) {
                        String content = packet.content().toString(StandardCharsets.UTF_8);
                        System.out.println("[UDP Server] 收到: " + content);

                        byte[] replyBytes = ("Reply: " + content).getBytes(StandardCharsets.UTF_8);
                        ctx.writeAndFlush(new DatagramPacket(
                                ctx.alloc().buffer().writeBytes(replyBytes),
                                packet.sender()
                        ));
                    }
                });
        Channel serverChannel = serverBootstrap.bind(9002).sync().channel();

        // ========== 启动UDP客户端 ==========
        EventLoopGroup clientGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try {
            Bootstrap clientBootstrap = new Bootstrap();
            clientBootstrap.group(clientGroup)
                    .channel(NioDatagramChannel.class)
                    .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) {
                            String reply = packet.content().toString(StandardCharsets.UTF_8);
                            System.out.println("[UDP Client] 收到回复: " + reply);
                            received.add(reply);
                            latch.countDown();
                        }
                    });

            // UDP客户端不需要connect，但绑定一个本地端口用于接收回复
            Channel clientChannel = clientBootstrap.bind(0).sync().channel();

            // 发送3个UDP数据包
            for (int i = 1; i <= 3; i++) {
                byte[] data = ("UDP-Message-" + i).getBytes(StandardCharsets.UTF_8);
                DatagramPacket packet = new DatagramPacket(
                        clientChannel.alloc().buffer().writeBytes(data),
                        new InetSocketAddress("127.0.0.1", 9002)
                );
                clientChannel.writeAndFlush(packet);
                System.out.println("[UDP Client] 发送: UDP-Message-" + i);
            }

            // 等待所有回复
            assertTrue(latch.await(5, TimeUnit.SECONDS), "应在5秒内收到所有回复");
            assertEquals(3, received.size());

            clientChannel.close().sync();
            serverChannel.close().sync();
        } finally {
            clientGroup.shutdownGracefully();
            serverGroup.shutdownGracefully();
        }
    }
}
