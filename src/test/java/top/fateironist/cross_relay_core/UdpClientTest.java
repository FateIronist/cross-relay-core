package top.fateironist.cross_relay_core;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
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
 * Netty UDP 客户端测试类
 * 演示：UDP客户端连接、connect模式 vs 无连接模式、批量发送
 *
 * UDP客户端两种模式：
 * 1. 无连接模式（bind）: 绑定本地端口，手动指定目标地址发送
 * 2. 连接模式（connect）: 绑定目标地址，发送时无需指定目标（类似TCP的使用方式）
 */
class UdpClientTest {

    /**
     * 测试1: UDP客户端 connect模式
     * connect后发送数据无需指定目标地址，Netty自动路由到连接的目标
     */
    @Test
    void testUdpClientConnectMode() throws Exception {
        CountDownLatch serverLatch = new CountDownLatch(2);
        CountDownLatch clientLatch = new CountDownLatch(2);

        // 启动UDP服务端
        EventLoopGroup serverGroup = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
        Bootstrap serverBootstrap = new Bootstrap();
        serverBootstrap.group(serverGroup)
                .channel(NioDatagramChannel.class)
                .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) {
                        String content = packet.content().toString(StandardCharsets.UTF_8);
                        System.out.println("[Server] 收到: " + content);
                        serverLatch.countDown();

                        // 回复
                        byte[] reply = ("ACK: " + content).getBytes(StandardCharsets.UTF_8);
                        ctx.writeAndFlush(new DatagramPacket(
                                ctx.alloc().buffer().writeBytes(reply),
                                packet.sender()
                        ));
                    }
                });
        Channel serverChannel = serverBootstrap.bind(9003).sync().channel();

        // 启动UDP客户端（connect模式）
        EventLoopGroup clientGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try {
            Bootstrap clientBootstrap = new Bootstrap();
            clientBootstrap.group(clientGroup)
                    .channel(NioDatagramChannel.class)
                    .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) {
                            String reply = packet.content().toString(StandardCharsets.UTF_8);
                            System.out.println("[Client] 收到: " + reply);
                            clientLatch.countDown();
                        }
                    });

            // connect模式：绑定目标地址
            Channel clientChannel = clientBootstrap.connect("127.0.0.1", 9003).sync().channel();

            // connect模式下，发送数据无需指定目标地址
            for (int i = 1; i <= 2; i++) {
                byte[] data = ("Connect-Mode-" + i).getBytes(StandardCharsets.UTF_8);
                ByteBuf buf = clientChannel.alloc().buffer().writeBytes(data);
                clientChannel.writeAndFlush(buf); // 直接写ByteBuf，不需要包装成DatagramPacket
            }

            assertTrue(clientLatch.await(5, TimeUnit.SECONDS));
            assertTrue(serverLatch.await(5, TimeUnit.SECONDS));

            clientChannel.close().sync();
            serverChannel.close().sync();
        } finally {
            clientGroup.shutdownGracefully();
            serverGroup.shutdownGracefully();
        }
    }

    /**
     * 测试2: UDP批量发送与性能测试
     * 演示高并发场景下的UDP批量数据发送
     */
    @Test
    void testUdpBatchSend() throws Exception {
        int totalMessages = 100;
        CountDownLatch receivedLatch = new CountDownLatch(totalMessages);
        java.util.concurrent.atomic.AtomicInteger receivedCount = new java.util.concurrent.atomic.AtomicInteger(0);

        // 服务端
        EventLoopGroup serverGroup = new MultiThreadIoEventLoopGroup(4, NioIoHandler.newFactory());
        Bootstrap serverBootstrap = new Bootstrap();
        serverBootstrap.group(serverGroup)
                .channel(NioDatagramChannel.class)
                .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) {
                        receivedCount.incrementAndGet();
                        receivedLatch.countDown();
                    }
                });
        Channel serverChannel = serverBootstrap.bind(9004).sync().channel();

        // 客户端批量发送
        EventLoopGroup clientGroup = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
        try {
            Bootstrap clientBootstrap = new Bootstrap();
            clientBootstrap.group(clientGroup)
                    .channel(NioDatagramChannel.class)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            // UDP客户端不需要特殊handler，只需能发送数据
                        }
                    });

            Channel clientChannel = clientBootstrap.bind(0).sync().channel();

            long startTime = System.nanoTime();

            // 批量发送
            for (int i = 0; i < totalMessages; i++) {
                byte[] data = ("Batch-" + i).getBytes(StandardCharsets.UTF_8);
                DatagramPacket packet = new DatagramPacket(
                        clientChannel.alloc().buffer().writeBytes(data),
                        new InetSocketAddress("127.0.0.1", 9004)
                );
                clientChannel.writeAndFlush(packet);
            }

            long sendTimeMs = (System.nanoTime() - startTime) / 1_000_000;
            System.out.println("[Client] 批量发送 " + totalMessages + " 条消息耗时: " + sendTimeMs + "ms");

            // 等待接收（UDP可能丢包，所以用 > 0 判断）
            receivedLatch.await(5, TimeUnit.SECONDS);
            System.out.println("[Server] 实际收到: " + receivedCount.get() + "/" + totalMessages + " 条消息");

            // 本地回环一般不会丢包
            assertTrue(receivedCount.get() > 0, "至少应收到部分消息");

            serverChannel.close().sync();
            clientChannel.close().sync();
        } finally {
            clientGroup.shutdownGracefully();
            serverGroup.shutdownGracefully();
        }
    }
}
