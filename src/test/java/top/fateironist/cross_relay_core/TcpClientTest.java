package top.fateironist.cross_relay_core;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Netty TCP 客户端测试类
 * 演示：Bootstrap 连接服务端、ChannelHandler 读写数据、粘包/拆包处理
 *
 * 关键概念：
 * - Bootstrap: 客户端启动引导类
 * - LengthFieldBasedFrameDecoder: 基于长度字段的帧解码器（解决TCP粘包/拆包）
 * - ChannelFuture: Netty异步操作的结果，可添加监听器
 */
class TcpClientTest {

    public static void main(String[] args) throws Exception {
        new TcpClientTest().testTcpClientConnect();
    }

    /**
     * 测试1: TCP客户端连接Echo服务端，发送并接收消息
     * 同时启动一个简易Echo服务端配合测试
     */
    @Test
    void testTcpClientConnect() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        String[] received = new String[1];

        // ========== 先启动Echo服务端 ==========
        EventLoopGroup serverBoss = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        EventLoopGroup serverWorker = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(serverBoss, serverWorker)
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
                                        ctx.writeAndFlush("ServerReply: " + msg);
                                    }
                                });
                    }
                });
        Channel serverChannel = serverBootstrap.bind(8901).sync().channel();

        // ========== 启动TCP客户端 ==========
        EventLoopGroup clientGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());

        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(clientGroup)
                    .channel(NioSocketChannel.class)               // 客户端使用 NioSocketChannel
                    .option(ChannelOption.TCP_NODELAY, true)        // 禁用Nagle算法
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000) // 连接超时5秒
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast(new StringDecoder(StandardCharsets.UTF_8))
                                    .addLast(new StringEncoder(StandardCharsets.UTF_8))
                                    .addLast(new SimpleChannelInboundHandler<String>() {
                                        @Override
                                        protected void channelRead0(ChannelHandlerContext ctx, String msg) {
                                            System.out.println("[TCP Client] 收到服务端回复: " + msg);
                                            received[0] = msg;
                                            latch.countDown();
                                        }

                                        @Override
                                        public void channelActive(ChannelHandlerContext ctx) {
                                            System.out.println("[TCP Client] 已连接服务端");
                                            // 连接建立后立即发送数据
                                            ctx.writeAndFlush("Hello from TCP Client!");
                                        }
                                    });
                        }
                    });

            // 连接服务端（异步）
            ChannelFuture connectFuture = bootstrap.connect("127.0.0.1", 8901);
            connectFuture.sync(); // 同步等待连接成功

            // 等待收到回复（最多5秒）
            assertTrue(latch.await(5, TimeUnit.SECONDS), "应在5秒内收到回复");
            assertEquals("ServerReply: Hello from TCP Client!", received[0]);

            // 关闭连接
            connectFuture.channel().close().sync();
        } finally {
            clientGroup.shutdownGracefully();
            serverChannel.close().sync();
            serverBoss.shutdownGracefully();
            serverWorker.shutdownGracefully();
        }
    }

    /**
     * 测试2: 演示TCP粘包/拆包解决方案 —— LengthFieldBasedFrameDecoder
     *
     * TCP是流式协议，没有消息边界，可能出现：
     * - 粘包: 多条消息合并成一个数据包
     * - 拆包: 一条消息被拆成多个数据包
     *
     * LengthFieldBasedFrameDecoder 方案：
     * 在每条消息前加一个长度字段，接收方根据长度字段准确切分消息
     *
     * 数据格式: [长度字段(4字节)] [消息内容(N字节)]
     */
    @Test
    void testTcpWithLengthFieldCodec() throws Exception {
        CountDownLatch latch = new CountDownLatch(3);
        java.util.List<String> receivedMessages = java.util.Collections.synchronizedList(new java.util.ArrayList<>());

        // 启动带长度字段编解码的服务端
        EventLoopGroup serverGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(serverGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                // 入站解码器: 最大帧长1MB, 长度字段偏移0, 长度字段4字节, 长度偏移量0, 跳过前4字节
                                .addLast(new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4))
                                .addLast(new StringDecoder(StandardCharsets.UTF_8))
                                // 出站编码器: 在消息前加4字节长度头
                                .addLast(new LengthFieldPrepender(4))
                                .addLast(new StringEncoder(StandardCharsets.UTF_8))
                                .addLast(new SimpleChannelInboundHandler<String>() {
                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx, String msg) {
                                        System.out.println("[Server] 收到完整消息: " + msg);
                                        ctx.writeAndFlush("Echo: " + msg);
                                    }
                                });
                    }
                });
        Channel serverChannel = serverBootstrap.bind(8902).sync().channel();

        // 客户端
        EventLoopGroup clientGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(clientGroup)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast(new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4))
                                    .addLast(new StringDecoder(StandardCharsets.UTF_8))
                                    .addLast(new LengthFieldPrepender(4))
                                    .addLast(new StringEncoder(StandardCharsets.UTF_8))
                                    .addLast(new SimpleChannelInboundHandler<String>() {
                                        @Override
                                        protected void channelRead0(ChannelHandlerContext ctx, String msg) {
                                            receivedMessages.add(msg);
                                            latch.countDown();
                                        }

                                        @Override
                                        public void channelActive(ChannelHandlerContext ctx) {
                                            // 连续发送3条消息，测试粘包处理
                                            for (int i = 1; i <= 3; i++) {
                                                ctx.writeAndFlush("Message-" + i);
                                            }
                                        }
                                    });
                        }
                    });

            bootstrap.connect("127.0.0.1", 8902).sync();
            assertTrue(latch.await(5, TimeUnit.SECONDS));
            assertEquals(3, receivedMessages.size());

            serverChannel.close().sync();
        } finally {
            clientGroup.shutdownGracefully();
            serverGroup.shutdownGracefully();
        }
    }
}
