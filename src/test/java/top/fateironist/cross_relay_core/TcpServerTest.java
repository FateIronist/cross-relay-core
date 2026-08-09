package top.fateironist.cross_relay_core;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Netty TCP 服务端测试类
 * 演示：ServerBootstrap 启动、ChannelPipeline 配置、ChannelHandler 编写
 *
 * 架构要点：
 * - bossGroup: 负责接受客户端连接（通常1个线程即可）
 * - workerGroup: 负责处理已接受连接的IO读写
 * - ChannelPipeline: 处理器链，数据流经每个 handler 依次处理
 */
class TcpServerTest {

    /**
     * 测试1: 基础TCP Echo服务端
     * 启动一个监听8899端口的Echo服务，收到什么就回复什么
     */
    @Test
    void testTcpEchoServer() throws Exception {
        // 4.2.x 新API: 使用 MultiThreadIoEventLoopGroup 替代已废弃的 NioEventLoopGroup
        // NioIoHandler.newFactory() 创建NIO处理器工厂
        EventLoopGroup bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        EventLoopGroup workerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)          // 指定服务端Channel类型
                    .handler(new LoggingHandler(LogLevel.INFO))     // boss组日志（可观察连接接入）
                    .option(ChannelOption.SO_BACKLOG, 128)         // 连接队列大小
                    .childOption(ChannelOption.SO_KEEPALIVE, true) // 保持连接
                    .childOption(ChannelOption.TCP_NODELAY, true)  // 禁用Nagle算法，减少延迟
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        /**
                         * 每个新连接都会调用此方法，用于配置该连接的Pipeline
                         * Pipeline是Netty的核心概念：数据像流水线一样经过每个Handler
                         */
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            // 入站：字节 -> 字符串（解码器）
                            pipeline.addLast(new StringDecoder(StandardCharsets.UTF_8));
                            // 出站：字符串 -> 字节（编码器）
                            pipeline.addLast(new StringEncoder(StandardCharsets.UTF_8));
                            // 业务处理：Echo逻辑
                            pipeline.addLast(new SimpleChannelInboundHandler<String>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, String msg) {
                                    System.out.println("[TCP Server] 收到: " + msg);
                                    // 回复客户端
                                    ctx.writeAndFlush("Echo: " + msg);
                                }

                                @Override
                                public void channelActive(ChannelHandlerContext ctx) {
                                    System.out.println("[TCP Server] 新连接: " + ctx.channel().remoteAddress());
                                }

                                @Override
                                public void channelInactive(ChannelHandlerContext ctx) {
                                    System.out.println("[TCP Server] 连接关闭: " + ctx.channel().remoteAddress());
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                    System.err.println("[TCP Server] 异常: " + cause.getMessage());
                                    ctx.close();
                                }
                            });
                        }
                    });

            // 绑定端口并同步等待绑定成功
            ChannelFuture future = bootstrap.bind(8899).sync();
            System.out.println("[TCP Server] 已启动，监听端口: 8899");
            assertTrue(future.channel().isActive());

            // 运行10秒后关闭（实际项目中会一直运行）
            Thread.sleep(10_000);

            // 优雅关闭
            future.channel().close().sync();
        } finally {
            // shutdownGracefully: 先拒绝新连接，再处理完剩余任务后关闭
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    /**
     * 测试2: 虚拟线程兼容的TCP服务端
     * Netty 4.2.x 优化了虚拟线程支持，FastThreadLocal 对虚拟线程回退检查优化至 O(1)
     *
     * 注意：Netty 的 EventLoop 本身是平台线程（NIO线程），
     * 但业务逻辑可以提交到虚拟线程执行，避免阻塞NIO线程
     */
    @Test
    void testTcpServerWithVirtualThreadHandler() throws Exception {
        EventLoopGroup bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        EventLoopGroup workerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        // 创建虚拟线程执行器，用于处理业务逻辑
        var virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new StringDecoder(StandardCharsets.UTF_8));
                            ch.pipeline().addLast(new StringEncoder(StandardCharsets.UTF_8));
                            ch.pipeline().addLast(new SimpleChannelInboundHandler<String>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, String msg) {
                                    // 将耗时业务逻辑提交到虚拟线程，不阻塞NIO EventLoop
                                    virtualThreadExecutor.submit(() -> {
                                        try {
                                            // 模拟耗时操作（虚拟线程中阻塞不会占用平台线程）
                                            Thread.sleep(100);
                                            System.out.println("[VirtualThread] " + Thread.currentThread() + " 处理: " + msg);
                                            ctx.writeAndFlush("VirtualEcho: " + msg);
                                        } catch (InterruptedException e) {
                                            Thread.currentThread().interrupt();
                                        }
                                    });
                                }
                            });
                        }
                    });

            ChannelFuture future = bootstrap.bind(8900).sync();
            System.out.println("[TCP Server] 虚拟线程模式启动，监听端口: 8900");
            assertTrue(future.channel().isActive());

            Thread.sleep(10_000);
            future.channel().close().sync();
        } finally {
            virtualThreadExecutor.close();
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
