package top.fateironist.cross_relay_core.control;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import io.netty.util.CharsetUtil;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.Server;
import top.fateironist.cross_relay_core.ServerStatus;
import top.fateironist.cross_relay_core.model.control.event.ControlEvent;
import top.fateironist.cross_relay_core.model.control.ControlProtocolEventEnum;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.ServerInfoServerStartArgs;
import top.fateironist.cross_relay_core.util.JsonUtil;

import java.util.Map;
import java.util.concurrent.Future;

/**
 * 服务端元数据的 UDP 发现服务：在固定端口回答 SERVER_INFO 查询，用 JSON 回吐本机 ProxyServerInfo（control/proxyRequest/infoServer 三地址）
 * 属备选寻址手段，库内目前无实际调用方（客户端侧对应 ProxyServerInfo.getMetaDataFromServerInfoServer()），服务端信息后续也可能改由 HTTP 等渠道暴露
 */
public class ServerInfoServer implements Server {
    // 固定端口，客户端侧的发现逻辑按此端口硬编码，不可随意变更
    public static final int PORT = 3461;
    private final Bootstrap bootstrap;
    private final EventLoopGroup workerGroup;
    // 应答内容，即本机的服务端元数据
    private final ProxyServerInfo proxyServerInfo;

    public volatile ServerStatus status = ServerStatus.INIT;

    // 绑定后的 UDP channel，供关闭时使用
    private Channel channel;

    public ServerInfoServer(EventLoopGroup workerGroup, ProxyServerInfo proxyServerInfo) {
        this.proxyServerInfo = proxyServerInfo;
        this.workerGroup = workerGroup;
        this.bootstrap = new Bootstrap();
    }

    /**
     * 绑定固定端口启动 UDP 服务，仅在 INIT 状态下生效（重复调用直接返回失败 Future）
     * 收到 SERVER_INFO 查询时当场应答当前 proxyServerInfo，其余类型的事件忽略
     */
    public Future<Void> start(AbstractArgs arg) {
        if (status == ServerStatus.INIT) {
            ServerInfoServerStartArgs args = (ServerInfoServerStartArgs) arg;
            var opts = args.getOptions();

            bootstrap.group(workerGroup)
                .channel(NioDatagramChannel.class)
                .handler(new ChannelInitializer<NioDatagramChannel>() {
                    @Override
                    protected void initChannel(NioDatagramChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new ChannelTrafficShapingHandler(opts.getWriteLimit(), opts.getReadLimit()));
                        pipeline.addLast(new SimpleChannelInboundHandler<DatagramPacket>() {
                            /**
                             * 解析查询报文，仅在事件类型为 SERVER_INFO 时把本机元数据回给请求来源地址
                             * 报文内容由客户端构造，解析失败会抛出交由 exceptionCaught 吞掉，不影响后续查询
                             */
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) throws JsonProcessingException {
                                ByteBuf content = msg.content();
                                String request = content.toString(CharsetUtil.UTF_8);
                                ControlEvent<Map<String, Object>> event = JsonUtil.OBJECT_MAPPER.readValue(request, new TypeReference<ControlEvent<Map<String, Object>>>() {});

                                if (ControlProtocolEventEnum.SERVER_INFO.equals(event.getType())) {
                                    try {
                                        byte[] responseBytes = JsonUtil.OBJECT_MAPPER.writeValueAsBytes(new ControlEvent<>(ControlProtocolEventEnum.SERVER_INFO.getType(), proxyServerInfo));
                                        DatagramPacket response = new DatagramPacket(
                                                ctx.alloc().buffer().writeBytes(responseBytes),
                                                msg.sender()
                                        );
                                        ctx.writeAndFlush(response);
                                    } catch (JsonProcessingException e) {
                                        throw new RuntimeException(e);
                                    }
                                }
                            }

                            /**
                             * 刻意留空：发现服务面向未知来源的 UDP 请求，单个畸形报文不应导致服务中断，故吞掉异常
                             */
                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                            }
                        });
                    }
                });

            ChannelFuture channelFuture = bootstrap.bind(PORT);
            channel = channelFuture.channel();
            channelFuture.addListener(f -> {
                if (f.isSuccess()) {
                    status = ServerStatus.RUNNING;
                }
            });
            return channelFuture;
        }

        return DefaultEventLoopGroup.failFuture(new Exception("Server is not in INIT state"));
    }

    /** 异步关闭 UDP channel，状态经 STOPPING 在关闭完成后落到 SHUTDOWN */
    @Override
    public Future<?> shutdown() {
        if (status == ServerStatus.RUNNING || status == ServerStatus.INIT) {
            status = ServerStatus.STOPPING;
            return channel.close().addListener(f -> status = ServerStatus.SHUTDOWN);
        }
        return DefaultEventLoopGroup.emptyFuture();
    }

    /** 同步关闭 UDP channel，阻塞至 channel 关闭完成 */
    @Override
    public void shutdownNow() {
        if (status == ServerStatus.RUNNING || status == ServerStatus.INIT) {
            status = ServerStatus.STOPPING;
            try {
                channel.close().sync();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            status = ServerStatus.SHUTDOWN;
        }
    }
}
