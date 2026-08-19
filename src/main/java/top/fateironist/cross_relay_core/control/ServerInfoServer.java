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
import top.fateironist.cross_relay_core.Server;
import top.fateironist.cross_relay_core.model.control.ControlEvent;
import top.fateironist.cross_relay_core.model.control.ControlProtocolEventEnum;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.ServerInfoServerStartArgs;
import top.fateironist.cross_relay_core.util.JsonUtil;

import java.util.Map;
import java.util.concurrent.Future;

/**
 * 对外暴露自身服务器代理信息UDP服务
 */
public class ServerInfoServer implements Server {
    public static final int PORT = 3461;
    private final Bootstrap bootstrap;
    private final EventLoopGroup workerGroup;
    private final ProxyServerInfo proxyServerInfo;

    public ServerInfoServer(EventLoopGroup workerGroup, ProxyServerInfo proxyServerInfo) {
        this.proxyServerInfo = proxyServerInfo;
        this.workerGroup = workerGroup;
        this.bootstrap = new Bootstrap();
    }

    public Future<Void> start(AbstractArgs arg) {
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

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                            }
                        });
                    }
                });

        return bootstrap.bind(PORT);
    }

    @Override
    public Future<?> shutdown() {
        return workerGroup.shutdownGracefully();
    }

    @Override
    public void shutdownNow() {
        workerGroup.shutdownNow();
    }
}
