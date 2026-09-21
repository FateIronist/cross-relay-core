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
import lombok.Getter;
import top.fateironist.constack.Container;
import top.fateironist.constack.Promise;
import top.fateironist.cross_relay_core.FutureBridge;
import top.fateironist.cross_relay_core.Server;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.ServerInfoServerStartArgs;
import top.fateironist.cross_relay_core.model.control.ControlProtocolEventEnum;
import top.fateironist.cross_relay_core.model.control.event.ControlEvent;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;
import top.fateironist.cross_relay_core.util.JsonUtil;

import java.util.Map;
import java.util.concurrent.Future;

/**
 * 对外暴露自身服务器代理信息UDP服务：顶层容器，load = 绑定 UDP 端口即激活。
 */
public class ServerInfoServer extends Container implements Server {
    public static final int PORT = 3461;
    private final EventLoopGroup workerGroup;
    private final ProxyServerInfo proxyServerInfo;

    @Getter
    private volatile Channel channel;

    public ServerInfoServer(EventLoopGroup workerGroup, ProxyServerInfo proxyServerInfo) {
        this.workerGroup = workerGroup;
        this.proxyServerInfo = proxyServerInfo;

        // Effect：关闭 UDP 服务 channel（本容器绑定的 Netty Channel，外部注入的 EventLoopGroup 不清理）
        effect(c -> {
            Channel ch = channel;
            if (ch != null) {
                ch.close();
            }
        });
    }

    @Override
    public Future<Void> start(AbstractArgs arg) {
        return FutureBridge.bridge(load(arg), null);
    }

    @Override
    protected java.util.concurrent.Future<Object> start(Object... args) {
        ServerInfoServerStartArgs serverArgs = (ServerInfoServerStartArgs) args[0];
        var opts = serverArgs.getOptions();

        Bootstrap bootstrap = new Bootstrap();
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

                            if (ControlProtocolEventEnum.SERVER_INFO.matches(event.getType())) {
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

        Promise<Object> promise = new Promise<>();
        ChannelFuture bindFuture = bootstrap.bind(PORT);
        bindFuture.addListener(f -> {
            if (f.isSuccess()) {
                channel = bindFuture.channel();
                promise.setSuccess(null);
            } else {
                promise.setFailure(f.cause());
            }
        });
        return promise;
    }

    @Override
    protected Container createChild(Container parent, Object... args) {
        throw new UnsupportedOperationException("ServerInfoServer does not support child containers");
    }

    @Override
    public Future<?> shutdown() {
        return disposal();
    }

    @Override
    public void shutdownNow() {
        try {
            disposal().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
