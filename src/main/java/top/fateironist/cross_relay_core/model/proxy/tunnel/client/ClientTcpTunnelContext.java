package top.fateironist.cross_relay_core.model.proxy.tunnel.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import lombok.Setter;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

import java.util.function.Function;

public class ClientTcpTunnelContext extends TunnelContext {
    @Setter
    private Channel serviceChannel;
    @Setter
    private Channel clientProxyChannel;

    public ClientTcpTunnelContext(String tunnelId) {
        super(tunnelId, TransportLayerProtocol.TCP);
    }

    @Override
    public Future<?> closeGracefully(Function<TunnelContext, Future<?>> closeRemote) {
        tunnelCloseHook.accept(this);
        return DefaultEventLoopGroup.combine(serviceChannel.close(), clientProxyChannel.close(), closeRemote.apply(this));
    }

    @Override
    public Future<?> closeLocal() {
        return DefaultEventLoopGroup.combine(serviceChannel.close(), clientProxyChannel.close());
    }

    public void writeToServiceAndFlush(ByteBuf msg) {
        serviceChannel.writeAndFlush(msg.retain());
    }

    public void writeToClientProxyAndFlush(ByteBuf msg) {
        clientProxyChannel.writeAndFlush(msg.retain());
    }
}
