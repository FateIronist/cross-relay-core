package top.fateironist.cross_relay_core.model.proxy.tunnel.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.concurrent.Future;
import lombok.Setter;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

import java.net.InetSocketAddress;
import java.util.function.Function;

public class ClientUdpTunnelContext extends TunnelContext {
    @Setter
    private Channel duplexChannel;
    @Setter
    private InetSocketAddress serviceAddress;
    @Setter
    private InetSocketAddress serverProxyAddress;

    public ClientUdpTunnelContext(String tunnelId) {
        super(tunnelId, TransportLayerProtocol.UDP);
    }

    @Override
    public Future<?> closeGracefully(Function<TunnelContext, Future<?>> closeRemote) {
        tunnelCloseHook.accept(this);
        return DefaultEventLoopGroup.combine(duplexChannel.close(), closeRemote.apply(this));
    }

    @Override
    public Future<?> closeLocal() {
        return duplexChannel.close();
    }

    public void writeToServiceAndFlush(ByteBuf msg) {
        duplexChannel.writeAndFlush(new DatagramPacket(msg.retain(), serviceAddress));
    }

    public void writeToServerProxyAndFlush(ByteBuf msg) {
        duplexChannel.writeAndFlush(new DatagramPacket(msg.retain(), serverProxyAddress));
    }
}
