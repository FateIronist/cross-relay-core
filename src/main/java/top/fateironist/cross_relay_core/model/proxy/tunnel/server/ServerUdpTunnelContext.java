package top.fateironist.cross_relay_core.model.proxy.tunnel.server;

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

public class ServerUdpTunnelContext extends TunnelContext {
    @Setter
    private Channel requesterChannel;
    @Setter
    private Channel clientProxyChannel;
    @Setter
    private InetSocketAddress requesterAddress;
    @Setter
    private InetSocketAddress clientProxyAddress;

    public ServerUdpTunnelContext(String tunnelId) {
        super(tunnelId, TransportLayerProtocol.UDP);
    }

    @Override
    public Future<?> closeGracefully(Function<TunnelContext, Future<?>> closeRemote) {
        tunnelCloseHook.accept(this);
        return DefaultEventLoopGroup.combine(requesterChannel.close(), closeRemote.apply(this));
    }

    @Override
    public Future<?> closeLocal() {
        return requesterChannel.close();
    }

    public void writeToRequesterAndFlush(ByteBuf msg) {
        requesterChannel.writeAndFlush(new DatagramPacket(msg.retain(), requesterAddress));
    }

    public void writeToClientProxyAndFlush(ByteBuf msg) {
        clientProxyChannel.writeAndFlush(new DatagramPacket(msg.retain(), clientProxyAddress));
    }

}
