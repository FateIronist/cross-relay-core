package top.fateironist.cross_relay_core.model.proxy.tunnel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.socket.DatagramPacket;
import lombok.Getter;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;

import java.net.InetSocketAddress;
import java.util.concurrent.Future;

public class ServerUdpTunnelContext extends TunnelContext {
    @Getter
    private Channel requesterChannel;
    @Getter
    private Channel clientProxyChannel;
    @Getter
    private InetSocketAddress requesterAddress;
    @Getter
    private InetSocketAddress clientProxyAddress;

    private long lastActiveTime = System.currentTimeMillis();

    private final long timeOut = 30000;

    public ServerUdpTunnelContext(String tunnelId) {
        super(tunnelId, TransportLayerProtocol.UDP);
    }

    public void writeToRequesterAndFlush(ByteBuf msg) {
        lastActiveTime = System.currentTimeMillis();
        if (status == TunnelStatus.OPEN) requesterChannel.writeAndFlush(new DatagramPacket(msg.retain(), requesterAddress));
    }

    public void writeToClientProxyAndFlush(ByteBuf msg) {
        lastActiveTime = System.currentTimeMillis();
        if (status == TunnelStatus.OPEN) clientProxyChannel.writeAndFlush(new DatagramPacket(msg.retain(), clientProxyAddress));
    }

    public void setClientProxy(InetSocketAddress clientProxyAddress, Channel clientProxyChannel) {
        if (clientProxyAddress == null) this.clientProxyAddress = clientProxyAddress;
        if (clientProxyChannel == null) this.clientProxyChannel = clientProxyChannel;
        tryOpen();
    }

    public void setRequester(InetSocketAddress requesterAddress, Channel requesterChannel) {
        if (requesterAddress == null) this.requesterAddress = requesterAddress;
        if (requesterChannel == null) this.requesterChannel = requesterChannel;
        tryOpen();
    }

    @Override
    protected boolean tryOpen() {
        if (requesterChannel != null && clientProxyChannel != null && requesterAddress != null && clientProxyAddress != null) {
            status = TunnelStatus.OPEN;
            return true;
        }

        return false;
    }

    public Future<?> checkTimeout() {
        if (isTimeout()) {
            return closeGracefully();
        }

        return DefaultEventLoopGroup.emptyFuture();
    }

    public boolean isTimeout() {
        return status == TunnelStatus.OPEN && System.currentTimeMillis() - lastActiveTime > timeOut;
    }

}
