package top.fateironist.cross_relay_core.model.proxy.tunnel.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.socket.DatagramPacket;
import lombok.Getter;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelStatus;

import java.net.InetSocketAddress;

public class ServerUdpTunnelContext extends TunnelContext {
    @Getter
    private Channel requesterChannel;
    @Getter
    private Channel clientProxyChannel;
    @Getter
    private InetSocketAddress requesterAddress;
    @Getter
    private InetSocketAddress clientProxyAddress;

    public ServerUdpTunnelContext(String tunnelId) {
        super(tunnelId, TransportLayerProtocol.UDP);
    }

    public void writeToRequesterAndFlush(ByteBuf msg) {
        if (status == TunnelStatus.OPEN) requesterChannel.writeAndFlush(new DatagramPacket(msg.retain(), requesterAddress));
    }

    public void writeToClientProxyAndFlush(ByteBuf msg) {
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

}
