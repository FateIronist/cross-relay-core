package top.fateironist.cross_relay_core.model.proxy.tunnel.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.concurrent.Future;
import lombok.Getter;
import lombok.Setter;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelStatus;

import java.net.InetSocketAddress;
import java.util.function.Function;

public class ClientUdpTunnelContext extends TunnelContext {
    @Getter
    private Channel duplexChannel;
    @Getter
    private InetSocketAddress serviceAddress;
    @Getter
    private InetSocketAddress serverProxyAddress;

    public ClientUdpTunnelContext(String tunnelId) {
        super(tunnelId, TransportLayerProtocol.UDP);
    }

    public void writeToServiceAndFlush(ByteBuf msg) {
        if (status == TunnelStatus.OPEN) duplexChannel.writeAndFlush(new DatagramPacket(msg.retain(), serviceAddress));
    }

    public void writeToServerProxyAndFlush(ByteBuf msg) {
        if (status == TunnelStatus.OPEN) duplexChannel.writeAndFlush(new DatagramPacket(msg.retain(), serverProxyAddress));
    }

    public void writeToOpposite(InetSocketAddress sender, ByteBuf msg) {
        if (sender.equals(serviceAddress)) {
            writeToServerProxyAndFlush(msg);
        } else if (sender.equals(serverProxyAddress)) {
            writeToServiceAndFlush(msg);
        }
    }

    public void setDuplexChannel(Channel channel) {
        if (duplexChannel == null) duplexChannel = channel;
        tryOpen();
    }

    public void setServerProxyAddress(InetSocketAddress serverProxyAddress) {
        if (this.serverProxyAddress == null) this.serverProxyAddress = serverProxyAddress;
        tryOpen();
    }
    public void setServiceAddress(InetSocketAddress serviceAddress) {
        if (this.serviceAddress == null) this.serviceAddress = serviceAddress;
        tryOpen();
    }

    @Override
    protected boolean tryOpen() {
        if (duplexChannel != null && serviceAddress != null && serverProxyAddress != null) {
            status = TunnelStatus.OPEN;
            return true;
        }

        return false;
    }
}
