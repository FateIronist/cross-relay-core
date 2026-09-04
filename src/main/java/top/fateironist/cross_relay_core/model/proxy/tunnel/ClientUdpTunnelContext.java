package top.fateironist.cross_relay_core.model.proxy.tunnel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.socket.DatagramPacket;
import lombok.Getter;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;

import java.net.InetSocketAddress;
import java.util.concurrent.Future;

public class ClientUdpTunnelContext extends TunnelContext {
    @Getter
    private Channel duplexChannel;
    @Getter
    private InetSocketAddress serviceAddress;
    @Getter
    private InetSocketAddress serverProxyAddress;

    private long lastActiveTime = System.currentTimeMillis();

    private final long timeOut = 30000;

    public ClientUdpTunnelContext(String tunnelId) {
        super(tunnelId, TransportLayerProtocol.UDP);
    }

    public void writeToServiceAndFlush(ByteBuf msg) {
        lastActiveTime = System.currentTimeMillis();
        if (status == TunnelStatus.OPEN) duplexChannel.writeAndFlush(new DatagramPacket(msg.retain(), serviceAddress));
    }

    public void writeToServerProxyAndFlush(ByteBuf msg) {
        lastActiveTime = System.currentTimeMillis();
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
