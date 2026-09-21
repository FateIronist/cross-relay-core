package top.fateironist.cross_relay_core.model.proxy.tunnel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.socket.DatagramPacket;
import lombok.Getter;
import top.fateironist.constack.Container;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.proxy.ClientUdpProxyContext;

import java.net.InetSocketAddress;

public class ClientUdpTunnelContext extends TunnelContext {
    @Getter
    private Channel duplexChannel;
    @Getter
    private InetSocketAddress serviceAddress;
    @Getter
    private InetSocketAddress serverProxyAddress;

    private long lastActiveTime = System.currentTimeMillis();

    private final long timeOut = 30000;

    public ClientUdpTunnelContext(Container parent, String tunnelId) {
        super(parent, tunnelId, TransportLayerProtocol.UDP);
    }

    public void writeToServiceAndFlush(ByteBuf msg) {
        lastActiveTime = System.currentTimeMillis();
        if (state() == State.ACTIVE) duplexChannel.writeAndFlush(new DatagramPacket(msg.retain(), serviceAddress));
    }

    public void writeToServerProxyAndFlush(ByteBuf msg) {
        lastActiveTime = System.currentTimeMillis();
        if (state() == State.ACTIVE) duplexChannel.writeAndFlush(new DatagramPacket(msg.retain(), serverProxyAddress));
    }

    public void writeToOpposite(InetSocketAddress sender, ByteBuf msg) {
        if (sender.equals(serviceAddress)) {
            writeToServerProxyAndFlush(msg);
        } else if (sender.equals(serverProxyAddress)) {
            writeToServiceAndFlush(msg);
        }
    }

    public void setDuplexChannel(Channel channel) {
        if (duplexChannel == null) {
            duplexChannel = channel;
            // Effect：关闭本端数据报 channel（本容器持有的 Netty Channel）
            effect(c -> channel.close());
        }
        tryOpen();
    }

    public void setServerProxyAddress(InetSocketAddress serverProxyAddress) {
        if (this.serverProxyAddress == null) {
            this.serverProxyAddress = serverProxyAddress;
            // 受控写入：地址确立即登记父代理容器的地址路由索引
            ((ClientUdpProxyContext) parent()).registerTunnelAddress(serverProxyAddress, this);
        }
        tryOpen();
    }

    public void setServiceAddress(InetSocketAddress serviceAddress) {
        if (this.serviceAddress == null) {
            this.serviceAddress = serviceAddress;
            // 受控写入：地址确立即登记父代理容器的地址路由索引
            ((ClientUdpProxyContext) parent()).registerTunnelAddress(serviceAddress, this);
        }
        tryOpen();
    }

    @Override
    protected boolean isBothEndsReady() {
        return duplexChannel != null && serviceAddress != null && serverProxyAddress != null;
    }

    /** 空闲判定：交由上层巡检调度使用 */
    public boolean isTimeout() {
        return state() == State.ACTIVE && System.currentTimeMillis() - lastActiveTime > timeOut;
    }
}
