package top.fateironist.cross_relay_core.model.proxy.tunnel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.socket.DatagramPacket;
import lombok.Getter;
import top.fateironist.constack.Container;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;

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

    private long lastActiveTime = System.currentTimeMillis();

    private final long timeOut = 30000;

    public ServerUdpTunnelContext(Container parent, String tunnelId) {
        super(parent, tunnelId, TransportLayerProtocol.UDP);
    }

    public void writeToRequesterAndFlush(ByteBuf msg) {
        lastActiveTime = System.currentTimeMillis();
        if (state() == State.ACTIVE) requesterChannel.writeAndFlush(new DatagramPacket(msg.retain(), requesterAddress));
    }

    public void writeToClientProxyAndFlush(ByteBuf msg) {
        lastActiveTime = System.currentTimeMillis();
        if (state() == State.ACTIVE) clientProxyChannel.writeAndFlush(new DatagramPacket(msg.retain(), clientProxyAddress));
    }

    public void setClientProxy(InetSocketAddress clientProxyAddress, Channel clientProxyChannel) {
        if (this.clientProxyAddress == null) {
            this.clientProxyAddress = clientProxyAddress;
        }
        if (this.clientProxyChannel == null) {
            this.clientProxyChannel = clientProxyChannel;
            // Effect：关闭 client-proxy 侧数据报 channel（本容器持有的 Netty Channel）
            effect(c -> clientProxyChannel.close());
        }
        tryOpen();
    }

    public void setRequester(InetSocketAddress requesterAddress, Channel requesterChannel) {
        if (this.requesterAddress == null) {
            this.requesterAddress = requesterAddress;
        }
        if (this.requesterChannel == null) {
            this.requesterChannel = requesterChannel;
            // Effect：关闭 requester 侧数据报 channel（本容器持有的 Netty Channel）
            effect(c -> requesterChannel.close());
        }
        tryOpen();
    }

    @Override
    protected boolean isBothEndsReady() {
        return requesterChannel != null && clientProxyChannel != null && requesterAddress != null && clientProxyAddress != null;
    }

    /** 空闲巡检：超时隧道优雅关闭（通知对端并销毁回收） */
    public void checkTimeout() {
        if (isTimeout()) {
            closeGracefully();
        }
    }

    public boolean isTimeout() {
        return state() == State.ACTIVE && System.currentTimeMillis() - lastActiveTime > timeOut;
    }
}
