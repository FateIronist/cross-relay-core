package top.fateironist.cross_relay_core.model.proxy.tunnel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import lombok.Getter;
import top.fateironist.constack.Container;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;

public class ServerTcpTunnelContext extends TunnelContext {
    @Getter
    private Channel requesterChannel;
    @Getter
    private Channel clientProxyChannel;

    public ServerTcpTunnelContext(Container parent, String tunnelId) {
        super(parent, tunnelId, TransportLayerProtocol.TCP);
    }

    public void writeToRequesterAndFlush(ByteBuf msg) {
        if (state() == State.ACTIVE) requesterChannel.writeAndFlush(msg.retain());
    }

    public void writeToClientProxyAndFlush(ByteBuf msg) {
        if (state() == State.ACTIVE) clientProxyChannel.writeAndFlush(msg.retain());
    }

    public void setRequesterChannel(Channel requesterChannel) {
        if (this.requesterChannel == null) {
            this.requesterChannel = requesterChannel;
            // Effect：关闭 requester 侧连接（本容器持有的 Netty Channel）
            effect(c -> requesterChannel.close());
        }
        tryOpen();
    }

    public void setClientProxyChannel(Channel clientProxyChannel) {
        if (this.clientProxyChannel == null) {
            this.clientProxyChannel = clientProxyChannel;
            // Effect：关闭 client-proxy 侧连接（本容器持有的 Netty Channel）
            effect(c -> clientProxyChannel.close());
        }
        tryOpen();
    }

    @Override
    protected boolean isBothEndsReady() {
        return requesterChannel != null && clientProxyChannel != null;
    }
}
