package top.fateironist.cross_relay_core.model.proxy.tunnel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import lombok.Getter;
import top.fateironist.constack.Container;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;

public class ClientTcpTunnelContext extends TunnelContext {
    @Getter
    private Channel serviceChannel;
    @Getter
    private Channel clientProxyChannel;

    public ClientTcpTunnelContext(Container parent, String tunnelId) {
        super(parent, tunnelId, TransportLayerProtocol.TCP);
    }

    public void writeToServiceAndFlush(ByteBuf msg) {
        if (state() == State.ACTIVE) serviceChannel.writeAndFlush(msg.retain());
    }

    public void writeToClientProxyAndFlush(ByteBuf msg) {
        if (state() == State.ACTIVE) clientProxyChannel.writeAndFlush(msg.retain());
    }

    public void setServiceChannel(Channel serviceChannel) {
        if (this.serviceChannel == null) {
            this.serviceChannel = serviceChannel;
            // Effect：关闭本地服务侧连接（本容器持有的 Netty Channel）
            effect(c -> serviceChannel.close());
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
        return serviceChannel != null && clientProxyChannel != null;
    }
}
