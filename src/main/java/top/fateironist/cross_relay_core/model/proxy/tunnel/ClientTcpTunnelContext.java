package top.fateironist.cross_relay_core.model.proxy.tunnel.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import lombok.Getter;
import lombok.Setter;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelStatus;

import java.util.function.Function;

public class ClientTcpTunnelContext extends TunnelContext {
    @Getter
    private Channel serviceChannel;
    @Getter
    private Channel clientProxyChannel;

    public ClientTcpTunnelContext(String tunnelId) {
        super(tunnelId, TransportLayerProtocol.TCP);
    }

    @Override
    public Future<?> closeGracefully(Function<TunnelContext, Future<?>> closeRemote) {
        return DefaultEventLoopGroup.combine(super.closeGracefully(closeRemote), serviceChannel.close(), clientProxyChannel.close());
    }

    @Override
    public Future<?> closeLocal() {
        return DefaultEventLoopGroup.combine(super.closeLocal(), serviceChannel.close(), clientProxyChannel.close());
    }

    public void writeToServiceAndFlush(ByteBuf msg) {
        if (status == TunnelStatus.OPEN) serviceChannel.writeAndFlush(msg.retain());
    }

    public void writeToClientProxyAndFlush(ByteBuf msg) {
        if (status == TunnelStatus.OPEN) clientProxyChannel.writeAndFlush(msg.retain());
    }

    public void setServiceChannel(Channel serviceChannel) {
        if (this.serviceChannel == null) this.serviceChannel = serviceChannel;
        tryOpen();
    }

    public void setClientProxyChannel(Channel clientProxyChannel) {
        if (this.clientProxyChannel == null) this.clientProxyChannel = clientProxyChannel;
        tryOpen();
    }

    @Override
    public boolean tryOpen() {
        if (serviceChannel != null && clientProxyChannel != null) {
            status = TunnelStatus.OPEN;
            return true;
        }

        return false;
    }
}
