package top.fateironist.cross_relay_core.model.proxy.tunnel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import lombok.Getter;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;

import java.util.function.Consumer;
import java.util.function.Function;

public class ServerTcpTunnelContext extends TunnelContext {
    @Getter
    private Channel requesterChannel;
    @Getter
    private Channel clientProxyChannel;

    public ServerTcpTunnelContext(String tunnelId) {
        super(tunnelId, TransportLayerProtocol.TCP);
    }

    @Override
    public Future<?> closeGracefully() {
        return DefaultEventLoopGroup.combine(super.closeGracefully(), requesterChannel.close(), clientProxyChannel.close());
    }

    @Override
    public Future<?> closeLocal() {
        return DefaultEventLoopGroup.combine(super.closeLocal(), requesterChannel.close(), clientProxyChannel.close());
    }

    public void writeToRequesterAndFlush(ByteBuf msg) {
        if (status == TunnelStatus.OPEN) requesterChannel.writeAndFlush(msg.retain());
    }

    public void writeToClientProxyAndFlush(ByteBuf msg) {
        if (status == TunnelStatus.OPEN) clientProxyChannel.writeAndFlush(msg.retain());
    }

    public void setRequesterChannel(Channel requesterChannel) {
        if (this.requesterChannel == null) this.requesterChannel = requesterChannel;
        tryOpen();
    }

    public void setClientProxyChannel(Channel clientProxyChannel) {
        if (this.clientProxyChannel == null) this.clientProxyChannel = clientProxyChannel;
        tryOpen();
    }

    @Override
    protected boolean tryOpen() {
        if (requesterChannel != null && clientProxyChannel != null) {
            status = TunnelStatus.OPEN;
            return true;
        }
        return false;
    }
}
