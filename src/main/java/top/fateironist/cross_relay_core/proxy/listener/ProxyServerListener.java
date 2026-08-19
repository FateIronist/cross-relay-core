package top.fateironist.cross_relay_core.proxy.listener;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import top.fateironist.cross_relay_core.Listener;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

public class ProxyServerListener implements Listener {

    public boolean beforeClientToServerChannelAccept(TransportLayerProtocol protocol, Channel channel) {
        return true;
    }

    public boolean beforeRequesterToServerChannelAccept(TransportLayerProtocol protocol, Channel channel) {
        return true;
    }

    public TunnelContext onClientToServerChannelRegister(String id, Channel channel) {
        return null;
    }

    public Future<Channel> onRequesterRequireTunnel(TunnelContext context, EventLoop eventLoop) {
        throw new UnsupportedOperationException("onRequesterRequireTunnel must be implemented");
    }

    public Future<?> closeRemoteTunnel(TunnelContext context) {
        throw new UnsupportedOperationException("closeRemoteTunnel must be implemented to use closeGracefully");
    }

    public void onTunnelEstablished(TunnelContext context) {

    }

    public void onTunnelClose(TunnelContext context) {

    }

    public void caughtTunnelException(TunnelContext context, Throwable cause) {

    }


}
