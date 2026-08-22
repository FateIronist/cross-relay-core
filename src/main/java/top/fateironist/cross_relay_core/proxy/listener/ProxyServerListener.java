package top.fateironist.cross_relay_core.proxy.listener;

import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import top.fateironist.cross_relay_core.Listener;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.proxy.tunnel.OldTunnelContext;

public class ProxyServerListener implements Listener {

    public boolean beforeClientToServerConnectionAccept(TransportLayerProtocol protocol, Channel serverChannel, Object remoteConnection) {
        return true;
    }

    public boolean beforeRequesterToServerConnectionAccept(TransportLayerProtocol protocol, Channel serverChannel, Object remoteConnection) {
        return true;
    }

    public void onRequesterRequireTunnel(OldTunnelContext context) {
        throw new UnsupportedOperationException("onRequesterRequireTunnel must be implemented");
    }

    public Future<?> closeRemoteTunnel(OldTunnelContext context) {
        throw new UnsupportedOperationException("closeRemoteTunnel must be implemented to use closeGracefully");
    }

    public void onTunnelEstablished(OldTunnelContext context) {

    }

    public void onTunnelClose(OldTunnelContext context) {

    }

    public void caughtTunnelException(OldTunnelContext context, Throwable cause) {

    }


}
