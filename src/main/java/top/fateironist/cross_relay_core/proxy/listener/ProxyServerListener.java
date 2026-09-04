package top.fateironist.cross_relay_core.proxy.listener;

import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import top.fateironist.cross_relay_core.Listener;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

public class ProxyServerListener implements Listener {

    public boolean beforeClientToServerConnectionAccept(TransportLayerProtocol protocol, Channel serverChannel, Object remoteConnection) {
        return true;
    }

    public boolean beforeRequesterToServerConnectionAccept(TransportLayerProtocol protocol, Channel serverChannel, Object remoteConnection) {
        return true;
    }

    public void onTunnelEstablished(TunnelContext context) {

    }

    public void onTunnelClose(TunnelContext context) {

    }

    public void caughtTunnelException(TunnelContext context, Throwable cause) {

    }


}
