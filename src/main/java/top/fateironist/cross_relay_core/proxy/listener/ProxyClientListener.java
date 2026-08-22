package top.fateironist.cross_relay_core.proxy.listener;

import io.netty.util.concurrent.Future;
import top.fateironist.cross_relay_core.Listener;
import top.fateironist.cross_relay_core.model.proxy.tunnel.OldTunnelContext;

public class ProxyClientListener implements Listener {

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
