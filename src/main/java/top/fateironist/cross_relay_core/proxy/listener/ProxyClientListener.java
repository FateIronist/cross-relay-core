package top.fateironist.cross_relay_core.proxy.listener;

import io.netty.util.concurrent.Future;
import top.fateironist.cross_relay_core.Listener;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

public class ProxyClientListener implements Listener {

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
