package top.fateironist.cross_relay_core.proxy.listener;

import io.netty.channel.Channel;
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

    public TunnelContext onClientToServerChannelRegister(String id) {
        return null;
    }

    public Future<Channel> onRequesterRequireTunnel(TunnelContext context) {
        return null;
    }

    public Future<?> closeRemoteTunnel(TunnelContext context) {
        return null;
    }

    public void onTunnelClose(TunnelContext context) {

    }


}
