package top.fateironist.cross_relay_core.model.proxy.tunnel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.info.ClientServiceInfo;
import top.fateironist.cross_relay_core.model.info.OriginalRequesterInfo;
import top.fateironist.cross_relay_core.model.info.ProxyClientInfo;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;

public class ClientTunnelContext extends TunnelContext {
    private Channel clientToServiceChannel;
    private Channel clientToServerChannel;

    public ClientTunnelContext(String tunnelId,
                               TransportLayerProtocol transportLayerProtocol,
                               ClientServiceInfo clientServiceInfo,
                               ProxyClientInfo proxyClientInfo,
                               ProxyServerInfo proxyServerInfo,
                               OriginalRequesterInfo originalRequesterInfo) {
        super(tunnelId, transportLayerProtocol, clientServiceInfo, proxyClientInfo, proxyServerInfo, originalRequesterInfo);
    }

    public Channel getClientToServiceChannel() {
        return clientToServiceChannel;
    }

    public void setClientToServiceChannel(Channel clientToServiceChannel) {
        if (getClientToServiceChannel() == null) {
            this.clientToServiceChannel = clientToServiceChannel;
        }
    }

    public Channel getClientToServerChannel() {
        return clientToServerChannel;
    }

    public void setClientToServerChannel(Channel clientToServerChannel) {
        if (getClientToServerChannel() == null) {
            this.clientToServerChannel = clientToServerChannel;
        }
    }

    public void writeToServiceAndFlush(ByteBuf byteBuf) {
        byteBuf.retain();
        clientToServiceChannel.writeAndFlush(byteBuf);
    }

    public void writeToServerAndFlush(ByteBuf byteBuf) {
        byteBuf.retain();
        clientToServerChannel.writeAndFlush(byteBuf);
    }
}
