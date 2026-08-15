package top.fateironist.cross_relay_core.model.tunnel;

import io.netty.util.AttributeKey;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.info.ProxyClientInfo;
import top.fateironist.cross_relay_core.model.info.ClientServiceInfo;
import top.fateironist.cross_relay_core.model.info.OriginalRequesterInfo;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;

public abstract class TunnelContext {
    public static final AttributeKey<TunnelContext> KEY = AttributeKey.valueOf("tunnelContext");

    private String tunnelId;

    private TransportLayerProtocol transportLayerProtocol;

    private ClientServiceInfo clientServiceInfo;
    private ProxyClientInfo proxyClientInfo;
    private ProxyServerInfo proxyServerInfo;
    private OriginalRequesterInfo originalRequesterInfo;

    public TunnelContext(String tunnelId,
                                     TransportLayerProtocol transportLayerProtocol,
                                     ClientServiceInfo clientServiceInfo,
                                     ProxyClientInfo proxyClientInfo,
                                     ProxyServerInfo proxyServerInfo,
                                     OriginalRequesterInfo originalRequesterInfo
                                     ) {
        this.tunnelId = tunnelId;
        this.transportLayerProtocol = transportLayerProtocol;
        this.clientServiceInfo = clientServiceInfo;
        this.proxyClientInfo = proxyClientInfo;
        this.proxyServerInfo = proxyServerInfo;
        this.originalRequesterInfo = originalRequesterInfo;
    }

//    public abstract Future<Boolean> close();

    public String getTunnelId() {
        return tunnelId;
    }

    public TransportLayerProtocol getTransportLayerProtocol() {
        return transportLayerProtocol;
    }

    public ClientServiceInfo getClientServiceInfo() {
        return clientServiceInfo;
    }

    public ProxyClientInfo getClientProxyInfo() {
        return proxyClientInfo;
    }

    public ProxyServerInfo getProxyServerInfo() {
        return proxyServerInfo;
    }

    public OriginalRequesterInfo getOriginalRequesterInfo() {
        return originalRequesterInfo;
    }

}
