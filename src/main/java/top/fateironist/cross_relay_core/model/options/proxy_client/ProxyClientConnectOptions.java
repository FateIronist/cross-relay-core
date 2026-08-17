package top.fateironist.cross_relay_core.model.options.proxy_client;

import lombok.Builder;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.info.OriginalRequesterInfo;
import top.fateironist.cross_relay_core.model.info.ProxyClientInfo;
import top.fateironist.cross_relay_core.model.options.Options;

@Builder
public class ProxyClientConnectOptions implements Options {
    public String tunnelId;
    public TransportLayerProtocol protocol;

    public ProxyClientInfo proxyClientInfo;
    public OriginalRequesterInfo originalRequesterInfo;
}
