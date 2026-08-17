package top.fateironist.cross_relay_core.model.options.proxy_server;

import lombok.Builder;
import top.fateironist.cross_relay_core.model.info.ClientServiceInfo;
import top.fateironist.cross_relay_core.model.info.ProxyClientInfo;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;
import top.fateironist.cross_relay_core.model.options.Options;

@Builder
public class RequesterProxyServerStartOptions implements Options {
    public ClientServiceInfo clientServiceInfo;
    public ProxyClientInfo proxyClientInfo;
    public ProxyServerInfo proxyServerInfo;
}
