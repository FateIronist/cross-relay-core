package top.fateironist.cross_relay_core.model.options.proxy_server;

import lombok.Builder;
import top.fateironist.cross_relay_core.model.options.Options;

@Builder
public class ClientProxyServerStartOptions implements Options {
    public boolean enableProxyTcp;
    public boolean enableProxyUdp;

    public int clientProxyPort;
    public int maxTcpConnections;
    public int maxUdpReceiveBuffer;
    public int maxUdpSendBuffer;
}
