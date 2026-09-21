package top.fateironist.cross_relay_core.model.control.event;

import lombok.Data;
import lombok.NoArgsConstructor;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;

@Data
@NoArgsConstructor
public class CommonInfo {
    private String tunnelId;
    private String proxyId;
    private TransportLayerProtocol protocol;

    public CommonInfo(String proxyId) {
        this.proxyId = proxyId;
    }

    public CommonInfo(String tunnelId, String proxyId) {
        this.tunnelId = tunnelId;
        this.proxyId = proxyId;
    }

    public CommonInfo(String tunnelId, String proxyId, TransportLayerProtocol protocol) {
        this.tunnelId = tunnelId;
        this.proxyId = proxyId;
        this.protocol = protocol;
    }
}
