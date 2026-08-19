package top.fateironist.cross_relay_core.model.info;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;

import java.net.InetSocketAddress;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClientServiceInfo {
    private String id;
    private String serviceName;
    private String description;
    private InetSocketAddress address;
    private boolean isAvailable;
    private TransportLayerProtocol transportLayerProtocol;
    private long lastUpdatedTime;

    public boolean checkAddress() {
        return address != null && (address.getAddress().isLoopbackAddress() || address.getAddress().isAnyLocalAddress()) && address.getPort() > 0;
    }

    public ClientServiceInfo(InetSocketAddress address, TransportLayerProtocol transportLayerProtocol) {
        this.address = address;
        this.transportLayerProtocol = transportLayerProtocol;
    }
}
