package top.fateironist.cross_relay_core.model.info;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;

import java.net.InetSocketAddress;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OriginalRequesterInfo {
    private String id;
    private InetSocketAddress address;
    private TransportLayerProtocol protocol;
    private boolean isAlive;
    private long timestamp;

    public OriginalRequesterInfo(InetSocketAddress inetSocketAddress, TransportLayerProtocol transportLayerProtocol) {
        this.address = address;
        this.protocol = protocol;
        this.isAlive = true;
        this.timestamp = System.currentTimeMillis();
    }
}
