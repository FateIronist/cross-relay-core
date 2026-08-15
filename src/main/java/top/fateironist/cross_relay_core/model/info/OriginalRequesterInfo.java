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
    private InetSocketAddress inetSocketAddress;
    private TransportLayerProtocol transportLayerProtocol;
    private boolean isAlive;
    private long timestamp;
}
