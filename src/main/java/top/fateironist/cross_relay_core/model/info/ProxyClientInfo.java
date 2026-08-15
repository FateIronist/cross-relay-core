package top.fateironist.cross_relay_core.model.info;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.net.InetSocketAddress;

@Data
@NoArgsConstructor
public class ProxyClientInfo {
    private String id;
    private String credentials;
    private InetSocketAddress address;
    private boolean isAlive;
    private long lastUpdatedTime = System.currentTimeMillis();

    public ProxyClientInfo(InetSocketAddress address) {
        this.address = address;
        this.isAlive = true;
    }

    public void setAdditional(ProxyClientInfo info) {
        if (id == null) id = info.id;
        if (credentials == null) credentials = info.credentials;
        if (address == null) address = info.address;
        lastUpdatedTime = System.currentTimeMillis();
    }
}
