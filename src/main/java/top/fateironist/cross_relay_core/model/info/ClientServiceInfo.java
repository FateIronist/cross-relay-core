package top.fateironist.cross_relay_core.model.info;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;

import java.net.InetSocketAddress;

/**
 * 客户端侧被代理的内网服务描述：要暴露给服务端访问的那个本地服务（地址、协议、可用性等）。
 * 关键约束见 checkAddress()——地址必须回环或本地，保证代理层不会跨越到不可信网络。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClientServiceInfo {
    // 服务标识
    private String id;
    // 服务名称，供人阅读与展示
    private String serviceName;
    // 服务描述
    private String description;
    // 服务监听地址（IP:Port），即客户端本地真实服务的位置
    private InetSocketAddress address;
    // 该服务当前是否可用
    private boolean isAvailable;
    // 该服务使用的传输层协议（TCP/UDP），决定走哪套 Proxy 实现
    private TransportLayerProtocol transportLayerProtocol;
    // 该信息最近一次更新的时间戳（毫秒）
    private long lastUpdatedTime;

    /**
     * 校验服务地址是否合法：地址非空、端口大于 0，且 IP 必须是回环地址或本地任意地址（isAnyLocalAddress）。
     * 这是代理层安全边界的一部分——拒绝把外部地址登记为内网服务，避免中继被当作通向外部网络的跳板。
     */
    public boolean checkAddress() {
        return address != null && (address.getAddress().isLoopbackAddress() || address.getAddress().isAnyLocalAddress()) && address.getPort() > 0;
    }

    /** 仅填充地址与协议的便捷构造器，其余字段留空（调用方需自行补全可用性等状态） */
    public ClientServiceInfo(InetSocketAddress address, TransportLayerProtocol transportLayerProtocol) {
        this.address = address;
        this.transportLayerProtocol = transportLayerProtocol;
    }
}
