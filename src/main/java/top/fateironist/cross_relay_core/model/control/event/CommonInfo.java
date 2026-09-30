package top.fateironist.cross_relay_core.model.control.event;

import lombok.Data;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;

/**
 * 代理业务事件的通用 body 载体，聚合 tunnelId / proxyId / protocol 三元信息，同时也是客户端回连服务端时首包 JSON 注册包的内容。
 * 服务端收到注册包后据此定位挂起的 tunnel 并完成配对；不同事件按需只填其中部分字段。
 */
@Data
public class CommonInfo {
    // 隧道标识，形如 TUN-<uuid>，一条隧道对应一次性的 requester 与内网服务之间的连接
    private String tunnelId;
    // 代理标识，形如 PXY-<uuid>，一个代理可跨多条隧道复用
    private String proxyId;
    // 该隧道使用的传输层协议，服务端据此选择 TCP 或 UDP 的 Proxy 实现
    private TransportLayerProtocol protocol;

    /** 仅带 proxyId，用于 PROXY_CLOSE 这类代理级事件 */
    public CommonInfo(String proxyId) {
        this.proxyId = proxyId;
    }

    /** 带隧道与代理信息，用于 TUNNEL_CLOSE 以及数据面的注册包 */
    public CommonInfo(String tunnelId, String proxyId) {
        this.tunnelId = tunnelId;
        this.proxyId = proxyId;
    }

    /** 最完整的形式，用于 REQUIRE_CHANNEL：客户端需据 protocol 决定回连搭建 TCP 还是 UDP 通道 */
    public CommonInfo(String tunnelId, String proxyId, TransportLayerProtocol protocol) {
        this.tunnelId = tunnelId;
        this.proxyId = proxyId;
        this.protocol = protocol;
    }
}
