package top.fateironist.cross_relay_core.model.info;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;

import java.net.InetSocketAddress;

/**
 * 原始请求者（外部访问者）的信息快照：来源地址、所用协议、存活状态与记录时间。
 * 已知问题（见设计文档第 6 节）：两参数构造器的形参未被真正使用——构造体内写成了 this.address = address 这类自赋值，
 * 其中的 address / protocol 会被解析为字段自身而非同名的形参，导致外部传入的地址与协议不会生效，相关字段保持为 null。
 * 此处仅作标注，不改动实现。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OriginalRequesterInfo {
    // 请求者标识
    private String id;
    // 请求者来源地址（IP:Port）
    private InetSocketAddress address;
    // 请求者使用的传输层协议
    private TransportLayerProtocol protocol;
    // 该请求者当前是否存活
    private boolean isAlive;
    // 本条信息记录的时间戳（毫秒）
    private long timestamp;

    /**
     * 按来源地址与协议构造，并初始化为存活状态、打上当前时间戳。
     * 注意：受上述已知问题影响，此构造器实际不会写入 address 与 protocol 字段。
     */
    public OriginalRequesterInfo(InetSocketAddress inetSocketAddress, TransportLayerProtocol transportLayerProtocol) {
        this.address = address;
        this.protocol = protocol;
        this.isAlive = true;
        this.timestamp = System.currentTimeMillis();
    }
}
