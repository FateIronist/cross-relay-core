package top.fateironist.cross_relay_core.model.info;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.net.InetSocketAddress;

/**
 * 客户端身份信息：标识、认证凭证与来源地址，在 control 握手阶段经 SESSION_SECRET_ACK 上报给服务端，
 * 供服务端 beforePermit 认证以及后续代理准入裁决使用。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProxyClientInfo {
    // 客户端标识
    private String id;
    // 认证凭证，内容由上层业务定义与校验，核心层不解释其语义
    private String credentials;
    // 客户端来源地址；服务端侧由 channelActive 时的远端地址构造为占位值
    private InetSocketAddress address;
    // 客户端是否存活
    private boolean isAlive;
    // 本条身份信息最近一次更新的时间戳（毫秒）
    private long lastUpdatedTime = System.currentTimeMillis();

    /** 以来源地址构造并直接标记为存活，服务端在 channelActive 时用它建立占位身份 */
    public ProxyClientInfo(InetSocketAddress address) {
        this.address = address;
        this.isAlive = true;
    }

    /** 以凭证构造，客户端侧发起连接时用它携带自己的身份 */
    public ProxyClientInfo(String credentials) {
        this.credentials = credentials;
    }

    /**
     * 字段级合并：只把本对象中仍为 null 的字段（id / credentials / address）用入参补齐，已有值不被覆盖，最后刷新 lastUpdatedTime。
     * 服务端用它把客户端上报的身份合并到按远端地址建出的占位对象上；客户端用它合并 CONNECTION_PERMIT 中带回的信息。
     */
    public void setAdditional(ProxyClientInfo info) {
        if (id == null) id = info.id;
        if (credentials == null) credentials = info.credentials;
        if (address == null) address = info.address;
        lastUpdatedTime = System.currentTimeMillis();
    }
}
