package top.fateironist.cross_relay_core.model.info;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import top.fateironist.cross_relay_core.model.DeploymentMode;
import top.fateironist.cross_relay_core.model.control.event.ControlEvent;
import top.fateironist.cross_relay_core.model.control.ControlProtocolEventEnum;
import top.fateironist.cross_relay_core.util.JsonUtil;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Set;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
/**
 * 服务端元数据：服务端名称、部署模式，以及客户端访问服务端所需的三个地址（control / proxyRequest / infoServer）。
 * 客户端必须先取得它才能发起 control 连接——库内提供 UDP 发现作为备选手段（见 getMetaDataFromServerInfoServer），
 * 握手时服务端也会随 CONNECTION_PERMIT 下发自己的实例，客户端以 setAdditional 做字段级合并。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProxyServerInfo {
    // 服务端自己随机生成并通过url分发；若为手动填入则通过serverControl端口回填。
    private String id;

    // 元数据的分发地址（如 HTTP 端点等外部渠道），用于客户端在连 control 之前获取本对象
    private URL metaDataUrl;

    // 服务端名称
    private String serverName;

    // 部署模式（单机/分布式），决定寻址方式：单机直连地址，分布式需经注册中心
    private DeploymentMode deploymentMode;

    // 若为单机部署，则无需注册中心，直连服务器
    private ProxyServerAddress address;

    // 若为分布式部署则需注册中心，从注册中心动态获取，负载均衡
    private Set<InetSocketAddress> registerAddresses;

    // 该服务端是否提供 TCP 代理能力
    private boolean enableProxyTcp = false;
    // 该服务端是否提供 UDP 代理能力
    private boolean enableProxyUdp = false;

    // 该服务端当前是否可达，由探测结果更新
    private boolean isAvailable;

    // 延迟
    private Long latency;
    private Long lastPing = System.currentTimeMillis();

    // 最后更新时间
    private Long lastUpdateTime = System.currentTimeMillis();

    /** 服务端三地址集合，客户端连接 control 前必须三项齐备（见 isSufficient） */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ProxyServerAddress {
        // control 控制通道地址（默认端口 3416），客户端在此完成握手
        private InetSocketAddress serverControlAddress;
        // proxy 请求地址（默认端口 3418），客户端回连建立隧道时连此
        private InetSocketAddress serverProxyRequestAddress;
        // UDP 元数据发现服务地址（默认端口 3461）
        private InetSocketAddress serverInfoServerAddress;

        /** 字段级合并：仅补齐本对象中为 null 的地址，已有值不覆盖 */
        public void setAdditional(ProxyServerAddress info) {
            if (serverControlAddress == null) {
                serverControlAddress = info.serverControlAddress;
            }
            if (serverProxyRequestAddress == null) {
                serverProxyRequestAddress = info.serverProxyRequestAddress;
            }
            if (serverInfoServerAddress == null) {
                serverInfoServerAddress = info.serverInfoServerAddress;
            }
        }

        /** 三个地址是否都已具备，缺一不可；客户端借此判断元数据是否已足够发起连接 */
        @JsonIgnore
        public boolean isSufficient() {
            return serverControlAddress != null && serverProxyRequestAddress != null && serverInfoServerAddress != null;
        }
    }

    /** 单机部署工厂：直接给定服务端三地址，无需注册中心 */
    public static ProxyServerInfo createSingleProxyServerInfo(String serverName, ProxyServerAddress address, boolean enableProxyTcp, boolean enableProxyUdp) {
        return ProxyServerInfo.builder()
                .serverName(serverName)
                .address(address)
                .enableProxyTcp(enableProxyTcp)
                .enableProxyUdp(enableProxyUdp)
                .build();
    }

    /** 分布式部署工厂：给出注册中心地址集合，具体服务端地址由注册中心动态获取并做负载均衡 */
    public static ProxyServerInfo createDistributedProxyServerInfo(String serverName, Set<InetSocketAddress> registerAddresses, boolean enableProxyTcp, boolean enableProxyUdp) {
        return ProxyServerInfo.builder()
                .serverName(serverName)
                .registerAddresses(registerAddresses)
                .enableProxyTcp(enableProxyTcp)
                .enableProxyUdp(enableProxyUdp)
                .build();
    }


    /**
     * 字段级合并：仅补齐本对象中为 null 的字段，已有值不被覆盖；address 已存在时进一步对其做字段级合并，最后刷新 lastUpdateTime。
     * 客户端收到 CONNECTION_PERMIT 后用它把服务端下发的信息合并进本地这份元数据。
     */
    public void setAdditional(ProxyServerInfo info) {
        if (id == null) this.id = info.id;
        if (serverName == null) this.serverName = info.serverName;
        if (deploymentMode == null) this.deploymentMode = info.deploymentMode;
        if (address == null) {
            this.address = info.address;
        } else if (info.address != null) {
            address.setAdditional(info.address);
        }
        if (registerAddresses == null) this.registerAddresses = info.registerAddresses;
        lastUpdateTime = System.currentTimeMillis();
    }

    /**
     * 收到对端 PONG 时更新延迟统计：把 lastPing 回填为 PING 的发送时刻、latency 记为本次往返耗时，
     * 同时标记服务端可达并刷新 lastUpdateTime。
     */
    public void receivePong(long lastPingTime) {
        long now = System.currentTimeMillis();
        lastPing = lastPingTime;
        latency = now - lastPingTime;
        isAvailable = true;

        lastUpdateTime = now;
    }

    /** 预留的元数据获取入口（非 UDP 渠道），当前为空实现，库内无调用点 */
    public void getMetaDataFromNetwork() {
        // TODO: 获取元数据
    }

    // UDP 应答接收缓冲区大小，取 UDP 数据报的理论上限
    private static final int BUFFER_SIZE = 65535;
    
    /**
     * UDP 发现（备选寻址手段，当前库内无调用点）：仅单机部署下有效，向 address.serverInfoServerAddress 以原生 DatagramSocket
     * 发出 SERVER_INFO 查询并解析应答，成功则经 setAdditional 合并元数据并置 isAvailable；失败时按 1s 递增退避重试，
     * 达到 maxRetry 后置 isAvailable=false 并抛 RuntimeException，中断则恢复中断标志后抛出。
     */
    public void getMetaDataFromServerInfoServer(int maxRetry, int timeout) {
        if (deploymentMode == DeploymentMode.Single) {
            if (address == null || address.serverInfoServerAddress == null){
                throw new RuntimeException("ServerInfoServer address is null");
            }

            InetSocketAddress infoAddress = address.serverInfoServerAddress;
            for (int attempt = 1; attempt <= maxRetry; attempt++) {
                long startTime = System.currentTimeMillis();
                try (DatagramSocket socket = new DatagramSocket(infoAddress)) {
                    socket.setSoTimeout(timeout);

                    // 构造请求
                    ControlEvent<Void> request = new ControlEvent<>(ControlProtocolEventEnum.SERVER_INFO.getType(), null);
                    byte[] requestData = JsonUtil.OBJECT_MAPPER.writeValueAsBytes(request);

                    // 发送请求
                    DatagramPacket sendPacket = new DatagramPacket(requestData, requestData.length);
                    socket.send(sendPacket);

                    // 接收响应
                    byte[] buffer = new byte[BUFFER_SIZE];
                    DatagramPacket receivePacket = new DatagramPacket(buffer, buffer.length);
                    socket.receive(receivePacket);

                    // 记录延迟
                    long latency = System.currentTimeMillis() - startTime;
                    this.latency = latency;
                    this.lastUpdateTime = System.currentTimeMillis();

                    // 反序列化响应
                    byte[] responseData = new byte[receivePacket.getLength()];
                    System.arraycopy(buffer, 0, responseData, 0, receivePacket.getLength());
                    ControlEvent<ProxyServerInfo> response = JsonUtil.OBJECT_MAPPER.readValue(
                            responseData,
                            new TypeReference<ControlEvent<ProxyServerInfo>>() {}
                    );

                    if (response.getType().equals(ControlProtocolEventEnum.SERVER_INFO.getType()) && response.getBody() != null) {
                        ProxyServerInfo serverInfo = response.getBody();
                        setAdditional(serverInfo);
                        this.isAvailable = true;
                        return;
                    }
                } catch (Exception e) {
                    if (attempt == maxRetry) {
                        this.isAvailable = false;
                        throw new RuntimeException("Failed to fetch metadata from ServerInfoServer after " + maxRetry + " attempts", e);
                    }
                    // 重试前等待
                    try {
                        Thread.sleep(1000L * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during retry", ie);
                    }
                }
            }
        } else {
            // TODO: 分布式部署，从注册中心动态获取
        }
    }

    /** 返回客户端应当连接的 proxy 地址；当前为 TODO 占位实现，恒返回 null，尚未完成 */
    public InetSocketAddress getProxyServerAddress() {
        return null; // TODO
    }

}
