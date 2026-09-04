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
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProxyServerInfo {
    // 服务端自己随机生成并通过url分发；若为手动填入则通过serverControl端口回填。
    private String id;

    private URL metaDataUrl;

    private String serverName;

    private DeploymentMode deploymentMode;

    // 若为单机部署，则无需注册中心，直连服务器
    private ProxyServerAddress address;

    // 若为分布式部署则需注册中心，从注册中心动态获取，负载均衡
    private Set<InetSocketAddress> registerAddresses;

    private boolean enableProxyTcp = false;
    private boolean enableProxyUdp = false;

    private boolean isAvailable;

    // 延迟
    private Long latency;
    private Long lastPing = System.currentTimeMillis();

    // 最后更新时间
    private Long lastUpdateTime = System.currentTimeMillis();

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ProxyServerAddress {
        private InetSocketAddress serverControlAddress;
        private InetSocketAddress serverProxyRequestAddress;
        private InetSocketAddress serverInfoServerAddress;

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

        @JsonIgnore
        public boolean isSufficient() {
            return serverControlAddress != null && serverProxyRequestAddress != null && serverInfoServerAddress != null;
        }
    }

    public static ProxyServerInfo createSingleProxyServerInfo(String serverName, ProxyServerAddress address, boolean enableProxyTcp, boolean enableProxyUdp) {
        return ProxyServerInfo.builder()
                .serverName(serverName)
                .address(address)
                .enableProxyTcp(enableProxyTcp)
                .enableProxyUdp(enableProxyUdp)
                .build();
    }

    public static ProxyServerInfo createDistributedProxyServerInfo(String serverName, Set<InetSocketAddress> registerAddresses, boolean enableProxyTcp, boolean enableProxyUdp) {
        return ProxyServerInfo.builder()
                .serverName(serverName)
                .registerAddresses(registerAddresses)
                .enableProxyTcp(enableProxyTcp)
                .enableProxyUdp(enableProxyUdp)
                .build();
    }


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

    public void receivePong(long lastPingTime) {
        long now = System.currentTimeMillis();
        lastPing = lastPingTime;
        latency = now - lastPingTime;
        isAvailable = true;

        lastUpdateTime = now;
    }

    public void getMetaDataFromNetwork() {
        // TODO: 获取元数据
    }

    private static final int BUFFER_SIZE = 65535;
    
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

    public InetSocketAddress getProxyServerAddress() {
        return null; // TODO
    }

}
