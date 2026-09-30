package top.fateironist.cross_relay_core.model;

/** 服务端部署模式，供 ProxyServerInfo 描述自身是如何被寻址的：单机直连，或经注册中心做分布式寻址 */
public enum DeploymentMode {
    Single, // 单机部署：无需注册中心，客户端直接用 ProxyServerInfo.address 中的三地址（control / proxyRequest / infoServer）连接
    Distributed; // 分布式部署：客户端经 ProxyServerInfo.registerAddresses 指向的注册中心动态获取服务端地址并做负载均衡（当前仅声明，寻址逻辑未实现）
}
