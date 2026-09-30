package top.fateironist.cross_relay_core.model;

/** 传输层协议枚举，用于在 Proxy 层分流：ProxyClient 据此二选一（ProxyTcpClient / ProxyUdpClient），ProxyServer 据此启停子服务器 */
public enum TransportLayerProtocol {
    TCP, // 面向连接的字节流：一条隧道对应独立的两条连接，回连后以首包 JSON 注册包完成隧道配对
    UDP; // 无连接数据报：channel 多连接复用，按 InetSocketAddress 区分对端，首包即注册、惰性建隧道
}
