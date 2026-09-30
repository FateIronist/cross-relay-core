package top.fateironist.cross_relay_core.model.control;

/**
 * 代理业务层事件：承载隧道与代理的调度语义（请求建立通道、关闭隧道、下线代理），是控制面与数据面的耦合点。
 * 由 ProxyContext 体系经 ControlContext.writeAndFlush 发到加密 control 通道上，接收方按 type 分发处理。
 * 注意 REGISTER_PROXY 与 REGISTER_PROXY_ACK 在库内没有发送点——核心层不负责“注册代理”的编排，由上层业务通过 listener 的 addEventHandler 自行实现请求-应答。
 */
public enum ProxyControlEventEnum implements ControlEventEnum{
    // 客户端→服务端（约定流向）：请求在服务端建立代理监听；库内无发送点，由上层业务经 listener 注册的 handler 发出
    REGISTER_PROXY,
    // 服务端→客户端（约定流向）：注册代理的结果应答；同样库内无发送点，由上层业务自行实现
    REGISTER_PROXY_ACK,
    // 服务端→客户端：服务端收到 requester 请求、需要客户端回连建隧道时发出，body 为 CommonInfo{tunnelId, proxyId, protocol}，发送点在 ServerProxyContext.requireChannel()；
    // 客户端侧拦截该指令并驱动 ProxyTcpClient/ProxyUdpClient 的逻辑尚未实现，枚举已预留
    REQUIRE_CHANNEL,
    // 双向：通知对端某条隧道关闭，body 为 CommonInfo{tunnelId, proxyId}，发送点在 ProxyContext.closeRemoteTunnel()
    TUNNEL_CLOSE,
    // 双向：通知对端某个代理整体下线，body 为 CommonInfo{proxyId}，发送点在 ProxyContext.closeRemoteProxy()
    PROXY_CLOSE;

    @Override
    public String getType() {
        return this.toString();
    }
}
