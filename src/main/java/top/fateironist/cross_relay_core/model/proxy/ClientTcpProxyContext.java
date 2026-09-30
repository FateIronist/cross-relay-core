package top.fateironist.cross_relay_core.model.proxy;

import io.netty.channel.ChannelHandlerContext;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ClientTcpTunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

import java.util.List;

/**
 * 客户端 TCP 代理上下文：一个 proxyId 对应一个、跨隧道复用，由 ProxyTcpClient.createContext 以 computeIfAbsent 保证唯一。
 * 本类不额外维护状态，隧道的双连接配对全部由 ClientTcpTunnelContext 承担。
 */
public class ClientTcpProxyContext extends ClientProxyContext {
    /** 协议固定为 TCP，调用方只需给出 proxyId 与对 control 的依赖 */
    public ClientTcpProxyContext(String proxyId, List<ChannelHandlerContext> handlerContexts, ControlContext controlContext) {
        super(proxyId, TransportLayerProtocol.TCP, handlerContexts, controlContext);
    }

    /** 每收到一条 REQUIRE_CHANNEL 的隧道指令，就由 ProxyTcpClient 建一个"连内网服务 + 回连服务端"的双连接隧道 */
    @Override
    public TunnelContext createNewTunnelContext(String tunnelId) {
        return new ClientTcpTunnelContext(tunnelId);
    }
}
