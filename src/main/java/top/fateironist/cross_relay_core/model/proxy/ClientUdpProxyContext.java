package top.fateironist.cross_relay_core.model.proxy;

import io.netty.channel.ChannelHandlerContext;
import top.fateironist.constack.Container;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.ClientUdpTunnelContext;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClientUdpProxyContext extends ClientProxyContext {
    /** 地址路由索引：对端地址（service/server-proxy）-> 隧道（UDP 无连接，按地址寻址隧道） */
    private final Map<InetSocketAddress, ClientUdpTunnelContext> addressContextMap = new ConcurrentHashMap<>();

    public ClientUdpProxyContext(Container parent, String proxyId, List<ChannelHandlerContext> handlerContexts, ControlContext controlContext) {
        super(parent, proxyId, TransportLayerProtocol.UDP, handlerContexts, controlContext);
    }

    @Override
    public TunnelContext createNewTunnelContext(Container parent, String tunnelId) {
        return new ClientUdpTunnelContext(parent, tunnelId);
    }

    /** 受控方法：隧道端点确立时登记地址路由索引（由隧道容器回调） */
    public void registerTunnelAddress(InetSocketAddress address, ClientUdpTunnelContext tunnel) {
        addressContextMap.put(address, tunnel);
    }

    /** 受控清理：隧道销毁时注销地址路由索引 */
    @Override
    public void unregisterTunnel(String tunnelId) {
        TunnelContext tunnel = tunnelRegisterMap.get(tunnelId);
        if (tunnel instanceof ClientUdpTunnelContext udpTunnel) {
            if (udpTunnel.getServerProxyAddress() != null) {
                addressContextMap.remove(udpTunnel.getServerProxyAddress(), udpTunnel);
            }
            if (udpTunnel.getServiceAddress() != null) {
                addressContextMap.remove(udpTunnel.getServiceAddress(), udpTunnel);
            }
        }
        super.unregisterTunnel(tunnelId);
    }

    public ClientUdpTunnelContext getTunnelContext(InetSocketAddress address) {
        return addressContextMap.get(address);
    }
}
