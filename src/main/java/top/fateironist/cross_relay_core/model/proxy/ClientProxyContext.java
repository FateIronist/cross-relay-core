package top.fateironist.cross_relay_core.model.proxy;

import io.netty.channel.ChannelHandlerContext;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.control.ControlContext;

import java.util.List;

/**
 * 客户端侧 ProxyContext 的公共基类。当前不含任何字段与方法，仅作为"客户端这一侧"的继承锚点，
 * 使 TCP/UDP 两个客户端实现共用同一分支，并与服务端的 ServerProxyContext 分支对称。
 */
public abstract class ClientProxyContext extends ProxyContext{
    /** 构造参数原样透传给基类，协议由更下层的 TCP/UDP 实现固定传入 */
    public ClientProxyContext(String proxyId, TransportLayerProtocol protocol, List<ChannelHandlerContext> handlerContexts, ControlContext controlContext) {
        super(proxyId, protocol, handlerContexts, controlContext);
    }
}
