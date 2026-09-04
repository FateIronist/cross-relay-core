package top.fateironist.cross_relay_core.model.proxy;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Promise;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.control.ProxyControlEventEnum;
import top.fateironist.cross_relay_core.model.control.event.CommonInfo;
import top.fateironist.cross_relay_core.model.control.event.ControlEvent;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public abstract class ServerProxyContext extends ProxyContext{
    protected final Map<String, Promise<Object>> requesterWaitMap = new ConcurrentHashMap<>();

    public ServerProxyContext(String proxyId, TransportLayerProtocol protocol, List<ChannelHandlerContext> handlerContexts, ControlContext controlContext) {
        super(proxyId, protocol, handlerContexts, controlContext);
    }


    @Override
    protected Consumer<TunnelContext> tunnelCloseHook() {
        return new Consumer<TunnelContext>() {
            @Override
            public void accept(TunnelContext context) {
                ServerProxyContext.super.tunnelCloseHook().accept(context);
                requesterWaitMap.remove(context.getTunnelId());
            }
        };
    }

    public void requireChannel(String tunnelId, TransportLayerProtocol protocol) {
        ControlEvent<CommonInfo> controlEvent = new ControlEvent<>(ProxyControlEventEnum.REQUIRE_CHANNEL.getType(), new CommonInfo(tunnelId, proxyId, protocol));
        controlClient(controlEvent);
    }

}
