package top.fateironist.cross_relay_core.model.proxy;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import lombok.Getter;
import lombok.Setter;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.control.ProxyControlEventEnum;
import top.fateironist.cross_relay_core.model.control.event.CommonInfo;
import top.fateironist.cross_relay_core.model.control.event.ControlEvent;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

@Getter
public abstract class ProxyContext {
    public static final AttributeKey<ProxyContext> KEY = AttributeKey.valueOf("ProxyContext");

    protected final String proxyId;
    protected TransportLayerProtocol transportLayerProtocol;
    protected final List<ChannelHandlerContext> channelHandlerContextList;
    protected final ControlContext controlContext;
    protected final Map<String, TunnelContext> tunnelRegisterMap = new ConcurrentHashMap<>();

    @Setter
    protected Consumer<ProxyContext> closeProxyHook;

    public ProxyContext(String proxyId, TransportLayerProtocol protocol, List<ChannelHandlerContext> handlerContexts, ControlContext controlContext) {
        this.proxyId = proxyId;
        this.transportLayerProtocol = protocol;
        this.channelHandlerContextList = handlerContexts;
        this.controlContext = controlContext;

    }

    public TunnelContext newTunnelContext() {
        return newTunnelContext(generateTunnelId());
    }

    public TunnelContext newTunnelContext(String tunnelId) {
        TunnelContext tunnelContext = createNewTunnelContext(tunnelId);
        tunnelContext.setTunnelCloseHook(tunnelCloseHook());
        tunnelContext.setCloseRemoteTunnelHook(closeRemoteTunnelHook());

        tunnelRegisterMap.put(tunnelContext.getTunnelId(), tunnelContext);

        return tunnelContext;
    }

    public abstract TunnelContext createNewTunnelContext(String tunnelId);

    public TunnelContext getTunnelContext(String tunnelId) {
        return tunnelRegisterMap.get(tunnelId);
    }

    protected Consumer<TunnelContext> tunnelCloseHook() {
        return new Consumer<TunnelContext>() {
            @Override
            public void accept(TunnelContext context) {
                tunnelRegisterMap.remove(context.getTunnelId());
            }
        };
    };

    protected Consumer<TunnelContext> closeRemoteTunnelHook() {
        return new Consumer<TunnelContext>() {
            @Override
            public void accept(TunnelContext context) {
                closeRemoteTunnel(context.getTunnelId());
            }
        };
    };

    public static String generateProxyId() {
        return "PXY-" + UUID.randomUUID().toString().replace("-","");
    }

    protected static String generateTunnelId() {
        return "TUN-" + UUID.randomUUID().toString().replace("-","");
    };

    public Future<?> close() {
        return DefaultEventLoopGroup.newPromise(promise -> {
            closeProxyHook.accept(this);
            closeRemoteProxy();

            tunnelRegisterMap.forEach((key, value) -> {
                try {
                    value.closeGracefully().get();
                } catch (Exception e) {
                    promise.setFailure(e);
                }
            });

            channelHandlerContextList.forEach(ctx -> {
                try {
                    ctx.close().get();
                } catch (Exception e) {
                    promise.setFailure(e);
                }
            });

            promise.setSuccess(null);
        });
    };

    public void addHandlerContext(ChannelHandlerContext ctx) {
        channelHandlerContextList.add(ctx);
    }

    public <T> void controlClient(ControlEvent<T> controlEvent) {
        controlContext.writeAndFlush(controlEvent);
    }

    public void closeRemoteTunnel(String tunnelId) {
        ControlEvent<CommonInfo> controlEvent = new ControlEvent<>(ProxyControlEventEnum.TUNNEL_CLOSE.getType(), new CommonInfo(tunnelId, proxyId));
        controlClient(controlEvent);
    }

    public void closeRemoteProxy() {
        ControlEvent<CommonInfo> controlEvent = new ControlEvent<>(ProxyControlEventEnum.PROXY_CLOSE.getType(), new CommonInfo(proxyId));
        controlClient(controlEvent);
    }
}
