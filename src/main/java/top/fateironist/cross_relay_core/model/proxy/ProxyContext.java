package top.fateironist.cross_relay_core.model.proxy;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;

@Getter
public abstract class ProxyContext {
    protected final String proxyId;
    protected TransportLayerProtocol transportLayerProtocol;
    protected final List<ChannelHandlerContext> channelHandlerContextList;
    protected final Map<String, TunnelContext> tunnelRegisterMap = new ConcurrentHashMap<>();

    public ProxyContext(String proxyId, TransportLayerProtocol protocol, List<ChannelHandlerContext> handlerContexts) {
        this.proxyId = proxyId;
        this.transportLayerProtocol = protocol;
        this.channelHandlerContextList = handlerContexts;
    }

    public TunnelContext newTunnelContext() {
        TunnelContext tunnelContext = createNewTunnelContext(generateTunnelId());
        tunnelContext.setTunnelCloseHook(tunnelCloseHook());

        tunnelRegisterMap.put(tunnelContext.getTunnelId(), tunnelContext);

        return tunnelContext;
    }

    public abstract TunnelContext createNewTunnelContext(String tunnelId);

    protected Consumer<TunnelContext> tunnelCloseHook() {
        return new Consumer<TunnelContext>() {
            @Override
            public void accept(TunnelContext context) {
                tunnelRegisterMap.remove(context.getTunnelId());
            }
        };
    };

    protected String generateTunnelId() {
        return "TUN-" + UUID.randomUUID().toString().replace("-","");
    };

    public Future<?> close(Function<ProxyContext, io.netty.util.concurrent.Future<?>> closeRemote) {
        return DefaultEventLoopGroup.newPromise(promise -> {
            try {
                closeRemote.apply(this).get();
            } catch (Exception e) {
                promise.setFailure(e);
            }
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
}
