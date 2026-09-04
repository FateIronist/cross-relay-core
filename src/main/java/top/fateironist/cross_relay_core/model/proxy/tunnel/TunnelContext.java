package top.fateironist.cross_relay_core.model.proxy.tunnel;

import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import lombok.Getter;
import lombok.Setter;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;

import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

@Getter
public abstract class TunnelContext {
    public static final AttributeKey<TunnelContext> KEY = AttributeKey.valueOf("TunnelContext");

    protected final String tunnelId;
    protected final TransportLayerProtocol transportLayerProtocol;

    @Getter
    protected volatile TunnelStatus status = TunnelStatus.INIT;

    @Setter
    protected Consumer<TunnelContext> tunnelCloseHook;
    @Setter
    protected Consumer<TunnelContext> closeRemoteTunnelHook;

    public TunnelContext(String tunnelId, TransportLayerProtocol transportLayerProtocol) {
        this.tunnelId = tunnelId;
        this.transportLayerProtocol = transportLayerProtocol;
    }

    public Future<?> closeGracefully() {
        status = TunnelStatus.CLOSING;

        tunnelCloseHook.accept(this);
        closeRemoteTunnelHook.accept(this);

        status = TunnelStatus.CLOSED;
        return DefaultEventLoopGroup.emptyFuture();
    }

    public Future<?> closeLocal() {
        status = TunnelStatus.CLOSING;
        tunnelCloseHook.accept(this);
        status = TunnelStatus.CLOSED;
        return DefaultEventLoopGroup.emptyFuture();
    }

    protected abstract boolean tryOpen();
}
