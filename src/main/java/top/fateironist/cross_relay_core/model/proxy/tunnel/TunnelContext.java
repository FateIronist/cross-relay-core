package top.fateironist.cross_relay_core.model.proxy.tunnel;

import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import lombok.Getter;
import lombok.Setter;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;

import java.util.function.Consumer;
import java.util.function.Function;

@Getter
public abstract class TunnelContext {
    protected final String tunnelId;
    protected final TransportLayerProtocol transportLayerProtocol;

    @Setter
    protected Consumer<TunnelContext> tunnelCloseHook;

    public TunnelContext(String tunnelId, TransportLayerProtocol transportLayerProtocol) {
        this.tunnelId = tunnelId;
        this.transportLayerProtocol = transportLayerProtocol;
    }

    public abstract Future<?> closeGracefully(Function<TunnelContext, Future<?>> closeRemote);
    public abstract Future<?> closeLocal();
}
