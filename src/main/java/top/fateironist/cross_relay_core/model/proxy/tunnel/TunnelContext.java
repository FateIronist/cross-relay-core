package top.fateironist.cross_relay_core.model.proxy.tunnel;

import io.netty.channel.EventLoop;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import lombok.Getter;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.info.ProxyClientInfo;
import top.fateironist.cross_relay_core.model.info.ClientServiceInfo;
import top.fateironist.cross_relay_core.model.info.OriginalRequesterInfo;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;

import java.util.function.Consumer;
import java.util.function.Function;

@Getter
public abstract class TunnelContext {
    public static final AttributeKey<TunnelContext> KEY = AttributeKey.valueOf("tunnelContext");

    private String tunnelId;

    private TransportLayerProtocol transportLayerProtocol;

    private ClientServiceInfo clientServiceInfo;
    private ProxyClientInfo proxyClientInfo;
    private ProxyServerInfo proxyServerInfo;
    private OriginalRequesterInfo originalRequesterInfo;

    public TunnelContext(String tunnelId,
                                     TransportLayerProtocol transportLayerProtocol,
                                     ClientServiceInfo clientServiceInfo,
                                     ProxyClientInfo proxyClientInfo,
                                     ProxyServerInfo proxyServerInfo,
                                     OriginalRequesterInfo originalRequesterInfo
                                     ) {
        this.tunnelId = tunnelId;
        this.transportLayerProtocol = transportLayerProtocol;
        this.clientServiceInfo = clientServiceInfo;
        this.proxyClientInfo = proxyClientInfo;
        this.proxyServerInfo = proxyServerInfo;
        this.originalRequesterInfo = originalRequesterInfo;
    }

    public abstract Future<?> closeGracefully(Function<TunnelContext, Future<?>> closeRemote, EventLoop eventLoop);

    public abstract Future<?> closeLocal(EventLoop eventLoop);
}
