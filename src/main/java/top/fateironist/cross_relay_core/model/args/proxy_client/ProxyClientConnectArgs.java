package top.fateironist.cross_relay_core.model.args.proxy_client;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.args.AbstractOptions;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.info.OriginalRequesterInfo;
import top.fateironist.cross_relay_core.model.info.ProxyClientInfo;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;

import java.net.InetSocketAddress;
import java.util.function.Function;

@Getter
public class ProxyClientConnectArgs extends AbstractArgs {
    private String tunnelId;
    private String proxyId;
    private InetSocketAddress serviceAddress;
    private TransportLayerProtocol protocol;
    private ControlContext controlContext;
    private Options options;

    public ProxyClientConnectArgs(String tunnelId, String proxyId, InetSocketAddress serviceAddress, TransportLayerProtocol protocol, ControlContext controlContext,Function<Options.OptionsBuilder, Options.OptionsBuilder> optionsBuilder) {
        this.options = optionsBuilder.apply(Options.builder()).build();
        this.tunnelId = tunnelId;
        this.proxyId = proxyId;
        this.serviceAddress = serviceAddress;
        this.protocol = protocol;
        this.controlContext = controlContext;
    }

    @Getter
    @Builder
    @AllArgsConstructor
    public static class Options extends AbstractOptions {
        private ProxyClientInfo proxyClientInfo;
        private OriginalRequesterInfo originalRequesterInfo;
    }
}
