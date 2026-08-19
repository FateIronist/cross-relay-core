package top.fateironist.cross_relay_core.model.args.proxy_client;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.args.AbstractOptions;
import top.fateironist.cross_relay_core.model.info.OriginalRequesterInfo;
import top.fateironist.cross_relay_core.model.info.ProxyClientInfo;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;

import java.util.function.Function;

@Getter
public class ProxyClientConnectArgs extends AbstractArgs {
    private String tunnelId;
    private TransportLayerProtocol protocol;
    private Options options;

    public ProxyClientConnectArgs(String tunnelId, TransportLayerProtocol protocol, Function<Options.OptionsBuilder, Options.OptionsBuilder> optionsBuilder) {
        this.options = optionsBuilder.apply(Options.builder()).build();
        this.tunnelId = tunnelId;
        this.protocol = protocol;
    }

    @Getter
    @Builder
    @AllArgsConstructor
    public static class Options extends AbstractOptions {
        private ProxyClientInfo proxyClientInfo;
        private OriginalRequesterInfo originalRequesterInfo;
    }
}
