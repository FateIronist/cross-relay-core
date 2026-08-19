package top.fateironist.cross_relay_core.model.args.proxy_server;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import top.fateironist.cross_relay_core.model.info.ClientServiceInfo;
import top.fateironist.cross_relay_core.model.info.ProxyClientInfo;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.AbstractOptions;

import java.util.function.Function;

@Getter
public class RequesterProxyServerStartArgs extends AbstractArgs {
    private Options options;

    public RequesterProxyServerStartArgs(Function<Options.OptionsBuilder, Options.OptionsBuilder> optionsBuilder) {
        options = optionsBuilder.apply(Options.builder()).build();
    }

    @Getter
    @Builder
    @AllArgsConstructor
    public static class Options extends AbstractOptions {
        private ClientServiceInfo clientServiceInfo;
        private ProxyClientInfo proxyClientInfo;
        private ProxyServerInfo proxyServerInfo;
    }
}
