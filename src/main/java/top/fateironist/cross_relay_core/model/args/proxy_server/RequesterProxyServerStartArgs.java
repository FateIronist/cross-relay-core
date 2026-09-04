package top.fateironist.cross_relay_core.model.args.proxy_server;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.info.ClientServiceInfo;
import top.fateironist.cross_relay_core.model.info.ProxyClientInfo;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.AbstractOptions;

import java.util.function.Function;

@Getter
public class RequesterProxyServerStartArgs extends AbstractArgs {
    private ControlContext controlContext;
    private Options options;

    public RequesterProxyServerStartArgs(ControlContext controlContext, Function<Options.OptionsBuilder, Options.OptionsBuilder> optionsBuilder) {
        this.controlContext = controlContext;
        options = optionsBuilder.apply(Options.builder()).build();
    }

    @Getter
    @Builder
    @AllArgsConstructor
    public static class Options extends AbstractOptions {
        private ClientServiceInfo clientServiceInfo;
        private ProxyClientInfo proxyClientInfo;
        private ProxyServerInfo proxyServerInfo;

        @Builder.Default
        private int maxUdpReceiveBuffer = 64 * 1024;
        @Builder.Default
        private int maxUdpSendBuffer = 64 * 1024;

        @Builder.Default
        private int maxTcpConnections = 6;

        @Builder.Default
        private int requesterTimeout = 30000;
    }
}
