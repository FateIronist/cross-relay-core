package top.fateironist.cross_relay_core.model.args.proxy_server;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.AbstractOptions;

import java.util.function.Function;

@Getter
public class ClientProxyServerStartArgs extends AbstractArgs {
    private Options options;

    public ClientProxyServerStartArgs(Function<Options.OptionsBuilder, Options.OptionsBuilder> optionsBuilder) {
        options = optionsBuilder.apply(Options.builder()).build();
    }

    @Getter
    @Builder
    @AllArgsConstructor
    public static class Options extends AbstractOptions {
        @Builder.Default
        private boolean enableProxyTcp = true;
        @Builder.Default
        private boolean enableProxyUdp = true;
        @Builder.Default
        private int clientProxyPort = 3418;
        @Builder.Default
        private int maxTcpConnections = 128 * 6;
        @Builder.Default
        private int maxUdpReceiveBuffer = 64 * 1024;
        @Builder.Default
        private int maxUdpSendBuffer = 64 * 1024;
    }
}
