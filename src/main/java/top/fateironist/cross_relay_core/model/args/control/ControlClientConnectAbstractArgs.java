package top.fateironist.cross_relay_core.model.args.control;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.AbstractOptions;

import java.net.InetSocketAddress;
import java.util.function.Function;

@Getter
public class ControlClientConnectAbstractArgs extends AbstractArgs {
    /** 服务端控制通道地址，建连目标 */
    @Setter
    private InetSocketAddress serverControlAddress;
    private Options options;

    public ControlClientConnectAbstractArgs(Function<Options.OptionsBuilder, Options.OptionsBuilder> optionsBuilder) {
        options = optionsBuilder.apply(Options.builder()).build();
    }

    @Getter
    @Builder
    @AllArgsConstructor
    public static class Options extends AbstractOptions {
        @Builder.Default
        private long pingInterval = 5000; // ms
        @Builder.Default
        private long pingTimeout = 30000;
    }
}
