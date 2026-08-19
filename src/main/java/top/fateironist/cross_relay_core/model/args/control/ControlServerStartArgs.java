package top.fateironist.cross_relay_core.model.args.control;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.AbstractOptions;

import java.util.function.Function;

@Getter
public class ControlServerStartArgs extends AbstractArgs {
    private Options options;

    public ControlServerStartArgs(Function<Options.OptionsBuilder, Options.OptionsBuilder> optionsBuilder) {
        options = optionsBuilder.apply(Options.builder()).build();
    }

    @Getter
    @Builder
    @AllArgsConstructor
    public static class Options extends AbstractOptions {
        @Builder.Default
        private int port = 3416;
        @Builder.Default
        private int maxConnections = 128;
        @Builder.Default
        private long pingTimeout = 30000; // ms
    }
}
