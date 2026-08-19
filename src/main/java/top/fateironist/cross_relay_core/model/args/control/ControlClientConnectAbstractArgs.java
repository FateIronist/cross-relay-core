package top.fateironist.cross_relay_core.model.args.control;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.AbstractOptions;

import java.util.function.Function;

@Getter
public class ControlClientConnectAbstractArgs extends AbstractArgs {
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
