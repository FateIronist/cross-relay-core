package top.fateironist.cross_relay_core.model.args;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.function.Function;

@Getter
public class ServerInfoServerStartArgs extends AbstractArgs {
    private Options options;

    public ServerInfoServerStartArgs(Function<Options.OptionsBuilder, Options.OptionsBuilder> optionsBuilder) {
        options = optionsBuilder.apply(Options.builder()).build();
    }

    @Getter
    @Builder
    @AllArgsConstructor
    public static class Options extends AbstractOptions {
        @Builder.Default
        private long singleChannelReadLimit = 0;// byte/s
        @Builder.Default
        private long singleChannelWriteLimit = 0;// byte/s
        @Builder.Default
        private long globalChannelReadLimit = 0;// byte/s
        @Builder.Default
        private long globalChannelWriteLimit = 0;// byte/s

        public long getReadLimit() {
            return singleChannelReadLimit == 0L ? globalChannelReadLimit : singleChannelReadLimit;
        }

        public long getWriteLimit() {
            return singleChannelWriteLimit == 0L ? globalChannelWriteLimit : singleChannelWriteLimit;
        }
    }
}
