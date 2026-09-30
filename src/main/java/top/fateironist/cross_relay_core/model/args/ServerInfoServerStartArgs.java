package top.fateironist.cross_relay_core.model.args;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.function.Function;

/** UDP 元数据服务 ServerInfoServer 的启动参数；注意监听端口不由本参数配置，ServerInfoServer 内固定 bind 到常量 PORT = 3461 */
@Getter
public class ServerInfoServerStartArgs extends AbstractArgs {
    private Options options; // 构造时经 lambda 定制后 build 出的选项对象，ServerInfoServer.start 通过它取限速配置

    /**
     * 默认值的注入方式：外部传入的 optionsBuilder 接收一个已带 @Builder.Default 初值的 OptionsBuilder，
     * 只覆盖自己关心的字段，未被覆盖的字段保持字段声明处的默认值，最后 build 成不可变 Options。
     */
    public ServerInfoServerStartArgs(Function<Options.OptionsBuilder, Options.OptionsBuilder> optionsBuilder) {
        options = optionsBuilder.apply(Options.builder()).build();
    }

    /** ServerInfoServer 的选项：只含单 channel 级与全局级的读写限速，不含端口（端口为固定常量） */
    @Getter
    @Builder
    @AllArgsConstructor
    public static class Options extends AbstractOptions {
        @Builder.Default
        private long singleChannelReadLimit = 0;// byte/s，单 channel 读限速上限，0 表示不作单 channel 限速
        @Builder.Default
        private long singleChannelWriteLimit = 0;// byte/s，单 channel 写限速上限，0 表示不作单 channel 限速
        @Builder.Default
        private long globalChannelReadLimit = 0;// byte/s，全局读限速上限，0 表示不作全局限速
        @Builder.Default
        private long globalChannelWriteLimit = 0;// byte/s，全局写限速上限，0 表示不作全局限速

        /** 取实际生效的读限速：单 channel 限速优先，为 0（未配置）时回落到全局读限速 */
        public long getReadLimit() {
            return singleChannelReadLimit == 0L ? globalChannelReadLimit : singleChannelReadLimit;
        }

        /** 取实际生效的写限速：单 channel 限速优先，为 0（未配置）时回落到全局写限速 */
        public long getWriteLimit() {
            return singleChannelWriteLimit == 0L ? globalChannelWriteLimit : singleChannelWriteLimit;
        }
    }
}
