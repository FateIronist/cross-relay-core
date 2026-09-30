package top.fateironist.cross_relay_core.model.args.control;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.AbstractOptions;

import java.util.function.Function;

/** control 服务端启动参数：控制通道自身的监听端口与连接/超时上限，供 ControlServer.start 使用 */
@Getter
public class ControlServerStartArgs extends AbstractArgs {
    private Options options; // 构造时经 lambda 定制后 build 出的选项对象

    /**
     * 默认值的注入方式：外部传入的 optionsBuilder 接收一个已带 @Builder.Default 初值的 OptionsBuilder，
     * 只覆盖自己关心的字段，未被覆盖的字段保持字段声明处的默认值，最后 build 成不可变 Options。
     */
    public ControlServerStartArgs(Function<Options.OptionsBuilder, Options.OptionsBuilder> optionsBuilder) {
        options = optionsBuilder.apply(Options.builder()).build();
    }

    /** control 服务端选项：监听端口、accept 队列长度与读空闲超时 */
    @Getter
    @Builder
    @AllArgsConstructor
    public static class Options extends AbstractOptions {
        @Builder.Default
        private int port = 3416; // 控制通道监听端口，客户端 ControlClientConnectAbstractArgs.serverControlAddress 需与此一致
        @Builder.Default
        private int maxConnections = 128; // 作为 SO_BACKLOG 传给 ServerBootstrap，即已完成三次握手但尚未被 accept 的队列长度，并非同时在线的连接数上限
        @Builder.Default
        private long pingTimeout = 30000; // ms，读空闲超时，交由 IdleStateHandler 触发 ControlServerListener.onTimeOut（默认关闭该会话）
    }
}
