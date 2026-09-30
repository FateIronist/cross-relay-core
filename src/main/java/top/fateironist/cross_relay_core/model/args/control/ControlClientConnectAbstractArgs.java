package top.fateironist.cross_relay_core.model.args.control;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.AbstractOptions;

import java.net.InetSocketAddress;
import java.util.function.Function;

/** control 客户端连接参数：携带服务端 control 地址与心跳选项，供 ControlClient.connect 建立并保活加密控制通道 */
@Getter
public class ControlClientConnectAbstractArgs extends AbstractArgs {
    private InetSocketAddress serverControlAddress; // 服务端 control 通道地址（服务端默认监听 3416）；ControlClient.connect 直接用它发起 TCP 连接。本类未提供构造参数或 setter，源码中无赋值点，需由子类或上层补全
    private Options options; // 构造时经 lambda 定制后 build 出的选项对象

    /**
     * 默认值的注入方式：外部传入的 optionsBuilder 接收一个已带 @Builder.Default 初值的 OptionsBuilder，
     * 只覆盖自己关心的字段，未被覆盖的字段保持字段声明处的默认值，最后 build 成不可变 Options。
     */
    public ControlClientConnectAbstractArgs(Function<Options.OptionsBuilder, Options.OptionsBuilder> optionsBuilder) {
        options = optionsBuilder.apply(Options.builder()).build();
    }

    /** control 客户端选项：心跳发送周期与读空闲超时 */
    @Getter
    @Builder
    @AllArgsConstructor
    public static class Options extends AbstractOptions {
        @Builder.Default
        private long pingInterval = 5000; // ms，客户端 PING 定时器周期；定时器在收到 SESSION_SECRET_KEY 后启动，但仅 permit 之后才真正发包（ControlClient.java:111-117）
        @Builder.Default
        private long pingTimeout = 30000; // ms，读空闲超时，交由 IdleStateHandler 触发 ControlClientListener.onTimeOut（默认关闭连接）
    }
}
