package top.fateironist.cross_relay_core.model.args.proxy_server;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.AbstractOptions;

import java.util.function.Function;

/**
 * client-proxy 端（服务端接收内网客户端回连的监听）启动参数，供 ProxyServer.start 及其下 ProxyTcpServer / ProxyUdpServer 使用。
 * 拓扑约束：client-proxy 监听全局仅有一个、所有客户端共享——所有客户端的回连都进入这同一个监听，再由首包 JSON 注册包中的 proxyId 路由到各自的 ProxyContext。
 */
@Getter
public class ClientProxyServerStartArgs extends AbstractArgs {
    private Options options; // 构造时经 lambda 定制后 build 出的选项对象

    /**
     * 默认值的注入方式：外部传入的 optionsBuilder 接收一个已带 @Builder.Default 初值的 OptionsBuilder，
     * 只覆盖自己关心的字段，未被覆盖的字段保持字段声明处的默认值，最后 build 成不可变 Options。
     */
    public ClientProxyServerStartArgs(Function<Options.OptionsBuilder, Options.OptionsBuilder> optionsBuilder) {
        options = optionsBuilder.apply(Options.builder()).build();
    }

    /** client-proxy 端选项：子服务器启停开关、共享监听端口与连接/缓冲上限 */
    @Getter
    @Builder
    @AllArgsConstructor
    public static class Options extends AbstractOptions {
        @Builder.Default
        private boolean enableProxyTcp = true; // 是否启动 TCP 的 client-proxy 监听（ProxyServer 据此决定是否构造 ProxyTcpServer）
        @Builder.Default
        private boolean enableProxyUdp = true; // 是否启动 UDP 的 client-proxy 监听（ProxyServer 据此决定是否构造 ProxyUdpServer）
        @Builder.Default
        private int clientProxyPort = 3418; // client-proxy 监听端口，TCP 与 UDP 共用同一端口号且互不冲突；设为 0 表示由系统随机分配
        @Builder.Default
        private int maxTcpConnections = 128 * 6; // 128*6=768，作为 SO_BACKLOG 传给 client-proxy 的 ServerBootstrap，并非在线连接数硬上限
        @Builder.Default
        private int maxUdpReceiveBuffer = 64 * 1024; // 64KB，UDP client-proxy channel 的 SO_RCVBUF
        @Builder.Default
        private int maxUdpSendBuffer = 64 * 1024; // 64KB，UDP client-proxy channel 的 SO_SNDBUF
    }
}
