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

/**
 * requester 端（服务端面向外部请求者的监听）启动参数，供 ProxyTcpServer.startTcpRequesterProxyServer / ProxyUdpServer.startUdpRequesterProxyServer 使用。
 * 拓扑约束：该监听由 ProxyContext 在运行时按需创建、且与 ProxyContext 一一对应——每个客户端申请的 proxyContext 各有专属的一条 requester 监听，
 * 服务该 proxyContext 下的所有隧道；端口以 bind(0) 随机分配，不对外固定。
 */
@Getter
public class RequesterProxyServerStartArgs extends AbstractArgs {
    private ControlContext controlContext; // 该 requester 监听所属 proxyContext 的控制通道上下文：构造 ServerProxyContext 时传入，requireChannel 经它向客户端下发 REQUIRE_CHANNEL
    private Options options; // 构造时经 lambda 定制后 build 出的选项对象

    /**
     * 默认值的注入方式：外部传入的 optionsBuilder 接收一个已带 @Builder.Default 初值的 OptionsBuilder，
     * 只覆盖自己关心的字段，未被覆盖的字段保持字段声明处的默认值，最后 build 成不可变 Options。
     */
    public RequesterProxyServerStartArgs(ControlContext controlContext, Function<Options.OptionsBuilder, Options.OptionsBuilder> optionsBuilder) {
        this.controlContext = controlContext;
        options = optionsBuilder.apply(Options.builder()).build();
    }

    /** requester 端选项：该监听所代理的内网服务与相关身份信息，以及缓冲/连接/超时上限；三个信息类字段均无 @Builder.Default，未显式设置时为 null */
    @Getter
    @Builder
    @AllArgsConstructor
    public static class Options extends AbstractOptions {
        private ClientServiceInfo clientServiceInfo; // 该 requester 监听所代理的内网服务描述（地址 + 协议）；当前源码中无读取点，属预留字段，默认 null
        private ProxyClientInfo proxyClientInfo; // 该代理归属的客户端身份信息，默认 null；当前源码中无读取点
        private ProxyServerInfo proxyServerInfo; // 服务端自身信息，默认 null；当前源码中无读取点
        @Builder.Default
        private int maxUdpReceiveBuffer = 64 * 1024; // 64KB，UDP requester channel 的 SO_RCVBUF
        @Builder.Default
        private int maxUdpSendBuffer = 64 * 1024; // 64KB，UDP requester channel 的 SO_SNDBUF
        @Builder.Default
        private int maxTcpConnections = 6; // 作为 requester 的 ServerBootstrap 的 SO_BACKLOG（单个代理预期并发不高，故远小于 client-proxy 端的 768）
        @Builder.Default
        private int requesterTimeout = 30000; // ms，原意图为 requester 端空闲超时；对应的 IdleStateHandler 因缺少 IdleEventHandler 消费而无效，已在 ProxyUdpServer.java:164-165 注释停用
    }
}
