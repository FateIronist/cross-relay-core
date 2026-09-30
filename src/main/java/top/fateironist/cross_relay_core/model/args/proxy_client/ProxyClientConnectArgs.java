package top.fateironist.cross_relay_core.model.args.proxy_client;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.args.AbstractOptions;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.info.OriginalRequesterInfo;
import top.fateironist.cross_relay_core.model.info.ProxyClientInfo;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;

import java.net.InetSocketAddress;
import java.util.function.Function;

/**
 * Proxy 客户端（数据面）连接参数：描述"为哪条隧道、以哪个协议、连哪个内网服务"，
 * 供 ProxyClient.connect 分流到 ProxyTcpClient / ProxyUdpClient 后建立隧道。
 * 直接持有 ControlContext，是 proxy → control 单向依赖的体现（proxy 借此回发 TUNNEL_CLOSE / PROXY_CLOSE 等控制事件），control 自身不感知 proxy。
 */
@Getter
public class ProxyClientConnectArgs extends AbstractArgs {
    private String tunnelId; // 本次要建立的隧道 id（TUN-&lt;uuid&gt;）：客户端据此 newTunnelContext，并在回连服务端的首包 JSON 注册包中回传，供服务端完成 requester ↔ clientProxy 配对
    private String proxyId; // 隧道所属 proxyContext 的 id（PXY-&lt;uuid&gt;）：服务端收到注册包后按它把回连 channel 路由到对应的 ServerProxyContext
    private InetSocketAddress serviceAddress; // 内网真实服务地址；TCP 用于 serviceProxyBootstrap.connect，UDP 同时作为 ClientUdpProxyContext.addressContextMap 的键
    private TransportLayerProtocol protocol; // 传输层协议，决定 ProxyClient 走 TCP 还是 UDP 实现
    private ControlContext controlContext; // 已完成握手（permit=true）的控制通道上下文，proxy 侧回发控制事件的唯一出口
    private Options options; // 构造时经 lambda 定制后 build 出的选项对象

    /**
     * 默认值的注入方式：外部传入的 optionsBuilder 接收一个带 @Builder.Default 初值的 OptionsBuilder 并返回定制后的 builder，
     * 最后 build 成不可变 Options。注意本类 Options 的字段均无 @Builder.Default，未显式设置时即为 null。
     */
    public ProxyClientConnectArgs(String tunnelId, String proxyId, InetSocketAddress serviceAddress, TransportLayerProtocol protocol, ControlContext controlContext,Function<Options.OptionsBuilder, Options.OptionsBuilder> optionsBuilder) {
        this.options = optionsBuilder.apply(Options.builder()).build();
        this.tunnelId = tunnelId;
        this.proxyId = proxyId;
        this.serviceAddress = serviceAddress;
        this.protocol = protocol;
        this.controlContext = controlContext;
    }

    /** Proxy 客户端选项：随隧道下发的客户端身份与原始请求者信息；当前源码中无读取点，属预留字段 */
    @Getter
    @Builder
    @AllArgsConstructor
    public static class Options extends AbstractOptions {
        private ProxyClientInfo proxyClientInfo; // 发起该隧道的客户端身份信息，默认 null
        private OriginalRequesterInfo originalRequesterInfo; // 触发该隧道的原始请求者信息，默认 null；OriginalRequesterInfo 自身构造器存在字段赋值错位问题，使用前需先修复
    }
}
