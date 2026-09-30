package top.fateironist.cross_relay_core.model.proxy;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import lombok.Getter;
import lombok.Setter;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.control.ProxyControlEventEnum;
import top.fateironist.cross_relay_core.model.control.event.CommonInfo;
import top.fateironist.cross_relay_core.model.control.event.ControlEvent;
import top.fateironist.cross_relay_core.model.proxy.tunnel.TunnelContext;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * proxy 级上下文的抽象基类：一个 proxyId 对应一个、跨隧道复用，是隧道建立编排与 control 交互的收口处。
 * 隧道本身的创建、状态机与转发在 TunnelContext 体系，本类只负责：隧道注册表、生命周期钩子的注入、
 * 以及把 REQUIRE_CHANNEL / TUNNEL_CLOSE / PROXY_CLOSE 经 control 通道发出。
 * 关键约束：引导类（ProxyTcpServer/ProxyUdpServer/ProxyTcpClient/ProxyUdpClient）不直接触碰 control 通道，
 * 一切控制事件都必须走 controlClient(event)。
 */
@Getter
public abstract class ProxyContext {
    // 绑定在 channel 上的 AttributeKey：数据面 handler 凭它反查所属 proxy 上下文（子连接到达、UDP 收包时用于定位 proxyId 对应的上下文）
    public static final AttributeKey<ProxyContext> KEY = AttributeKey.valueOf("ProxyContext");

    // 形如 PXY-UUID，由 generateProxyId() 生成；一个 proxyId 在一端唯一对应一个本类实例（引导类以 computeIfAbsent 保证），跨隧道复用；注册包与 PROXY_CLOSE 都靠它定位
    protected final String proxyId;
    // 本 proxy 承载的传输层协议，requireChannel 时随 CommonInfo 一并告知客户端
    protected TransportLayerProtocol transportLayerProtocol;
    // 本 proxy 名下的 channel 上下文（服务端为专属的 requester 监听 channel，客户端为数据面 channel），close() 时据此逐个关闭；由 addHandlerContext() 在 channel 就绪时登记
    protected final List<ChannelHandlerContext> channelHandlerContextList;
    // 对 control 层的唯一依赖点：控制事件经它回写；proxy 层不持有 ControlClient/ControlServer
    protected final ControlContext controlContext;
    // tunnelId -> TunnelContext，本 proxy 下所有活跃隧道；建隧道时入表，关闭时由 tunnelCloseHook 出表；跨线程访问故用 ConcurrentHashMap
    protected final Map<String, TunnelContext> tunnelRegisterMap = new ConcurrentHashMap<>();

    // 生命周期钩子，由引导类在 createContext 时注入（把本上下文从各自的 proxyContextMap 移除）；close() 会直接调用它，故下线前必须已注入
    @Setter
    protected Consumer<ProxyContext> closeProxyHook;

    /** 由引导类的 createContext 调用；隧道构造由子类的 createNewTunnelContext 决定，本类只负责编排其生命周期 */
    public ProxyContext(String proxyId, TransportLayerProtocol protocol, List<ChannelHandlerContext> handlerContexts, ControlContext controlContext) {
        this.proxyId = proxyId;
        this.transportLayerProtocol = protocol;
        this.channelHandlerContextList = handlerContexts;
        this.controlContext = controlContext;

    }

    /** tunnelId 由本端自行生成的建隧道入口：服务端在外部 requester 到达时调用 */
    public TunnelContext newTunnelContext() {
        return newTunnelContext(generateTunnelId());
    }

    /**
     * 创建隧道上下文并完成隧道生命周期的"接线"：挂上 tunnelCloseHook（关闭时出 tunnelRegisterMap）与 closeRemoteTunnelHook（关闭时经 control 通知对端 TUNNEL_CLOSE），随后入表。
     * 钩子在创建瞬间就绑定好，因此后续所有关闭路径（requester/service 断开、UDP 超时、proxy 下线）都不需要再编排。
     * 带 tunnelId 的重载供客户端使用：tunnelId 由服务端经 REQUIRE_CHANNEL 下发，两端以此配对同一条隧道。
     */
    public TunnelContext newTunnelContext(String tunnelId) {
        TunnelContext tunnelContext = createNewTunnelContext(tunnelId);
        tunnelContext.setTunnelCloseHook(tunnelCloseHook());
        tunnelContext.setCloseRemoteTunnelHook(closeRemoteTunnelHook());

        tunnelRegisterMap.put(tunnelContext.getTunnelId(), tunnelContext);

        return tunnelContext;
    }

    /** 由子类决定具体「端 + 协议」的隧道实现，是本体系唯一的隧道构造扩展点 */
    public abstract TunnelContext createNewTunnelContext(String tunnelId);

    /** 按 tunnelId 反查本 proxy 下的隧道：注册包配对（registerClientProxy）与数据面转发前的定位都依赖它；查不到返回 null */
    public TunnelContext getTunnelContext(String tunnelId) {
        return tunnelRegisterMap.get(tunnelId);
    }

    /** 隧道关闭时的本地清理钩子：默认只把隧道移出 tunnelRegisterMap；子类可覆写以追加地址表等清理 */
    protected Consumer<TunnelContext> tunnelCloseHook() {
        return new Consumer<TunnelContext>() {
            @Override
            public void accept(TunnelContext context) {
                tunnelRegisterMap.remove(context.getTunnelId());
            }
        };
    };

    /** 隧道关闭时的对端通知钩子：经 control 通道发 TUNNEL_CLOSE，使对端同步释放同一条隧道 */
    protected Consumer<TunnelContext> closeRemoteTunnelHook() {
        return new Consumer<TunnelContext>() {
            @Override
            public void accept(TunnelContext context) {
                closeRemoteTunnel(context.getTunnelId());
            }
        };
    };

    /** 生成形如 PXY-UUID 的代理 id，供引导类在无外部指定时自造上下文 */
    public static String generateProxyId() {
        return "PXY-" + UUID.randomUUID().toString().replace("-","");
    }

    /** 生成形如 TUN-UUID 的隧道 id */
    protected static String generateTunnelId() {
        return "TUN-" + UUID.randomUUID().toString().replace("-","");
    };

    /**
     * 下线整个 proxy：先执行 closeProxyHook（引导类借此把本上下文移出 proxyContextMap，防止后续注册包再命中已关闭的上下文），
     * 再向对端发 PROXY_CLOSE，然后逐条隧道 closeGracefully（级联关闭 + 通知对端），最后逐个关闭名下 channel。
     * 注意：本方法体经 DefaultEventLoopGroup.newPromise 提交到共享 eventLoop 线程执行，内部又以 get() 同步等待各步完成，会阻塞该 eventLoop 线程；
     * 故引导类的 close/shutdown 都另起虚拟线程来调用它。
     */
    public Future<?> close() {
        return DefaultEventLoopGroup.newPromise(promise -> {
            closeProxyHook.accept(this);
            closeRemoteProxy();

            tunnelRegisterMap.forEach((key, value) -> {
                try {
                    value.closeGracefully().get();
                } catch (Exception e) {
                    promise.setFailure(e);
                }
            });

            channelHandlerContextList.forEach(ctx -> {
                try {
                    ctx.close().get();
                } catch (Exception e) {
                    promise.setFailure(e);
                }
            });

            promise.setSuccess(null);
        });
    };

    /** 把 channel 上下文登记进本 proxy，close() 时会逐个关闭；注意服务端侧构造时传入的是 List.of(...) 不可变列表，对它调用本方法会抛 UnsupportedOperationException */
    public void addHandlerContext(ChannelHandlerContext ctx) {
        channelHandlerContextList.add(ctx);
    }

    /**
     * 与 control 交互的唯一收敛入口：proxy 体系的全部控制事件（REQUIRE_CHANNEL / TUNNEL_CLOSE / PROXY_CLOSE）都经此发出，
     * 由 ControlContext 附加递增 msgId 后走加密 control 通道；引导类不触碰 control 通道，control 事件的内容构造也只存在于本体系内。
     */
    public <T> void controlClient(ControlEvent<T> controlEvent) {
        controlContext.writeAndFlush(controlEvent);
    }

    /**
     * 通知对端关闭指定隧道（TUNNEL_CLOSE），是 closeRemoteTunnelHook 的实际实现，由对端 TunnelContext.closeGracefully() 触发；
     * 它只负责发通知，本地隧道的清理由本端的关闭路径完成。
     */
    public void closeRemoteTunnel(String tunnelId) {
        ControlEvent<CommonInfo> controlEvent = new ControlEvent<>(ProxyControlEventEnum.TUNNEL_CLOSE.getType(), new CommonInfo(tunnelId, proxyId));
        controlClient(controlEvent);
    }

    /** 通知对端本 proxy 整体下线（PROXY_CLOSE），由 close() 调用；对端据此释放该 proxyId 下的全部隧道 */
    public void closeRemoteProxy() {
        ControlEvent<CommonInfo> controlEvent = new ControlEvent<>(ProxyControlEventEnum.PROXY_CLOSE.getType(), new CommonInfo(proxyId));
        controlClient(controlEvent);
    }
}
