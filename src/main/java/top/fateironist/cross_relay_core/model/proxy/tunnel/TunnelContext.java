package top.fateironist.cross_relay_core.model.proxy.tunnel;

import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import lombok.Getter;
import lombok.Setter;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;

import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 隧道级上下文的抽象基类：一条隧道一个、一次性，隧道级的建立状态机内聚在此。
 * 职责是三件事——两端资源（channel/address）的配对与 OPEN 判定（tryOpen）、关闭时的本地清理（tunnelCloseHook）、
 * 以及关闭时对同一隧道对端的通知（closeRemoteTunnelHook）。
 * 两个钩子都由所属 ProxyContext 在建隧道时注入，因此子类无需自己编排关闭路径，只需补上"关掉本端资源"这一步。
 */
@Getter
public abstract class TunnelContext {
    // 绑定在数据面 channel 上的 AttributeKey：TCP 中继连接的 childChannel 靠它反查所属隧道——没有该 attr 的首个包即被识别为注册包
    public static final AttributeKey<TunnelContext> KEY = AttributeKey.valueOf("TunnelContext");

    // 形如 TUN-UUID；服务端生成后经 REQUIRE_CHANNEL 下发给客户端，两端以同一 id 配对同一条隧道
    protected final String tunnelId;
    // 隧道承载的协议，建隧道时由具体子类固定传入（TCP/UDP）
    protected final TransportLayerProtocol transportLayerProtocol;

    // 隧道状态机 INIT/OPEN/CLOSING/CLOSED：只有 tryOpen() 会置 OPEN；转发方法一律以它为准，故 half-open 阶段不会漏数据
    @Getter
    protected volatile TunnelStatus status = TunnelStatus.INIT;

    // 本端清理钩子，由 ProxyContext 注入（出 tunnelRegisterMap，UDP 版还会清地址路由表）；closeGracefully 与 closeLocal 都会调用
    @Setter
    protected Consumer<TunnelContext> tunnelCloseHook;
    // 对端通知钩子，由 ProxyContext 注入（经 control 通道发 TUNNEL_CLOSE）；仅 closeGracefully 调用，closeLocal 刻意不通知对端
    @Setter
    protected Consumer<TunnelContext> closeRemoteTunnelHook;

    /** 状态恒从 INIT 起步；两端资源随后由子类的 setXxx 逐步补齐，每次补齐都会触发 tryOpen() */
    public TunnelContext(String tunnelId, TransportLayerProtocol transportLayerProtocol) {
        this.tunnelId = tunnelId;
        this.transportLayerProtocol = transportLayerProtocol;
    }

    /**
     * 优雅关闭：置 CLOSING → 本地清理（tunnelCloseHook）→ 通知对端一起关闭（closeRemoteTunnelHook，即 TUNNEL_CLOSE）→ 置 CLOSED。
     * 语义是"两端同步释放"，故对端断开、内网服务断开、UDP 超时、proxy 下线等本端触发点都应走这里。
     * 注意基类本身不关闭 channel，各 TCP/UDP 子类会覆写本方法把 channel 的关闭合并进来。
     */
    public Future<?> closeGracefully() {
        status = TunnelStatus.CLOSING;

        tunnelCloseHook.accept(this);
        closeRemoteTunnelHook.accept(this);

        status = TunnelStatus.CLOSED;
        return DefaultEventLoopGroup.emptyFuture();
    }

    /**
     * 仅本端关闭：只做本地清理、不通知对端，用于异常兜底等"对端已不可达或不必再打扰"的场景。
     */
    public Future<?> closeLocal() {
        status = TunnelStatus.CLOSING;
        tunnelCloseHook.accept(this);
        status = TunnelStatus.CLOSED;
        return DefaultEventLoopGroup.emptyFuture();
    }

    /**
     * 两端资源齐备性判定：由子类的 setXxxChannel()/setXxx() 在每次赋值后自动调用，不对外暴露调用时机。
     * 齐备条件由各端各自定义（TCP 看双 channel，UDP 还要看双方地址），满足即把 status 置为 OPEN 并返回 true，未齐备则保持 INIT 并返回 false。
     * 各子类的可见性不完全一致（客户端 TCP 为 public，其余为 protected），但都只在继承体系内部被调用。
     */
    protected abstract boolean tryOpen();
}
