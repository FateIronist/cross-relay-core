package top.fateironist.cross_relay_core.model.proxy;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import lombok.Getter;
import top.fateironist.constack.Container;
import top.fateironist.constack.DependencyPolicy;
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
import java.util.concurrent.TimeUnit;

/**
 * 代理容器：一个代理实例对应一个 ProxyContext，数据流经的 Netty Channel 与隧道均由本容器持有。
 * 结构面：作为顶层 Server/Client 容器的子容器存在；
 * 生命周期面：经 dependOn(CASCADE_DISPOSE) 绑定控制连接，控制连接死亡则本代理级联销毁；
 * 数据面：身份信息经依赖边精确取得，隧道经父子链 get() 向上查找。
 */
@Getter
public abstract class ProxyContext extends Container {
    public static final AttributeKey<ProxyContext> KEY = AttributeKey.valueOf("ProxyContext");

    protected final String proxyId;
    protected TransportLayerProtocol transportLayerProtocol;
    protected final List<ChannelHandlerContext> channelHandlerContextList;
    protected final Map<String, TunnelContext> tunnelRegisterMap = new ConcurrentHashMap<>();

    public ProxyContext(Container parent, String proxyId, TransportLayerProtocol protocol, List<ChannelHandlerContext> handlerContexts, ControlContext controlContext) {
        super(parent);
        this.proxyId = proxyId;
        this.transportLayerProtocol = protocol;
        this.channelHandlerContextList = handlerContexts;

        // 自注入：使隧道等后代容器可经 get(ProxyContext.class) 沿父链取回代理上下文
        put(ProxyContext.class, this);

        // 生命周期面：控制连接死亡 → 本代理级联销毁（隧道随本容器销毁级联回收）
        dependOn(controlContext, DependencyPolicy.CASCADE_DISPOSE);

        // Effect：关闭代理持有的所有 Netty Channel（本容器持有的数据面资源）
        effect(c -> channelHandlerContextList.forEach(ctx -> ctx.close()));
    }

    /** 跨层读取：控制连接上下文经依赖边精确取得（数据面的唯一上行通道） */
    protected ControlContext controlContext() {
        for (Container dependency : dependencies()) {
            if (dependency instanceof ControlContext controlContext) {
                return controlContext;
            }
        }
        return null;
    }

    public TunnelContext newTunnelContext() {
        return newTunnelContext(generateTunnelId());
    }

    /**
     * 创建隧道子容器并启动其生命周期；隧道注销（销毁时索引清理）由 createChild 注册的 Effect 完成。
     * 父容器处于非激活状态时带超时等待，避免事件循环线程被无限期阻塞。
     */
    public TunnelContext newTunnelContext(String tunnelId) {
        try {
            TunnelContext tunnelContext = (TunnelContext) child(10, TimeUnit.SECONDS, new Object[]{tunnelId}).get();
            tunnelContext.load();
            return tunnelContext;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Container createChild(Container parent, Object... args) {
        String tunnelId = (String) args[0];
        TunnelContext tunnelContext = createNewTunnelContext(parent, tunnelId);
        tunnelRegisterMap.put(tunnelContext.getTunnelId(), tunnelContext);
        // Effect：隧道销毁时注销其路由项；服务端子类经覆写 unregisterTunnel 追加等待表/地址表清理
        tunnelContext.effect(c -> unregisterTunnel(tunnelId));
        return tunnelContext;
    }

    /** 受控方法：隧道注销，清理本层路由索引；子类覆写追加各自索引清理 */
    public void unregisterTunnel(String tunnelId) {
        tunnelRegisterMap.remove(tunnelId);
    }

    /** 待实现：创建具体协议的隧道子容器，parent 为本容器 */
    public abstract TunnelContext createNewTunnelContext(Container parent, String tunnelId);

    public TunnelContext getTunnelContext(String tunnelId) {
        return tunnelRegisterMap.get(tunnelId);
    }

    public static String generateProxyId() {
        return "PXY-" + UUID.randomUUID().toString().replace("-", "");
    }

    protected static String generateTunnelId() {
        return "TUN-" + UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * 优雅关闭：通知对端 PROXY_CLOSE 后销毁本容器，销毁级联全部隧道子容器；幂等，重复调用返回同一销毁 Future。
     */
    public java.util.concurrent.Future<?> close() {
        if (state() == State.ACTIVE || state() == State.SUSPENDED) {
            // Effect：通知对端代理关闭（经控制通道发送）
            effect(c -> closeRemoteProxy());
        }
        return disposal();
    }

    public void addHandlerContext(ChannelHandlerContext ctx) {
        channelHandlerContextList.add(ctx);
    }

    /** 跨层写入：控制事件经控制连接上行发送 */
    public <T> void controlClient(ControlEvent<T> controlEvent) {
        controlContext().writeAndFlush(controlEvent);
    }

    public void closeRemoteTunnel(String tunnelId) {
        ControlEvent<CommonInfo> controlEvent = new ControlEvent<>(ProxyControlEventEnum.TUNNEL_CLOSE.getType(), new CommonInfo(tunnelId, proxyId));
        controlClient(controlEvent);
    }

    public void closeRemoteProxy() {
        ControlEvent<CommonInfo> controlEvent = new ControlEvent<>(ProxyControlEventEnum.PROXY_CLOSE.getType(), new CommonInfo(proxyId));
        controlClient(controlEvent);
    }
}
