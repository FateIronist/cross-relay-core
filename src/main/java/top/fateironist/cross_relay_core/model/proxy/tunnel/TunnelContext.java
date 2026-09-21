package top.fateironist.cross_relay_core.model.proxy.tunnel;

import io.netty.util.AttributeKey;
import lombok.Getter;
import top.fateironist.constack.Container;
import top.fateironist.constack.Promise;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.proxy.ProxyContext;

/**
 * 隧道容器：一条代理隧道对应一个 TunnelContext，作为 ProxyContext 的子容器。
 * 数据面：load 参数自上而下传递，运行期读取经 get() 沿父子链向上查找。
 *
 * <p>激活条件：隧道双端（requester↔client-proxy 或服务↔server-proxy）齐备，
 * 子类端点 setter 到达检查点后由 tryOpen() 完成 start Future，容器随之激活。
 */
@Getter
public abstract class TunnelContext extends Container {
    public static final AttributeKey<TunnelContext> KEY = AttributeKey.valueOf("TunnelContext");

    protected final String tunnelId;
    protected final TransportLayerProtocol transportLayerProtocol;

    /** 双端就绪信号：load 的 start Future，两端齐备才完成 */
    private Promise<Object> openPromise;

    public TunnelContext(Container parent, String tunnelId, TransportLayerProtocol transportLayerProtocol) {
        super(parent);
        this.tunnelId = tunnelId;
        this.transportLayerProtocol = transportLayerProtocol;

        // 自注入：使管道处理器可经 get(TunnelContext.class) 沿父链取回隧道上下文
        put(TunnelContext.class, this);
    }

    @Override
    protected java.util.concurrent.Future<Object> start(Object... args) {
        openPromise = new Promise<>();
        return openPromise;
    }

    @Override
    protected Container createChild(Container parent, Object... args) {
        throw new UnsupportedOperationException("TunnelContext does not support child containers");
    }

    /** 端点到达检查点：双端齐备则完成 open Promise，容器转 ACTIVE */
    protected void tryOpen() {
        if (openPromise != null && isBothEndsReady()) {
            openPromise.setSuccess(null);
        }
    }

    /** 子类双端就绪判定 */
    protected abstract boolean isBothEndsReady();

    /**
     * 优雅关闭：通知对端 TUNNEL_CLOSE 后销毁本容器，销毁级联关闭两端资源；幂等，重复调用返回同一销毁 Future。
     */
    public java.util.concurrent.Future<?> closeGracefully() {
        if (state() == State.ACTIVE || state() == State.SUSPENDED) {
            // Effect：通知对端隧道关闭（经父代理容器走控制通道发送）
            effect(c -> {
                Container proxy = parent();
                if (proxy instanceof ProxyContext proxyContext) {
                    proxyContext.closeRemoteTunnel(tunnelId);
                }
            });
        }
        return disposal();
    }

    /**
     * 本地关闭：对端已断开或本端主动回收，不发送通知，直接销毁；幂等。
     */
    public java.util.concurrent.Future<?> closeLocal() {
        return disposal();
    }
}
