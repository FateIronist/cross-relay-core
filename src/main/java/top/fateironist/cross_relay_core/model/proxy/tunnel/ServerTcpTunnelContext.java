package top.fateironist.cross_relay_core.model.proxy.tunnel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import lombok.Getter;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 服务端 TCP 隧道：requesterChannel（外部用户连接）与 clientProxyChannel（客户端回连连接）配对，二者齐备即 OPEN。
 * 级联关闭上：requester 侧断开会触发 closeGracefully 从而通知对端；
 * 而 client-proxy 侧断开目前仅记日志、不级联，属于已知覆盖缺口。
 */
public class ServerTcpTunnelContext extends TunnelContext {
    // 外部 requester 的连接；由 ProxyTcpServer 的 requester 父 handler 在 accept 后赋值，是本端发起 requireChannel 的直接原因
    @Getter
    private Channel requesterChannel;
    // 客户端回连的中继连接；由 registerClientProxy 在注册包配对成功时赋值，赋值即意味着数据面已闭环
    @Getter
    private Channel clientProxyChannel;

    public ServerTcpTunnelContext(String tunnelId) {
        super(tunnelId, TransportLayerProtocol.TCP);
    }

    /** 覆写：在基类的本地清理与对端通知之外，合并关闭本端两条连接 */
    @Override
    public Future<?> closeGracefully() {
        return DefaultEventLoopGroup.combine(super.closeGracefully(), requesterChannel.close(), clientProxyChannel.close());
    }

    /** 覆写：同样关掉两条连接，但不通知对端 */
    @Override
    public Future<?> closeLocal() {
        return DefaultEventLoopGroup.combine(super.closeLocal(), requesterChannel.close(), clientProxyChannel.close());
    }

    /** 客户端上行数据 -> 外部 requester；仅 OPEN 时 retain 转发，半开阶段直接丢弃 */
    public void writeToRequesterAndFlush(ByteBuf msg) {
        if (status == TunnelStatus.OPEN) requesterChannel.writeAndFlush(msg.retain());
    }

    /** 外部 requester 下行数据 -> 客户端中继连接；同样仅 OPEN 时 retain 转发 */
    public void writeToClientProxyAndFlush(ByteBuf msg) {
        if (status == TunnelStatus.OPEN) clientProxyChannel.writeAndFlush(msg.retain());
    }

    /** 由 registerRequester 调用（发生在 newTunnelContext 之后、requireChannel 之前）：此时隧道仍为半就绪，仅记录本端资源 */
    public void setRequesterChannel(Channel requesterChannel) {
        if (this.requesterChannel == null) this.requesterChannel = requesterChannel;
        tryOpen();
    }

    /** 由 registerClientProxy 在注册包配对成功时调用；与已就绪的 requesterChannel 一起把隧道置为 OPEN，数据由此开始流通 */
    public void setClientProxyChannel(Channel clientProxyChannel) {
        if (this.clientProxyChannel == null) this.clientProxyChannel = clientProxyChannel;
        tryOpen();
    }

    /** 齐备条件：requesterChannel 与 clientProxyChannel 均非空；两端各自独立判定，不依赖对端确认 */
    @Override
    protected boolean tryOpen() {
        if (requesterChannel != null && clientProxyChannel != null) {
            status = TunnelStatus.OPEN;
            return true;
        }
        return false;
    }
}
