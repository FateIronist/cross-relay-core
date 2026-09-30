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
 * 客户端 TCP 隧道：一条隧道对应两条新连接——serviceChannel（连内网真实服务）与 clientProxyChannel（回连服务端 requester 端口），
 * 二者齐备即 OPEN，数据在这两条连接之间搬运。
 * 级联关闭上：service 侧断开（channelInactive）会触发 closeGracefully；
 * 而 serverConnecter（即 clientProxyChannel）断开目前仅记日志、不级联，属于已知覆盖缺口。
 */
public class ClientTcpTunnelContext extends TunnelContext {
    // 连内网服务的连接，ProxyTcpClient 在其 channelActive 时赋值；先建它再建回连连接，保证注册包发出时内网侧已就绪
    @Getter
    private Channel serviceChannel;
    // 回连服务端 requester 端口的中继连接，channelActive 时赋值并随即发出 JSON 注册包完成配对
    @Getter
    private Channel clientProxyChannel;

    public ClientTcpTunnelContext(String tunnelId) {
        super(tunnelId, TransportLayerProtocol.TCP);
    }

    /** 覆写：在基类的本地清理与对端通知之外，合并关闭本端两条连接；三者的 Future 一并等待 */
    @Override
    public Future<?> closeGracefully() {
        return DefaultEventLoopGroup.combine(super.closeGracefully(), serviceChannel.close(), clientProxyChannel.close());
    }

    /** 覆写：同样关掉两条连接，但不通知对端 */
    @Override
    public Future<?> closeLocal() {
        return DefaultEventLoopGroup.combine(super.closeLocal(), serviceChannel.close(), clientProxyChannel.close());
    }

    /** 服务端下行数据 -> 内网服务；仅 OPEN 时 retain 转发，INIT/CLOSING 阶段的数据直接丢弃，避免漏进尚未就绪的隧道 */
    public void writeToServiceAndFlush(ByteBuf msg) {
        if (status == TunnelStatus.OPEN) serviceChannel.writeAndFlush(msg.retain());
    }

    /** 内网服务响应 -> 服务端中继连接；同样仅 OPEN 时 retain 转发 */
    public void writeToClientProxyAndFlush(ByteBuf msg) {
        if (status == TunnelStatus.OPEN) clientProxyChannel.writeAndFlush(msg.retain());
    }

    /** 由 ProxyTcpClient 的 serviceProxyBootstrap 在 channelActive 时调用；首次赋值后自动 tryOpen()，重复赋值被忽略以保证配对一次成型 */
    public void setServiceChannel(Channel serviceChannel) {
        if (this.serviceChannel == null) this.serviceChannel = serviceChannel;
        tryOpen();
    }

    /** 由 ProxyTcpClient 的 serverConnecterBootstrap 在 channelActive 时调用；若此时 serviceChannel 已就绪，隧道会在此刻 OPEN */
    public void setClientProxyChannel(Channel clientProxyChannel) {
        if (this.clientProxyChannel == null) this.clientProxyChannel = clientProxyChannel;
        tryOpen();
    }

    /** 齐备条件：serviceChannel 与 clientProxyChannel 均非空；两端各自独立判定，不依赖对端确认 */
    @Override
    public boolean tryOpen() {
        if (serviceChannel != null && clientProxyChannel != null) {
            status = TunnelStatus.OPEN;
            return true;
        }

        return false;
    }
}
