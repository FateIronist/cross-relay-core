package top.fateironist.cross_relay_core.model.proxy.tunnel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.socket.DatagramPacket;
import lombok.Getter;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;

import java.net.InetSocketAddress;
import java.util.concurrent.Future;

/**
 * 服务端 UDP 隧道：requester 与 clientProxy 各由「地址 + channel」两要素描述，四者齐备即 OPEN。
 * 与 TCP 的关键差异在于寻址：服务端本就有两条 channel（都是多连接复用的），发包时必须显式带上目标地址，
 * 因此转发以 new DatagramPacket(msg.retain(), 目标地址) 写回对应 channel；且 UDP 无断连事件，回收依赖 30s 无活动判定，由 ServerUdpProxyContext 的 10s 周期扫描驱动。
 */
public class ServerUdpTunnelContext extends TunnelContext {
    // 外部 requester 所在的 channel，即本 proxy 专属的 bind(0) requester channel；多个 requester 复用同一条，故必须配合地址才发得出去
    @Getter
    private Channel requesterChannel;
    // 客户端 duplex channel 在服务端 client-proxy 端口上的对端 channel；全局共享，多客户端复用
    @Getter
    private Channel clientProxyChannel;
    // 发起本次隧道的外部 requester 地址：既是下行发包的目标地址，也是 ServerUdpProxyContext 地址路由表的键
    @Getter
    private InetSocketAddress requesterAddress;
    // 客户端来源地址：既是上行发包的目标地址，也是 ServerUdpProxyContext 地址路由表的键
    @Getter
    private InetSocketAddress clientProxyAddress;

    // 最后一次转发活动的时间戳，每次 writeToXxxAndFlush 都会刷新；UDP 无断连事件，只能以此判活
    private long lastActiveTime = System.currentTimeMillis();

    // 30s 无活动即视为隧道失活，由 checkTimeout() 触发 closeGracefully()；固定值，当前不可配置
    private final long timeOut = 30000;

    public ServerUdpTunnelContext(String tunnelId) {
        super(tunnelId, TransportLayerProtocol.UDP);
    }

    /** 客户端上行数据 -> 外部 requester：刷新活动时间，且仅在 OPEN 时以 DatagramPacket 发往 requesterAddress */
    public void writeToRequesterAndFlush(ByteBuf msg) {
        lastActiveTime = System.currentTimeMillis();
        if (status == TunnelStatus.OPEN) requesterChannel.writeAndFlush(new DatagramPacket(msg.retain(), requesterAddress));
    }

    /** 外部 requester 下行数据 -> 客户端：刷新活动时间，且仅在 OPEN 时发往 clientProxyAddress（写回的是共享的 client-proxy channel，靠地址区分客户端） */
    public void writeToClientProxyAndFlush(ByteBuf msg) {
        lastActiveTime = System.currentTimeMillis();
        if (status == TunnelStatus.OPEN) clientProxyChannel.writeAndFlush(new DatagramPacket(msg.retain(), clientProxyAddress));
    }

    /**
     * 由注册包路径调用（ServerUdpProxyContext.registerClientProxy）：补齐 clientProxy 的地址与 channel。
     * 两个字段各自只在为空时赋值，已有值时不覆盖，保证隧道端点一次成型；赋值后 tryOpen()，若 requester 侧已就绪则隧道即刻 OPEN。
     * 判空必须判字段（this.xxx）而非方法参数，否则条件恒不成立，四元组永远齐备不了、隧道永不 OPEN。
     */
    public void setClientProxy(InetSocketAddress clientProxyAddress, Channel clientProxyChannel) {
        if (this.clientProxyAddress == null) this.clientProxyAddress = clientProxyAddress;
        if (this.clientProxyChannel == null) this.clientProxyChannel = clientProxyChannel;
        tryOpen();
    }

    /** 由 requester 首包路径调用：补齐 requester 的地址与 channel，同样是仅在为空时赋值，赋值后 tryOpen() */
    public void setRequester(InetSocketAddress requesterAddress, Channel requesterChannel) {
        if (this.requesterAddress == null) this.requesterAddress = requesterAddress;
        if (this.requesterChannel == null) this.requesterChannel = requesterChannel;
        tryOpen();
    }

    /** 齐备条件：requester 与 clientProxy 的 channel、address 四者全非空；比 TCP 多两个地址条件，因为 UDP 发包必须显式指定目标地址 */
    @Override
    protected boolean tryOpen() {
        if (requesterChannel != null && clientProxyChannel != null && requesterAddress != null && clientProxyAddress != null) {
            status = TunnelStatus.OPEN;
            return true;
        }

        return false;
    }

    /** 由 ServerUdpProxyContext 的 10s 周期定时器遍历调用：已 OPEN 且 30s 无活动则 closeGracefully()，借"出表 + 通知对端"完成资源回收 */
    public Future<?> checkTimeout() {
        if (isTimeout()) {
            return closeGracefully();
        }

        return DefaultEventLoopGroup.emptyFuture();
    }

    /** 超时判定以 OPEN 为前提：INIT 阶段的隧道尚未开始转发，不应被误判失活而回收 */
    public boolean isTimeout() {
        return status == TunnelStatus.OPEN && System.currentTimeMillis() - lastActiveTime > timeOut;
    }

}
