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
 * 客户端 UDP 隧道：全局只有一条 duplex channel，既连内网服务又连服务端，方向不靠连接区分、而靠每个包的来源地址判别。
 * 正因 UDP 没有断连事件，本类以"最后活动时间 + 30s"自行判定失活（checkTimeout），由客户端侧的驱动逻辑触发关闭。
 */
public class ClientUdpTunnelContext extends TunnelContext {
    // 唯一的 UDP channel：bind 后既作"连服务端"也作"连内网服务"的同一通道，收发共用
    @Getter
    private Channel duplexChannel;
    // 内网服务地址，取自 ProxyClientConnectArgs；是本类判向与发包的目标之一
    @Getter
    private InetSocketAddress serviceAddress;
    // 服务端 requester 端地址，取自 ProxyUdpClient 的构造参数；注册包与上行数据都发往它
    @Getter
    private InetSocketAddress serverProxyAddress;

    // 最后一次转发活动的时间戳，每次 writeToXxxAndFlush 都会刷新；UDP 无断连事件，只能用"有没有流量"来判活
    private long lastActiveTime = System.currentTimeMillis();

    // 30s 无活动即视为隧道失活，由 checkTimeout() 触发 closeGracefully()；固定值，当前不可配置
    private final long timeOut = 30000;

    public ClientUdpTunnelContext(String tunnelId) {
        super(tunnelId, TransportLayerProtocol.UDP);
    }

    /** 服务端下行数据 -> 内网服务：刷新活动时间，且仅在 OPEN 时以 DatagramPacket 发往 serviceAddress */
    public void writeToServiceAndFlush(ByteBuf msg) {
        lastActiveTime = System.currentTimeMillis();
        if (status == TunnelStatus.OPEN) duplexChannel.writeAndFlush(new DatagramPacket(msg.retain(), serviceAddress));
    }

    /** 内网服务上行数据 -> 服务端：刷新活动时间，且仅在 OPEN 时发往 serverProxyAddress；隧道建立时的注册包也经此发出 */
    public void writeToServerProxyAndFlush(ByteBuf msg) {
        lastActiveTime = System.currentTimeMillis();
        if (status == TunnelStatus.OPEN) duplexChannel.writeAndFlush(new DatagramPacket(msg.retain(), serverProxyAddress));
    }

    /**
     * 按包来源判向的双向转发入口：来源等于 serviceAddress 视作内网服务上行，等于 serverProxyAddress 视作服务端下行。
     * 两者都不匹配的陌生地址直接丢弃——UDP 没有连接归属，来源地址就是这里唯一的合法性依据。
     */
    public void writeToOpposite(InetSocketAddress sender, ByteBuf msg) {
        if (sender.equals(serviceAddress)) {
            writeToServerProxyAndFlush(msg);
        } else if (sender.equals(serverProxyAddress)) {
            writeToServiceAndFlush(msg);
        }
    }

    /** 由 duplex channel 的 channelActive 赋值；首次赋值后自动 tryOpen()，重复赋值忽略 */
    public void setDuplexChannel(Channel channel) {
        if (duplexChannel == null) duplexChannel = channel;
        tryOpen();
    }

    /** 取服务端 requester 端地址（ProxyUdpClient 的构造参数）；首次赋值后自动 tryOpen() */
    public void setServerProxyAddress(InetSocketAddress serverProxyAddress) {
        if (this.serverProxyAddress == null) this.serverProxyAddress = serverProxyAddress;
        tryOpen();
    }
    /** 取内网服务地址（ProxyClientConnectArgs）；首次赋值后自动 tryOpen() */
    public void setServiceAddress(InetSocketAddress serviceAddress) {
        if (this.serviceAddress == null) this.serviceAddress = serviceAddress;
        tryOpen();
    }

    /** 齐备条件：duplexChannel + serviceAddress + serverProxyAddress 三者齐备；UDP 无需第二条连接，故用两个地址代替 TCP 的第二条 channel */
    @Override
    protected boolean tryOpen() {
        if (duplexChannel != null && serviceAddress != null && serverProxyAddress != null) {
            status = TunnelStatus.OPEN;
            return true;
        }

        return false;
    }

    /** 由客户端侧的驱动逻辑周期调用：已 OPEN 且超时则 closeGracefully()（本地清理 + 通知对端），否则返回空 Future，调用方可无脑等待 */
    public Future<?> checkTimeout() {
        if (isTimeout()) {
            return closeGracefully();
        }

        return DefaultEventLoopGroup.emptyFuture();
    }

    /** 超时判定以 OPEN 为前提：INIT 阶段的隧道只是建好了资源、还没开始转发，不应被误判失活而回收 */
    public boolean isTimeout() {
        return status == TunnelStatus.OPEN && System.currentTimeMillis() - lastActiveTime > timeOut;
    }
}
