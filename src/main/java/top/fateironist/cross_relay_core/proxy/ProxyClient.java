package top.fateironist.cross_relay_core.proxy;

import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import lombok.extern.slf4j.Slf4j;
import top.fateironist.cross_relay_core.Client;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.proxy_client.ProxyClientConnectArgs;
import top.fateironist.cross_relay_core.model.info.ClientServiceInfo;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;
import top.fateironist.cross_relay_core.proxy.listener.ProxyClientListener;

import java.net.InetSocketAddress;

/**
 * Proxy 层客户端聚合门面：依据 ProxyClientConnectArgs.protocol 在 TCP / UDP 两个实现中二选一，
 * 自身不持有 channel、也不直接触碰 control 通道，连接建立、注册配对与关闭编排全部委托给被选中的实现类。
 */
@Slf4j
public class ProxyClient implements Client {
    // 内网服务描述：内网真实服务的地址等信息，随连接请求下发给子实现
    private final ClientServiceInfo clientServiceInfo;
    // 服务端元数据：control / proxyRequest / infoServer 三个地址，其中 proxyRequest 地址是客户端回连服务端的目标
    private final ProxyServerInfo proxyServerInfo;
    // 全部子实现共用同一个 EventLoopGroup（由上层注入，通常是全局共享组），门面自身不创建线程资源
    private final EventLoopGroup workerGroup;

    // 生命周期通知钩子，仅用于观测，原样透传给子实现
    private final ProxyClientListener listener;

    // TCP / UDP 两个实现二选一：connect 时按协议实例化其中一个，另一个保持 null，关闭时需兼容 null
    private ProxyTcpClient proxyTcpClient;
    private ProxyUdpClient proxyUdpClient;

    /** 由上层在拿到已握手的 control 通道与服务端信息后构建；构造时不建立任何连接，连接动作在 connect() 中按协议下发 */
    public ProxyClient(EventLoopGroup workerGroup, ClientServiceInfo clientServiceInfo, ProxyServerInfo proxyServerInfo, ProxyClientListener proxyClientListener) {
        this.workerGroup = workerGroup;
        this.clientServiceInfo = clientServiceInfo;
        this.proxyServerInfo = proxyServerInfo;
        this.listener = proxyClientListener;
    }

    /**
     * 按协议分派连接请求：TCP 走 ProxyTcpClient（每隧道两条连接），UDP 走 ProxyUdpClient（单条 duplex channel）。
     * 返回的 Future 完成时机由子实现决定——TCP 为两条连接均 sync 成功、UDP 为 bind 成功。
     * 注意：当前两个实现类的构造入参首位均为服务端 requester 地址，与下方调用点尚未对齐，需上层同步。
     */
    @Override
    public Future<Void> connect(AbstractArgs arg) {
        ProxyClientConnectArgs args = (ProxyClientConnectArgs) arg;
        var opts = args.getOptions();
        Promise<Void> promise = workerGroup.next().newPromise();

        // 0. 子实现只需服务端 requester 地址，从门面持有的元数据中取出（单机部署直连；分布式寻址待注册中心接入）
        InetSocketAddress serverProxyRequestAddress = proxyServerInfo.getAddress().getServerProxyRequestAddress();

        // 1. 门面只做协议分派，不做任何连接编排（双连接顺序、注册包、tryOpen 配对均在子实现内完成）
        if (args.getProtocol() == TransportLayerProtocol.TCP) {
            proxyTcpClient = new ProxyTcpClient(serverProxyRequestAddress, workerGroup, listener);
            return proxyTcpClient.connect(arg);
        } else {
            // 2. 非 TCP 一律走 UDP 实现，同样由其自行完成 duplex channel 绑定与首包注册
            proxyUdpClient = new ProxyUdpClient(serverProxyRequestAddress, workerGroup, listener);
            return proxyUdpClient.connect(arg);
        }
    }

    /**
     * 优雅关闭：切到独立 EventLoop 上依次等待 TCP / UDP 实现关闭完成（未创建的一侧跳过），
     * 子实现的 Future 以 get() 串行等待，返回的 promise 由本方法手动完成。
     */
    @Override
    public Future<?> close() {
        EventLoopGroup shutdownEventLoopGroup = new NioEventLoopGroup(1);
        EventLoop eventLoop = shutdownEventLoopGroup.next();
        Promise<?> promise = eventLoop.newPromise();
        // 1. 关闭动作串行执行，避免阻塞调用方线程
        eventLoop.execute(() -> {
            try {
                if (proxyTcpClient != null) proxyTcpClient.close().get();
                if (proxyUdpClient != null) proxyUdpClient.close().get();

            } catch (Exception e) {
                promise.setFailure(e);
            }

            promise.setSuccess(null);
        });

        return promise;
    }

    /** 立即关闭：当前线程直接对两个子实现调用 closeNow()，不做等待与异常处理，用于进程退出等场景 */
    @Override
    public void closeNow() {
        if (proxyTcpClient != null) proxyTcpClient.closeNow();
        if (proxyUdpClient != null) proxyUdpClient.closeNow();
    }

}
