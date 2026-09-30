package top.fateironist.cross_relay_core.proxy;

import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import lombok.extern.slf4j.Slf4j;
import top.fateironist.cross_relay_core.DefaultEventLoopGroup;
import top.fateironist.cross_relay_core.Server;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.proxy_server.ClientProxyServerStartArgs;
import top.fateironist.cross_relay_core.proxy.listener.ProxyServerListener;


/**
 * Proxy 数据面服务端的聚合门面：按 Options 的 enableProxyTcp / enableProxyUdp 决定启用哪些子服务器，
 * 自身不参与任何通道建立编排（编排内聚在 ProxyContext / TunnelContext 体系）。两种协议同时启用时在虚拟线程中并行启动，
 * 两者都成功才算整体启动成功；各子服务器均为 INIT→RUNNING→STOPPING→SHUTDOWN 状态机，本门面自身不持有状态。
 */
@Slf4j
public class ProxyServer implements Server {
    // TCP 用两个：boss 负责 accept，worker 负责 IO；UDP 只用 worker
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    // 生命周期钩子
    private final ProxyServerListener listener;

    // 子服务器，仅在 start 时按 Options 创建
    private ProxyTcpServer proxyTcpServer;
    private ProxyUdpServer proxyUdpServer;

    public ProxyServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup, ProxyServerListener listener) {
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.listener = listener;
    }

    /**
     * 按 Options 的开关启动对应子服务器。两种协议都启用时并行启动并等待两者 bind 成功，任一失败则整体失败
     * （TCP 失败时会回收已启动的 UDP 子服务器）；都未启用时返回已失败的 Future。
     * 子服务器实例写入成员字段，保证 shutdown / shutdownNow 可用。
     */
    @Override
    public Future<Void> start(AbstractArgs arg) {
        ClientProxyServerStartArgs args = (ClientProxyServerStartArgs) arg;
        var opts = args.getOptions();

        // 1.TCP 与 UDP 都启用：在虚拟线程中并行启动，两者都 sync 成功才算启动成功
        if (opts.isEnableProxyTcp() && opts.isEnableProxyUdp()) {
            EventLoop eventLoop = workerGroup.next();
            Promise<Void> promise = eventLoop.newPromise();

            this.proxyTcpServer = new ProxyTcpServer(bossGroup, workerGroup, listener);
            this.proxyUdpServer = new ProxyUdpServer(workerGroup, listener);

            Thread.ofVirtual().start(() -> {
                try {
                    Future<Void> tcpFuture = proxyTcpServer.start(arg);
                    Future<Void> udpFuture = proxyUdpServer.start(arg);
                    try {
                        tcpFuture.sync();
                        udpFuture.sync();
                    } catch (Exception e) {
                        // TCP/UDP 任一失败：回收已启动的子服务器，避免资源泄漏
                        try {
                            proxyUdpServer.shutdownNow();
                        } catch (Exception ignored) {
                        }
                        promise.setFailure(e);
                        return;
                    }
                } catch (Exception e) {
                    promise.setFailure(e);
                    return;
                }

                promise.setSuccess(null);
            });
            return promise;
        } else if (opts.isEnableProxyTcp()) {
            // 2.只启用 TCP：直接返回该子服务器的启动结果
            this.proxyTcpServer = new ProxyTcpServer(bossGroup, workerGroup, listener);
            return proxyTcpServer.start(arg);
        } else if (opts.isEnableProxyUdp()) {
            // 3.只启用 UDP
            this.proxyUdpServer = new ProxyUdpServer(workerGroup, listener);
            return proxyUdpServer.start(arg);
        }else {
            // 4.两者都未启用，无可用协议：返回失败 Future 而非 null，避免调用方 NPE
            return DefaultEventLoopGroup.failFuture(new Exception("No proxy protocol enabled"));
        }
    }



    /**
     * 优雅关闭：临时起一个 EventLoop 串行执行 TCP、UDP 子服务器的 shutdown（未创建的一侧跳过）。
     */
    @Override
    public Future<?> shutdown() {
        EventLoopGroup shutdownEventLoopGroup = new NioEventLoopGroup(1);
        EventLoop eventLoop = shutdownEventLoopGroup.next();
        Promise<?> promise = eventLoop.newPromise();
        eventLoop.execute(() -> {
            try {
                if (proxyTcpServer != null) proxyTcpServer.shutdown().get();
                if (proxyUdpServer != null) proxyUdpServer.shutdown().get();

            } catch (Exception e) {
                promise.setFailure(e);
            }

            promise.setSuccess(null);
        });

        return promise;
    }

    /** 立即关闭两个子服务器，不做优雅等待（未创建的一侧跳过） */
    @Override
    public void shutdownNow() {
        if (proxyTcpServer != null) proxyTcpServer.shutdownNow();
        if (proxyUdpServer != null) proxyUdpServer.shutdownNow();
    }
}
