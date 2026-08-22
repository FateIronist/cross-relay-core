package top.fateironist.cross_relay_core.proxy;

import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import lombok.extern.slf4j.Slf4j;
import top.fateironist.cross_relay_core.Server;
import top.fateironist.cross_relay_core.model.args.AbstractArgs;
import top.fateironist.cross_relay_core.model.args.proxy_server.ClientProxyServerStartArgs;
import top.fateironist.cross_relay_core.proxy.listener.ProxyServerListener;


@Slf4j
public class ProxyServer implements Server {
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ProxyServerListener listener;

    private ProxyTcpServer proxyTcpServer;
    private ProxyUdpServer proxyUdpServer;

    public ProxyServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup, ProxyServerListener listener) {
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.listener = listener;
    }

    @Override
    public Future<Void> start(AbstractArgs arg) {
        ClientProxyServerStartArgs args = (ClientProxyServerStartArgs) arg;
        var opts = args.getOptions();

        ProxyTcpServer proxyTcpServer;
        ProxyUdpServer proxyUdpServer;

        if (opts.isEnableProxyTcp() && opts.isEnableProxyUdp()) {
            EventLoop eventLoop = workerGroup.next();
            Promise<Void> promise = eventLoop.newPromise();

            proxyTcpServer = new ProxyTcpServer(bossGroup, workerGroup, listener);
            proxyUdpServer = new ProxyUdpServer(workerGroup, listener);

            Thread.ofVirtual().start(() -> {
                Future<Void> tcpFuture = proxyTcpServer.start(arg);
                Future<Void> udpFuture = proxyUdpServer.start(arg);
                try {
                    tcpFuture.sync();
                    udpFuture.sync();
                } catch (Exception e) {
                    promise.setFailure(e);
                    return;
                }

                promise.setSuccess(null);
            });
            return promise;
        } else if (opts.isEnableProxyTcp()) {
            proxyTcpServer = new ProxyTcpServer(bossGroup, workerGroup, listener);
            return proxyTcpServer.start(arg);
        } else if (opts.isEnableProxyUdp()) {
            proxyUdpServer = new ProxyUdpServer(workerGroup, listener);
            return proxyUdpServer.start(arg);
        }else {
            return null;
        }
    }



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

    @Override
    public void shutdownNow() {
        if (proxyTcpServer != null) proxyTcpServer.shutdownNow();
        if (proxyUdpServer != null) proxyUdpServer.shutdownNow();
    }
}
