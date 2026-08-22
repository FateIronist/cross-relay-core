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

@Slf4j
public class ProxyClient implements Client {
    private final ClientServiceInfo clientServiceInfo;
    private final ProxyServerInfo proxyServerInfo;
    private final EventLoopGroup workerGroup;

    private final ProxyClientListener listener;

    private ProxyTcpClient proxyTcpClient;
    private ProxyUdpClient proxyUdpClient;

    public ProxyClient(EventLoopGroup workerGroup, ClientServiceInfo clientServiceInfo, ProxyServerInfo proxyServerInfo, ProxyClientListener proxyClientListener) {
        this.workerGroup = workerGroup;
        this.clientServiceInfo = clientServiceInfo;
        this.proxyServerInfo = proxyServerInfo;
        this.listener = proxyClientListener;
    }

    @Override
    public Future<Void> connect(AbstractArgs arg) {
        ProxyClientConnectArgs args = (ProxyClientConnectArgs) arg;
        var opts = args.getOptions();
        Promise<Void> promise = workerGroup.next().newPromise();

        if (args.getProtocol() == TransportLayerProtocol.TCP) {
            proxyTcpClient = new ProxyTcpClient(clientServiceInfo, proxyServerInfo, workerGroup, listener);
            return proxyTcpClient.connect(arg);
        } else {
            proxyUdpClient = new ProxyUdpClient(clientServiceInfo, proxyServerInfo, workerGroup, listener);
            return proxyUdpClient.connect(arg);
        }
    }

    @Override
    public Future<?> close() {
        EventLoopGroup shutdownEventLoopGroup = new NioEventLoopGroup(1);
        EventLoop eventLoop = shutdownEventLoopGroup.next();
        Promise<?> promise = eventLoop.newPromise();
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

    @Override
    public void closeNow() {
        if (proxyTcpClient != null) proxyTcpClient.closeNow();
        if (proxyUdpClient != null) proxyUdpClient.closeNow();
    }

}
