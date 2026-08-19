package top.fateironist.cross_relay_core.model.proxy.tunnel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import top.fateironist.cross_relay_core.model.TransportLayerProtocol;
import top.fateironist.cross_relay_core.model.info.ClientServiceInfo;
import top.fateironist.cross_relay_core.model.info.OriginalRequesterInfo;
import top.fateironist.cross_relay_core.model.info.ProxyClientInfo;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;

import java.util.function.Function;

public class ClientTunnelContext extends TunnelContext {
    private Channel clientToServiceChannel;
    private Channel clientToServerChannel;

    public ClientTunnelContext(String tunnelId,
                               TransportLayerProtocol transportLayerProtocol,
                               ClientServiceInfo clientServiceInfo,
                               ProxyClientInfo proxyClientInfo,
                               ProxyServerInfo proxyServerInfo,
                               OriginalRequesterInfo originalRequesterInfo) {
        super(tunnelId, transportLayerProtocol, clientServiceInfo, proxyClientInfo, proxyServerInfo, originalRequesterInfo);
    }

    public Channel getClientToServiceChannel() {
        return clientToServiceChannel;
    }

    public void setClientToServiceChannel(Channel clientToServiceChannel) {
        if (getClientToServiceChannel() == null) {
            this.clientToServiceChannel = clientToServiceChannel;
        }
    }

    public Channel getClientToServerChannel() {
        return clientToServerChannel;
    }

    public void setClientToServerChannel(Channel clientToServerChannel) {
        if (getClientToServerChannel() == null) {
            this.clientToServerChannel = clientToServerChannel;
        }
    }

    public void writeToServiceAndFlush(ByteBuf byteBuf) {
        byteBuf.retain();
        clientToServiceChannel.writeAndFlush(byteBuf);
    }

    public void writeToServerAndFlush(ByteBuf byteBuf) {
        byteBuf.retain();
        clientToServerChannel.writeAndFlush(byteBuf);
    }

    @Override
    public Future<?> closeGracefully(Function<TunnelContext, Future<?>> closeRemote, EventLoop eventLoop) {
        Promise<?> promise = eventLoop.newPromise();
        Thread.ofVirtual().start(() -> {
            try {
                closeRemote.apply(this).sync();
                if (clientToServiceChannel != null && clientToServiceChannel.isOpen()) {
                    clientToServiceChannel.close().sync();
                }
                if (clientToServerChannel != null && clientToServerChannel.isOpen()) {
                    clientToServerChannel.close().sync();
                }
            } catch (InterruptedException e) {
                promise.setFailure(e);
            }
            promise.setSuccess(null);
        });
        return promise;
    }

    @Override
    public Future<?> closeLocal(EventLoop eventLoop) {
        Promise<?> promise = eventLoop.newPromise();
        Thread.ofVirtual().start(() -> {
            try {
                if (clientToServiceChannel != null && clientToServiceChannel.isOpen()) {
                    clientToServiceChannel.close().sync();
                }
                if (clientToServerChannel != null && clientToServerChannel.isOpen()) {
                    clientToServerChannel.close().sync();
                }
            } catch (InterruptedException e) {
                promise.setFailure(e);
            }
            promise.setSuccess(null);
        });
        return promise;
    }
}
