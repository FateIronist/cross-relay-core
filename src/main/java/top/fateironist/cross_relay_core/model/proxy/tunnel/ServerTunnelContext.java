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

import java.util.function.Consumer;
import java.util.function.Function;

public class ServerTunnelContext extends TunnelContext {
    private Channel serverToClientChannel;
    private Channel serverToRequesterChannel;

    public ServerTunnelContext(String tunnelId,
                               TransportLayerProtocol transportLayerProtocol,
                               ClientServiceInfo clientServiceInfo,
                               ProxyClientInfo proxyClientInfo,
                               ProxyServerInfo proxyServerInfo,
                               OriginalRequesterInfo originalRequesterInfo) {
        super(tunnelId, transportLayerProtocol, clientServiceInfo, proxyClientInfo, proxyServerInfo, originalRequesterInfo);
    }

    public Channel getServerToClientChannel() {
        return serverToClientChannel;
    }
    public void setServerToClientChannel(Channel serverToClientChannel) {
        if (getServerToClientChannel() == null) {
            this.serverToClientChannel = serverToClientChannel;
        }
    }
    public Channel getServerToRequesterChannel() {
        return serverToRequesterChannel;
    }
    public void setServerToRequesterChannel(Channel serverToRequesterChannel) {
        if (getServerToRequesterChannel() == null) {
            this.serverToRequesterChannel = serverToRequesterChannel;
        }
    }

    public void writeToClientAndFlush(ByteBuf byteBuf) {
        byteBuf.retain();
        serverToClientChannel.writeAndFlush(byteBuf);
    }

    public void writeToRequesterAndFlush(ByteBuf byteBuf) {
        byteBuf.retain();
        serverToRequesterChannel.writeAndFlush(byteBuf);
    }

    @Override
    public Future<?> closeGracefully(Function<TunnelContext, Future<?>> closeRemote, EventLoop eventLoop) {

        Promise<?> promise = eventLoop.newPromise();

        Thread.ofVirtual().start(() -> {
            try {
                closeRemote.apply(this).sync();

                if (serverToClientChannel != null && serverToClientChannel.isOpen()) {
                    serverToClientChannel.close().sync();
                }

                if (serverToRequesterChannel != null && serverToRequesterChannel.isOpen()) {
                    serverToRequesterChannel.close().sync();
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

                if (serverToClientChannel != null && serverToClientChannel.isOpen()) {
                    serverToClientChannel.close().sync();
                }

                if (serverToRequesterChannel != null && serverToRequesterChannel.isOpen()) {
                    serverToRequesterChannel.close().sync();
                }
            } catch (InterruptedException e) {
                promise.setFailure(e);
            }

            promise.setSuccess(null);
        });


        return promise;

    }
}
