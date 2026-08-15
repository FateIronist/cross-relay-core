package top.fateironist.cross_relay_core.model.control;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.ScheduledFuture;
import lombok.Data;
import top.fateironist.cross_relay_core.model.info.ProxyClientInfo;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;

import java.util.concurrent.atomic.AtomicLong;

@Data
public class ControlContext {
    public static final AttributeKey<ControlContext> KEY = AttributeKey.valueOf("controlContext");

    private String controlChannelId;

    private ProxyClientInfo proxyClientInfo;
    private ProxyServerInfo proxyServerInfo;
    private Channel channel;
    private ScheduledFuture<?> pingScheduler;

    private AtomicLong msgId = new AtomicLong(0);

    private boolean permit = false;

    private boolean encrypted = false;

    public ControlContext (String id, ProxyClientInfo proxyClientInfo, Channel channel) {
        this.controlChannelId = id;
        this.proxyClientInfo = proxyClientInfo;
        this.channel = channel;
    }

    public ControlContext (String id, ProxyServerInfo proxyServerInfo, Channel channel) {
        this.controlChannelId = id;
        this.proxyServerInfo = proxyServerInfo;
        this.channel = channel;
    }

    public void close() {
        if (pingScheduler != null) pingScheduler.cancel(true);
        channel.close();
    }

    public void writeAndFlush(ControlEvent event) {
        event.setId(msgId.getAndIncrement());
        channel.writeAndFlush(event);
    }

    public void writeAndFlush(String str) {
        channel.writeAndFlush(str);
    }
}
