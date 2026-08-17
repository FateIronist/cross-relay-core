package top.fateironist.cross_relay_core.model.control;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.ScheduledFuture;
import lombok.Data;
import top.fateironist.cross_relay_core.model.info.ProxyClientInfo;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
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

    private final Integer maxTolerableAbnormalEventCount = 5;
    private final AtomicInteger tolerableAbnormalEventCount = new AtomicInteger(0);


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

    public Future<?> close() {
        if (pingScheduler != null) pingScheduler.cancel(true);
        return channel.close();
    }

    public void writeAndFlush(ControlEvent event) {
        event.setId(msgId.getAndIncrement());
        channel.writeAndFlush(event);
    }

    public void writeAndFlush(String str) {
        channel.writeAndFlush(str);
    }


    public void handleAbnormalEvent(ControlEvent<Map<String, Object>> event) {
        if (tolerableAbnormalEventCount.getAndIncrement() > maxTolerableAbnormalEventCount) {
            close();
        }
    }
}
