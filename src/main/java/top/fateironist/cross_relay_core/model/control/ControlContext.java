package top.fateironist.cross_relay_core.model.control;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;
import lombok.Data;
import lombok.Setter;
import top.fateironist.cross_relay_core.model.control.event.ControlEvent;
import top.fateironist.cross_relay_core.model.info.ProxyClientInfo;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@Data
public class ControlContext {
    public static final AttributeKey<ControlContext> KEY = AttributeKey.valueOf("controlContext");

    private String controlId;

    private ProxyClientInfo proxyClientInfo;
    private ProxyServerInfo proxyServerInfo;
    private Channel channel;
    private ScheduledFuture<?> pingScheduler;

    private AtomicLong msgId = new AtomicLong(0);

    private boolean permit = false;

    private boolean encrypted = false;

    private final Integer maxTolerableAbnormalEventCount = 5;
    private final AtomicInteger tolerableAbnormalEventCount = new AtomicInteger(0);

    @Setter
    protected Consumer<ControlContext> closeHook;

    public ControlContext (String controlId, ProxyServerInfo proxyServerInfo, Channel channel) {
        this.controlId = controlId;
        this.proxyServerInfo = proxyServerInfo;
        this.channel = channel;
    }

    public ControlContext (ProxyClientInfo proxyClientInfo, Channel channel) {
        this.controlId = generateControlId();
        this.proxyClientInfo = proxyClientInfo;
        this.channel = channel;
    }

    public ControlContext (ProxyServerInfo proxyServerInfo, Channel channel) {
        this.controlId = generateControlId();
        this.proxyServerInfo = proxyServerInfo;
        this.channel = channel;
    }

    public String generateControlId() {
        return "CTL-" + UUID.randomUUID().toString().replace("-","");
    }

    public void closeRemote() {
        writeAndFlush(new ControlEvent(ControlProtocolEventEnum.CLOSE.getType(), null));
    }

    public Future<?> close() {
        closeRemote();
        if (pingScheduler != null) pingScheduler.cancel(true);
        if (closeHook != null) closeHook.accept(this);
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
