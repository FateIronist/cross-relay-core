package top.fateironist.cross_relay_core.model.control;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.ScheduledFuture;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import top.fateironist.constack.Container;
import top.fateironist.constack.Promise;
import top.fateironist.cross_relay_core.model.control.event.ControlEvent;
import top.fateironist.cross_relay_core.model.info.ProxyClientInfo;
import top.fateironist.cross_relay_core.model.info.ProxyServerInfo;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 控制连接容器：一条加密控制通道对应一个 ControlContext，作为 ControlServer/ControlClient 的子容器。
 * 数据面：身份信息（ProxyClientInfo/ProxyServerInfo）沿父子链向下传递，
 * 本容器经自注入 {@link #put(Class, Object)} 使任意后代容器可经 get() 沿父链取回本连接上下文。
 *
 * <p>激活条件：握手完成（服务端 permit / 客户端收到 CONNECTION_PERMIT），由 completeHandshake() 触发；
 * 连接通道由本容器创建持有，销毁时统一回收。
 */
@Slf4j
@Getter
public class ControlContext extends Container {
    public static final AttributeKey<ControlContext> KEY = AttributeKey.valueOf("controlContext");

    @Setter
    private String controlId;

    @Setter
    private ProxyClientInfo proxyClientInfo;
    @Setter
    private ProxyServerInfo proxyServerInfo;
    private final Channel channel;

    private ScheduledFuture<?> pingScheduler;

    private final AtomicLong msgId = new AtomicLong(0);

    @Setter
    private boolean permit = false;

    @Setter
    private boolean encrypted = false;

    private final Integer maxTolerableAbnormalEventCount = 5;
    private final AtomicInteger tolerableAbnormalEventCount = new AtomicInteger(0);

    /** 握手完成信号：load 的 start Future，握手成功才完成，容器随之激活 */
    private Promise<Object> handshakePromise;

    public ControlContext(Container parent, String controlId, ProxyClientInfo proxyClientInfo, ProxyServerInfo proxyServerInfo, Channel channel) {
        super(parent);
        this.controlId = controlId != null ? controlId : generateControlId();
        this.proxyClientInfo = proxyClientInfo;
        this.proxyServerInfo = proxyServerInfo;
        this.channel = channel;

        // 自注入：使隧道/代理等后代容器可经 get(ControlContext.class) 沿父链取回本连接
        put(ControlContext.class, this);

        // Effect：关闭控制通道（本容器持有的 Netty Channel）
        effect(c -> channel.close());
        // Effect：取消心跳定时任务（任务由本容器调度持有，可能尚未创建）
        effect(c -> {
            ScheduledFuture<?> scheduler = pingScheduler;
            if (scheduler != null) {
                scheduler.cancel(true);
            }
        });
    }

    public static String generateControlId() {
        return "CTL-" + UUID.randomUUID().toString().replace("-", "");
    }

    @Override
    protected java.util.concurrent.Future<Object> start(Object... args) {
        handshakePromise = new Promise<>();
        return handshakePromise;
    }

    @Override
    protected Container createChild(Container parent, Object... args) {
        throw new UnsupportedOperationException("ControlContext does not support child containers");
    }

    /** 握手完成（permit）：容器转为 ACTIVE 的前置信号 */
    public void completeHandshake() {
        if (handshakePromise != null) {
            handshakePromise.setSuccess(null);
        }
    }

    /** 握手失败（deny / 异常）：load 以失败完成 */
    public void failHandshake(Throwable cause) {
        if (handshakePromise != null) {
            handshakePromise.setFailure(cause);
        }
    }

    /** 定时心跳：加密握手完成后启动，发送 PING；任务句柄归本容器所有，销毁时经 Effect 取消 */
    public void schedulePing(long intervalMillis) {
        pingScheduler = channel.eventLoop().scheduleAtFixedRate(() -> {
            if (permit) {
                log.debug("[ControlContext] [{}] PING-SENT", controlId);
                writeAndFlush(new ControlEvent<>(ControlProtocolEventEnum.PING.getType(), Map.of("pingTime", System.currentTimeMillis())));
            }
        }, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * 优雅关闭：向对端发送 CLOSE 通知后销毁本容器；幂等，重复调用返回同一销毁 Future。
     * 对端已断开（channelInactive）时应直接调用 disposal()，不发送通知。
     */
    public java.util.concurrent.Future<?> close() {
        if (state() == State.ACTIVE || state() == State.SUSPENDED) {
            effect(c -> writeAndFlush(new ControlEvent<>(ControlProtocolEventEnum.CLOSE.getType(), null)));
        }
        return disposal();
    }

    public void writeAndFlush(ControlEvent<?> event) {
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
