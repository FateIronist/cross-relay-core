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

/**
 * control 通道的会话上下文：握手状态、双方身份信息、心跳调度与关闭回调都挂在这一对象上，并透过 channel attr（KEY）与连接绑定，
 * pipeline 中的业务 handler 通过 channel.attr(ControlContext.KEY).get() 反查回本会话。
 * 约束：permit / encrypted 两个状态位必须严格随握手阶段推进置位，它们决定收到的事件是进握手分支还是下发给业务层。
 */
@Data
public class ControlContext {
    // channel attr 的键，把会话对象挂到对应的 control 连接上，pipeline 中任意 handler 都可反查
    public static final AttributeKey<ControlContext> KEY = AttributeKey.valueOf("controlContext");

    // 会话标识，形如 CTL-<uuid>；服务端在 channelActive 时生成但暂不告知客户端，直到 CONNECTION_PERMIT 才下发；客户端侧创建时为 null，收到 permit 后回填
    private String controlId;

    // 客户端身份信息；服务端侧先按远端地址构造占位对象，收到 SESSION_SECRET_ACK 后用 setAdditional 合并出真实凭证
    private ProxyClientInfo proxyClientInfo;
    // 服务端信息与三地址；客户端侧在收到 CONNECTION_PERMIT 后把 body 中的 ProxyServerInfo 合并进来
    private ProxyServerInfo proxyServerInfo;
    // 底层的 control 连接，本会话的所有事件读写都经它
    private Channel channel;
    // 客户端侧握手完成后启动的心跳定时任务，close() 时取消，避免连接关闭后仍继续发包
    private ScheduledFuture<?> pingScheduler;

    // 事件信封 id 的自增序列，由 writeAndFlush 统一分配，保证同一会话内发出的报文 id 单调递增
    private AtomicLong msgId = new AtomicLong(0);

    // 准入状态位：true 表示对端已允许使用本 control 通道（服务端在 beforePermit 通过后置位，客户端在收到 CONNECTION_PERMIT 后置位）；未 permit 时事件不会下发给业务层
    private boolean permit = false;

    // 加密状态位：true 表示会话密钥已协商完成；在此之前 EventEncryptHandler 对报文作明文透传
    private boolean encrypted = false;

    // 握手阶段可容忍的异常事件上限，计数超过该值即关闭连接
    private final Integer maxTolerableAbnormalEventCount = 5;
    // 已收到的异常事件计数，每次 handleAbnormalEvent 递增一次
    private final AtomicInteger tolerableAbnormalEventCount = new AtomicInteger(0);

    // 关闭回调，在 close() 中执行；服务端用它把本会话从 controlContextMap 中移除，客户端不使用
    @Setter
    protected Consumer<ControlContext> closeHook;

    /** 显式指定 controlId 与服务端信息构造会话；客户端侧创建时传入 null，待收到 CONNECTION_PERMIT 后再 setControlId 回填 */
    public ControlContext (String controlId, ProxyServerInfo proxyServerInfo, Channel channel) {
        this.controlId = controlId;
        this.proxyServerInfo = proxyServerInfo;
        this.channel = channel;
    }

    /** 服务端侧使用：会话标识在此即时生成（暂不告知客户端），并带上按远端地址构造的客户端占位身份 */
    public ControlContext (ProxyClientInfo proxyClientInfo, Channel channel) {
        this.controlId = generateControlId();
        this.proxyClientInfo = proxyClientInfo;
        this.channel = channel;
    }

    /** 带服务端信息并自动生成会话标识的构造器；当前源码中无调用点 */
    public ControlContext (ProxyServerInfo proxyServerInfo, Channel channel) {
        this.controlId = generateControlId();
        this.proxyServerInfo = proxyServerInfo;
        this.channel = channel;
    }

    /** 生成会话标识，格式为 CTL-加去掉连字符的 uuid */
    public String generateControlId() {
        return "CTL-" + UUID.randomUUID().toString().replace("-","");
    }

    /** 只向对端发送 CLOSE 事件，不做任何本地资源释放；对端应据此感知本端会话即将结束 */
    public void closeRemote() {
        writeAndFlush(new ControlEvent(ControlProtocolEventEnum.CLOSE.getType(), null));
    }

    /**
     * 本端完整关闭会话，顺序为：先通知对端 CLOSE，再取消心跳定时器，然后执行 closeHook（服务端借此从会话表移除自己），最后关闭 channel。
     * 返回 channel 关闭的 Future，调用方若需等待关闭完成可对其 get()。
     */
    public Future<?> close() {
        closeRemote();
        if (pingScheduler != null) pingScheduler.cancel(true);
        if (closeHook != null) closeHook.accept(this);
        return channel.close();
    }

    /** 统一的事件发送出口：先分配递增的 msgId 再写入 channel；对外发送控制事件都应走这里，以保证 id 单调递增 */
    public void writeAndFlush(ControlEvent event) {
        event.setId(msgId.getAndIncrement());
        channel.writeAndFlush(event);
    }

    /** 直接写入原始字符串，不附加 msgId；供需要发送非 ControlEvent 报文的场景使用 */
    public void writeAndFlush(String str) {
        channel.writeAndFlush(str);
    }


    /**
     * 握手阶段收到不符合预期的事件（未加密就来业务事件、重复握手等）时调用。
     * 计数超过容忍上限即关闭连接，用于防止恶意或协议错乱的客户端反复发送无效事件。
     */
    public void handleAbnormalEvent(ControlEvent<Map<String, Object>> event) {
        if (tolerableAbnormalEventCount.getAndIncrement() > maxTolerableAbnormalEventCount) {
            close();
        }
    }
}
