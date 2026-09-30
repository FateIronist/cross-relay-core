package top.fateironist.cross_relay_core.control.listener;

import top.fateironist.cross_relay_core.Listener;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.control.event.ControlEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * 控制通道客户端侧的钩子集合与业务事件注册表，默认放行或空实现，由上层业务按需覆写
 * 只承担"生命周期通知"与"业务事件分发"两种角色，握手、通道建立等核心编排内聚在 ControlClient/ControlContext 中，不在此实现
 */
public class ControlClientListener implements Listener {
    // 业务事件注册表，onEvent 按事件 type 分发；未注册的类型静默忽略
    private final Map<String, BiConsumer<ControlContext, ControlEvent<Map<String, Object>>>> eventHandlers = new HashMap<>();

    /**
     * 收到 CONNECTION_PERMIT、即握手完成点后回调，此时 controlId 与服务端信息均已回填到 context
     * 上层通常在此启动后续流程（如向服务端注册代理），默认不做任何事
     */
    public void afterPermit(ControlContext context) {
    }

    /**
     * 加密完成后、permit 之前收到非 CONNECTION_PERMIT 的事件时回调，意味本次准入被服务端拒绝，随后连接会被关闭
     * 注意握手前（尚未加密）的异常事件不会走到这里，那类事件由 ControlContext 计入异常容忍计数
     */
    public void onDeny(ControlContext context) {
    }


    /**
     * permit 之后收到业务事件时回调，按事件 type 分发给已注册的 handler
     * 协议类事件（PONG 等）已被 ControlClient 内部消费，不会走到这里；final 以保证分发语义不被覆写
     */
    public final void onEvent(ControlContext context, ControlEvent<Map<String, Object>> event) {
        eventHandlers.getOrDefault(event.getType(), (c, e) -> {}).accept(context, event);
    }

    /**
     * 通道抛出异常时回调，是否关闭连接由实现方自行决定，默认不做任何事
     */
    public void caughtException(ControlContext context, Throwable throwable) {

    }

    /**
     * 读空闲超时（pingTimeout 内未收到任何数据）时回调
     * 默认直接关闭连接：control 通道的保活依赖对端 PING 到达，长时间静默即视为链路已失效
     */
    public void onTimeOut(ControlContext context) {
        context.close();
    }

    /**
     * 通道关闭时回调（无论本端主动关闭还是对端断开），供上层释放资源，默认不做任何事
     */
    public void onClose(ControlContext context) {

    }

    /**
     * 注册某个事件类型的业务处理器，permit 之后由 onEvent 分发，同类型重复注册会覆盖先前的 handler
     * @param event 事件类型，取自 ControlEventEnum.getType()
     * @param handler 处理逻辑，入参为会话上下文与事件本身
     * @return 自身，便于链式注册
     */
    public ControlClientListener addEventHandler(String event, BiConsumer<ControlContext, ControlEvent<Map<String, Object>>> handler) {
        eventHandlers.put(event, handler);
        return this;
    }
}
