package top.fateironist.cross_relay_core.control.listener;

import io.netty.channel.Channel;
import top.fateironist.cross_relay_core.Listener;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.control.event.ControlEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * 控制通道服务端侧的钩子集合与业务事件注册表，默认放行或空实现，由上层业务按需覆写
 * 只承担"生命周期通知"、"准入裁决"与"业务事件分发"三种角色，握手、密钥协商等核心编排内聚在 ControlServer/ControlContext 中，不在此实现
 */
public class ControlServerListener implements Listener {
    // 业务事件注册表，onEvent 按事件 type 分发；未注册的类型静默忽略
    private final Map<String, BiConsumer<ControlContext, ControlEvent<Map<String, Object>>>> eventHandlers = new HashMap<>();

    /**
     * TCP 层准入过滤，返回值为是否接收，可做 IP 黑名单等业务；返回 false 时 Connection 由 ControlServer 直接关闭
     * 此时子 channel 尚未创建会话上下文，是业务层最早的拒绝时机，此处也不应自己关闭 channel
     * @param channel 刚 accept 到的子 channel
     */
    public boolean beforeAccept(Channel channel) {
        return true;
    }

    /**
     * 服务端会话上下文创建完成、controlId 已分配后回调，此时连接尚未加密与准入
     * 默认不做任何事；注意 controlId 此刻并不下发，要等 permit 时才随 CONNECTION_PERMIT 告知客户端
     */
    public void afterAccept(ControlContext context) {
    }

    /**
     * 在业务上接收该连接前，进行身份等的验证，返回值为是否接收，此处不应当对channel进行close，ControlServer内部已经对false的情况进行close处理了
     * 触发时机为服务端收到 SESSION_SECRET_ACK、客户端身份已合并进 context 之后；返回 false 时 ControlServer 会回 ERROR 并关闭连接
     * @param event 携带客户端身份的 SESSION_SECRET_ACK 事件
     */
    public boolean beforePermit(ControlContext context, ControlEvent<Map<String, Object>> event) {
        return true;
    }

    /**
     * permit 之后收到业务事件时回调，按事件 type 分发给已注册的 handler
     * 协议类事件（PING 等）已被 ControlServer 内部消费，不会走到这里；final 以保证分发语义不被覆写
     */
    public final void onEvent(ControlContext context,ControlEvent<Map<String, Object>> event) {
        eventHandlers.getOrDefault(event.getType(), (c, e) -> {}).accept(context, event);
    }

    /**
     * 通道抛出异常时回调，是否关闭连接由实现方自行决定，默认不做任何事
     */
    public void caughtException(ControlContext context, Throwable throwable) {

    }

    /**
     * 读空闲超时（pingTimeout 内未收到任何数据）时回调
     * 默认直接关闭连接：control 通道的保活依赖客户端 PING 到达，长时间静默即视为链路已失效
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
    public ControlServerListener addEventHandler(String event, BiConsumer<ControlContext, ControlEvent<Map<String, Object>>> handler) {
        eventHandlers.put(event, handler);
        return this;
    }
}
