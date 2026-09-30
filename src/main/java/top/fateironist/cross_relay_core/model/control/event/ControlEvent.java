package top.fateironist.cross_relay_core.model.control.event;

import lombok.Data;

/**
 * control 通道上的统一事件信封，JSON 序列化后经加密通道传输，结构为 {id, ack, type, body}。
 * type 取自 ControlProtocolEventEnum 或 ProxyControlEventEnum；body 是泛型载体，接收端按 type 反序列化为
 * Map / ProxyClientInfo / ProxyServerInfo / CommonInfo / Error。
 */
@Data
public class ControlEvent<T> {
    // 发送方自增序号，由 ControlContext.writeAndFlush 统一分配，标识本次发送
    private Long id;
    // 被应答事件的 id；目前仅有 ControlEvent(Long ack) 会设置，收发双方尚未实际消费该字段
    private Long ack;
    // 事件类型字符串，与事件枚举的 getType() 对应，接收端据此分发
    private String type;
    // 事件负载，含义随 type 而定，可为 null（如 CLOSE、SERVER_INFO 请求）
    private T body;

    /** 供 JSON 反序列化使用的无参构造器 */
    public ControlEvent() {
    }

    /** 构造应答信封，仅设置被应答的 id */
    public ControlEvent(Long ack) {
        this.ack = ack;
    }

    /** 发送事件时最常用的构造方式：直接给出事件类型与负载 */
    public ControlEvent(String eventType, T body) {
        this.type = eventType;
        this.body = body;
    }
}
