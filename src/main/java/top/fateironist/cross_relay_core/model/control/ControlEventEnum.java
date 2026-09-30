package top.fateironist.cross_relay_core.model.control;

/**
 * control 事件类型的统一抽象，把事件类型收敛到同一套 getType/equals 约定上。
 * 实现分为两个语义分组：ControlProtocolEventEnum（协议/握手层）与 ProxyControlEventEnum（代理业务层），
 * 事件的 type 字段最终以枚举常量名（toString）的形式在报文中传输。
 */
public interface ControlEventEnum {
    /** 返回事件类型字符串，即枚举常量名，用作 ControlEvent.type 的取值 */
    String getType();
    /** 与报文中的 type 字符串比较；注意这是对 Object.equals 的重载而非覆写，仅接受字符串参数 */
    default boolean equals(String str) {
        return this.getType().equals(str);
    }
}
