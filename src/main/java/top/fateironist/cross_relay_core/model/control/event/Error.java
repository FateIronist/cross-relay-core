package top.fateironist.cross_relay_core.model.control.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * control 通道的错误应答体，服务端在拒绝或异常时随 ERROR 事件下发给客户端。
 * 仅承载一段人可读的原因说明（当前为英文文案），用于排查握手被拒的原因。
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Error {
    // 错误描述文本，由发送方给出，供对端日志排查
    private String message;
}
