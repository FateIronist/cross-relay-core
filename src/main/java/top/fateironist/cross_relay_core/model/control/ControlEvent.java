package top.fateironist.cross_relay_core.model.control;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

@Data
public class ControlEvent<T> {
    private Long id;
    private Long ack;
    private ControlEventEnum type;
    private T body;

    public ControlEvent() {
    }

    public ControlEvent(Long ack) {
        this.ack = ack;
    }

    public ControlEvent(ControlEventEnum eventType, T body) {
        this.type = eventType;
        this.body = body;
    }
}
