package top.fateironist.cross_relay_core.model.control.event;

import lombok.Data;

@Data
public class ControlEvent<T> {
    private Long id;
    private Long ack;
    private String type;
    private T body;

    public ControlEvent() {
    }

    public ControlEvent(Long ack) {
        this.ack = ack;
    }

    public ControlEvent(String eventType, T body) {
        this.type = eventType;
        this.body = body;
    }
}
