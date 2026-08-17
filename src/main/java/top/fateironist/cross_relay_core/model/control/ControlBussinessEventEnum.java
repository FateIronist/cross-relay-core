package top.fateironist.cross_relay_core.model.control;

public enum ControlBussinessEventEnum implements ControlEventEnum{
    REGISTER_PROXY,
    REGISTER_PROXY_ACK;

    @Override
    public String getType() {
        return this.toString();
    }
}
