package top.fateironist.cross_relay_core.model.control;

public enum ProxyControlEventEnum implements ControlEventEnum{
    REGISTER_PROXY,
    REGISTER_PROXY_ACK,
    REQUIRE_CHANNEL,
    TUNNEL_CLOSE,
    PROXY_CLOSE;

    @Override
    public String getType() {
        return this.toString();
    }
}
