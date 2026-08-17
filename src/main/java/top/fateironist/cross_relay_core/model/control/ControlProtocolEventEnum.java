package top.fateironist.cross_relay_core.model.control;

public enum ControlProtocolEventEnum implements ControlEventEnum{
    SERVER_INFO,
    CLIENT_INFO,
    PING,
    PONG,
    ACK,
    CONNECTION_PERMIT,
    SESSION_PUBLIC_KEY,
    SESSION_SECRET_KEY,   // S→C：被RSA加密后的AES会话密钥
    SESSION_SECRET_ACK,
    ERROR;

    @Override
    public String getType() {
        return this.toString();
    }
}
