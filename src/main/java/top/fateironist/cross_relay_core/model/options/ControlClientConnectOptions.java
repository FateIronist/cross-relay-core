package top.fateironist.cross_relay_core.model.options;

import lombok.Builder;

import java.net.SocketAddress;

@Builder
public class ControlClientConnectOptions {
    public SocketAddress address;
    public long pingInterval; // ms
    public long pingTimeout; // ms
}
