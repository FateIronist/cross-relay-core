package top.fateironist.cross_relay_core.model.options.control;

import lombok.Builder;
import top.fateironist.cross_relay_core.model.options.Options;

import java.net.SocketAddress;

@Builder
public class ControlClientConnectOptions implements Options {
    public long pingInterval; // ms
    public long pingTimeout; // ms
}
