package top.fateironist.cross_relay_core.model.options.control;

import lombok.Builder;
import top.fateironist.cross_relay_core.model.options.Options;

@Builder
public class ControlServerStartOptions implements Options {
    public int port;
    public int maxConnections;

    public long pingTimeout; // ms
}
