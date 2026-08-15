package top.fateironist.cross_relay_core.model.options;

import lombok.Builder;

@Builder
public class ControlServerStartOptions {
    public int port;
    public int maxConnections;

    public long pingTimeout; // ms

    // 单位 byte/s
    public long singleChannelReadLimit;
    public long singleChannelWriteLimit;
    public long globalChannelReadLimit;
    public long globalChannelWriteLimit;

    public long getReadLimit() {
        return singleChannelReadLimit == 0L ? globalChannelReadLimit : singleChannelReadLimit;
    }

    public long getWriteLimit() {
        return singleChannelWriteLimit == 0L ? globalChannelWriteLimit : singleChannelWriteLimit;
    }
}
