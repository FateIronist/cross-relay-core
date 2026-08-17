package top.fateironist.cross_relay_core.model.options;

public class ServerInfoServerStartOptions implements Options{
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
