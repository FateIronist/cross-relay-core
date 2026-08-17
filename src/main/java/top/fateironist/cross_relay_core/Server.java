package top.fateironist.cross_relay_core;

import top.fateironist.cross_relay_core.model.options.Options;

import java.util.concurrent.Future;

public interface Server {

    Future<Void> start(Options options);

    Future<?> shutdown();

    void shutdownNow();
}
