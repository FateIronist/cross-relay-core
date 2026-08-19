package top.fateironist.cross_relay_core;

import top.fateironist.cross_relay_core.model.args.AbstractArgs;

import java.util.concurrent.Future;

public interface Server {

    Future<Void> start(AbstractArgs abstractArgs);

    Future<?> shutdown();

    void shutdownNow();
}
