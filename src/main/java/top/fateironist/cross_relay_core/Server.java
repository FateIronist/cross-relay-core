package top.fateironist.cross_relay_core;

import top.fateironist.cross_relay_core.model.options.ControlServerStartOptions;

import java.util.concurrent.Future;

public interface Server {

    Future<Void> start(ControlServerStartOptions options);

    Future<?>  shutdown();

    void shutdownNow();
}
