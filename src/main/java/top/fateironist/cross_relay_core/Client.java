package top.fateironist.cross_relay_core;

import top.fateironist.cross_relay_core.model.options.ControlClientConnectOptions;

import java.util.concurrent.Future;

public interface Client {

    Future<Void> connect(ControlClientConnectOptions options);

    Future<?> close();

    void closeNow();
}
