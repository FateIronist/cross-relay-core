package top.fateironist.cross_relay_core;

import top.fateironist.cross_relay_core.model.options.Options;

import java.util.concurrent.Future;

public interface Client {

    Future<Void> connect(Options options);

    Future<?> close();

    void closeNow();
}
