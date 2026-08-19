package top.fateironist.cross_relay_core;

import top.fateironist.cross_relay_core.model.args.AbstractArgs;

import java.util.concurrent.Future;

public interface Client {

    Future<Void> connect(AbstractArgs abstractArgs);

    Future<?> close();

    void closeNow();
}
