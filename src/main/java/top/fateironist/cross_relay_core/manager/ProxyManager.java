package top.fateironist.cross_relay_core.manager;

import java.util.concurrent.Future;

public abstract class ProxyManager {
    public abstract Future<Boolean> closeTunnel(String tunnelId);
}
