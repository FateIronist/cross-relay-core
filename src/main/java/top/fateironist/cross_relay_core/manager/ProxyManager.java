package top.fateironist.cross_relay_core.manager;

import java.util.concurrent.Future;

public interface ProxyManager {
    Future<Boolean> closeTunnel(String tunnelId);
}
