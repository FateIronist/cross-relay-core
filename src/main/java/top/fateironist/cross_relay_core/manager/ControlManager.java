package top.fateironist.cross_relay_core.manager;

import top.fateironist.cross_relay_core.model.control.ControlContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ControlManager {
    private Map<String, ControlContext> controlContextMap;

    public ControlManager() {
        controlContextMap = new ConcurrentHashMap<>();
    }
}
