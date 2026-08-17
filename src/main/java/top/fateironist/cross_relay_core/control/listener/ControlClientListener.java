package top.fateironist.cross_relay_core.control.listener;

import top.fateironist.cross_relay_core.Listener;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.control.ControlEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

public class ControlClientListener implements Listener {
    private final Map<String, BiConsumer<ControlContext, ControlEvent<Map<String, Object>>>> eventHandlers = new HashMap<>();

    /**
     * 在channel被permit后，进行一些操作
     * @param context
     */
    public void afterPermit(ControlContext context) {
    }

    /**
     * 在channel被deny后，进行一些操作
     * @param context
     */
    public void onDeny(ControlContext context) {
    }


    /**
     * 当channel有消息时，进行一些操作
     * @param context
     * @param event
     */
    public final void onEvent(ControlContext context, ControlEvent<Map<String, Object>> event) {
        eventHandlers.getOrDefault(event.getType(), (c, e) -> {}).accept(context, event);
    }

    /**
     * 当channel发生异常时，进行一些操作
     * @param context
     * @param throwable
     */
    public void caughtException(ControlContext context, Throwable throwable) {

    }

    /**
     * 当channel超时未读写时，进行一些操作
     * @param context
     */
    public void onTimeOut(ControlContext context) {
        context.close();
    }

    /**
     * 当channel关闭时，进行一些操作
     * @param context
     */
    public void onClose(ControlContext context) {

    }

    public ControlClientListener addEventHandler(String event, BiConsumer<ControlContext, ControlEvent<Map<String, Object>>> handler) {
        eventHandlers.put(event, handler);
        return this;
    }
}
