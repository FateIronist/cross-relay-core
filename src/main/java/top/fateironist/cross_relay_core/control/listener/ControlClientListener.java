package top.fateironist.cross_relay_core.control.listener;

import top.fateironist.cross_relay_core.Listener;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.control.ControlEvent;

public class ControlClientListener implements Listener {

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
    public void onMessage(ControlContext context, ControlEvent event) {

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
}
