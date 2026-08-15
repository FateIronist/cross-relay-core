package top.fateironist.cross_relay_core.control.listener;

import io.netty.channel.Channel;
import top.fateironist.cross_relay_core.Listener;
import top.fateironist.cross_relay_core.model.control.ControlContext;
import top.fateironist.cross_relay_core.model.control.ControlEvent;

public class ControlServerListener implements Listener {

    /**
     * 在channel被accept后，进行一些操作，返回值为是否接收，可以用于ip黑名单等业务
     * @param channel
     * @return
     */
    public boolean beforeConnect(Channel channel) {
        return true;
    }

    /**
     * 在channel被accept后，进行一些操作
     * @param context
     */
    public void afterAccept(ControlContext context) {
    }

    /**
     * 在业务上接收该连接前，进行身份等的验证，返回值为是否接收，此处不应当对channel进行close，ControlServer内部已经对false的情况进行close处理了，
     * @param context
     * @param event
     * @return
     */
    public boolean beforePermit(ControlContext context, ControlEvent event) {
        return true;
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
