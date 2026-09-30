package top.fateironist.cross_relay_core.control.handler;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import top.fateironist.cross_relay_core.model.control.ControlContext;

import java.util.function.Consumer;

/**
 * 读空闲事件处理器：把上游 IdleStateHandler 产生的 IdleStateEvent 翻译成对 ControlContext 的回调
 * 自身不决定超时策略（默认由 listener.onTimeOut 关闭连接），因此必须与 IdleStateHandler 成对装配
 */
public class IdleEventHandler extends ChannelDuplexHandler {
    // 读空闲回调，装配 pipeline 时注入两端 listener 的 onTimeOut
    private final Consumer<ControlContext> READER_IDLE_CALLBACK;

    public IdleEventHandler(Consumer<ControlContext> readIdleCallback) {
        super();
        READER_IDLE_CALLBACK = readIdleCallback;
    }

    /**
     * 收到空闲事件时按 channel attr 反查 ControlContext 并回调，是否关闭由回调方决定
     * 目前只在 READER_IDLE（读空闲，即 pingTimeout 内未收到任何数据）时回调，其余空闲类型忽略
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;

            ControlContext controlContext = ctx.channel().attr(ControlContext.KEY).get();
            if (event.state() == IdleState.READER_IDLE) {
                // Handle writer idle event
                READER_IDLE_CALLBACK.accept(controlContext);
            }
        }
        super.userEventTriggered(ctx, evt);
    }
}
