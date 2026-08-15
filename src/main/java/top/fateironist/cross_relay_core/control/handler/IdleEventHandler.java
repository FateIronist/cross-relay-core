package top.fateironist.cross_relay_core.control.handler;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import top.fateironist.cross_relay_core.model.control.ControlContext;

import java.util.function.Consumer;

public class IdleEventHandler extends ChannelDuplexHandler {
    private final Consumer<ControlContext> READER_IDLE_CALLBACK;

    public IdleEventHandler(Consumer<ControlContext> readIdleCallback) {
        super();
        READER_IDLE_CALLBACK = readIdleCallback;
    }

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
