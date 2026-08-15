package top.fateironist.cross_relay_core.control.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import top.fateironist.cross_relay_core.util.JsonUtil;

public class JsonEncoder extends ChannelDuplexHandler {

    /**
     * 出站：ControlEvent → ByteBuf（JSON 序列化）
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            byte[] bytes = JsonUtil.OBJECT_MAPPER.writeValueAsBytes(msg);
            ctx.write(Unpooled.wrappedBuffer(bytes), promise);
    }
}
