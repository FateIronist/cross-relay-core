package top.fateironist.cross_relay_core.control.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import top.fateironist.cross_relay_core.util.JsonUtil;

/**
 * 控制事件的 JSON 序列化出站 handler：把 ControlEvent 转为 ByteBuf 后交给下游加密、加长度前缀
 * 不区分事件类型，统一使用 JsonUtil 的共享 ObjectMapper，故必须与 JsonDecoder 对称装配
 */
public class JsonEncoder extends ChannelDuplexHandler {

    /**
     * 出站：把待发送的 ControlEvent 序列化为 ByteBuf 后继续写出
     * 仅做序列化，不做类型校验——非 ControlEvent 的出站对象也会被按 JSON 序列化
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            byte[] bytes = JsonUtil.OBJECT_MAPPER.writeValueAsBytes(msg);
            ctx.write(Unpooled.wrappedBuffer(bytes), promise);
    }
}
