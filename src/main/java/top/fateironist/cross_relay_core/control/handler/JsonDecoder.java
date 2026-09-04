package top.fateironist.cross_relay_core.control.handler;

import com.fasterxml.jackson.core.type.TypeReference;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import top.fateironist.cross_relay_core.util.JsonUtil;

public class JsonDecoder<T> extends ChannelDuplexHandler {
    private TypeReference<T> clazz;

    public JsonDecoder(TypeReference<T> typeReference) {
        this.clazz = typeReference;
    }

    /**
     * 入站：ByteBuf → ControlEvent（JSON 反序列化）
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf byteBuf) {
            byte[] bytes = new byte[byteBuf.readableBytes()];
            byteBuf.readBytes(bytes);
            T event = JsonUtil.OBJECT_MAPPER.readValue(bytes, clazz);
            ctx.fireChannelRead(event);
        } else {
            ctx.fireChannelRead(msg);
        }
    }
}
