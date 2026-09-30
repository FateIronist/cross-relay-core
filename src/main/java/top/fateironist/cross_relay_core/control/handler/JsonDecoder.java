package top.fateironist.cross_relay_core.control.handler;

import com.fasterxml.jackson.core.type.TypeReference;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import top.fateironist.cross_relay_core.util.JsonUtil;

/**
 * 控制事件的 JSON 反序列化入站 handler：把拆帧后的 ByteBuf 还原为 ControlEvent<T>
 * 目标泛型由构造时传入的 TypeReference 决定，控制通道固定为 ControlEvent&lt;Map&lt;String, Object&gt;&gt;，body 的具体类型由各端按事件 type 自行转换
 */
public class JsonDecoder<T> extends ChannelDuplexHandler {
    private TypeReference<T> clazz;

    public JsonDecoder(TypeReference<T> typeReference) {
        this.clazz = typeReference;
    }

    /**
     * 入站：按 TypeReference 把 ByteBuf 反序列化为 ControlEvent 后向下游传递
     * 非 ByteBuf 的消息原样透传；反序列化异常向上抛出，由业务 handler 的 exceptionCaught 兜底
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
