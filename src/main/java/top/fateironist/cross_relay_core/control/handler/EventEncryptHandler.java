package top.fateironist.cross_relay_core.control.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.AttributeKey;
import top.fateironist.cross_relay_core.util.EncryptUtil;

import javax.crypto.SecretKey;
import java.security.PrivateKey;
import java.security.PublicKey;

public class EventEncryptHandler extends ChannelDuplexHandler {
    public static final AttributeKey<SecretKey> SESSION_SECRET_KEY = AttributeKey.valueOf("sessionSecretKey");
    public static final AttributeKey<PublicKey> SESSION_PUBLIC_KEY = AttributeKey.valueOf("sessionPublicKey");
    public static final AttributeKey<PrivateKey> SESSION_PRIVATE_KEY = AttributeKey.valueOf("sessionPrivateKey");

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        SecretKey secretKey = ctx.channel().attr(SESSION_SECRET_KEY).get();

        if (msg instanceof ByteBuf byteBuf) {
           if (secretKey != null) {
                byte[] bytes = new byte[byteBuf.readableBytes()];
                byteBuf.readBytes(bytes);

                byte[] decrypted;
                try {
                    decrypted = EncryptUtil.aesDecrypt(bytes, secretKey);
                } catch (Exception e) {
                    byteBuf.release();
                    throw e;
                }

                byteBuf.release();
                msg = Unpooled.wrappedBuffer(decrypted);
            }
        }

        super.channelRead(ctx, msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        SecretKey secretKey = ctx.channel().attr(SESSION_SECRET_KEY).get();

        if (msg instanceof ByteBuf byteBuf) {
            if (secretKey != null) {
                byte[] bytes = new byte[byteBuf.readableBytes()];
                byteBuf.readBytes(bytes);

                byte[] encrypted;
                try {
                    encrypted = EncryptUtil.aesEncrypt(bytes, secretKey);
                } catch (Exception e) {
                    byteBuf.release();
                    throw e;
                }

                byteBuf.release();
                msg = Unpooled.wrappedBuffer(encrypted);
            }
        }

        super.write(ctx, msg, promise);
    }
}
