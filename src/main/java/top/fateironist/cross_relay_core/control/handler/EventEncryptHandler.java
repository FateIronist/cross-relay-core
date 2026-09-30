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

/**
 * 控制通道的会话加解密 handler：attr 中无会话密钥时对 ByteBuf 原样透传，出现 SESSION_SECRET_KEY 后自动切换为 AES-256-GCM 加解密
 * 密钥的协商与写入由 ControlClient/ControlServer 负责，本类只做读取，因此同一条 pipeline 无需重建即可完成明密文切换
 */
public class EventEncryptHandler extends ChannelDuplexHandler {
    // 会话对称密钥，握手阶段由两端各自写入，本 handler 据此决定是否加解密
    public static final AttributeKey<SecretKey> SESSION_SECRET_KEY = AttributeKey.valueOf("sessionSecretKey");
    // 客户端 RSA 公钥，仅服务端侧使用，握手结束后清空
    public static final AttributeKey<PublicKey> SESSION_PUBLIC_KEY = AttributeKey.valueOf("sessionPublicKey");
    // 客户端 RSA 私钥，仅客户端侧使用，解出会话密钥后即清空以缩短私钥暴露窗口
    public static final AttributeKey<PrivateKey> SESSION_PRIVATE_KEY = AttributeKey.valueOf("sessionPrivateKey");

    /**
     * 入站解密：attr 中已存在会话密钥时先解密再交给后续 handler 反序列化，否则（握手阶段）原样透传
     * 解密失败会释放原始 ByteBuf 再抛出，交由 exceptionCaught 处理
     */
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

    /**
     * 出站加密：attr 中已存在会话密钥时先加密再写出，否则原样发出
     * 加解密均以整帧 ByteBuf 为单位，故本 handler 必须位于长度域编解码之后
     */
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
