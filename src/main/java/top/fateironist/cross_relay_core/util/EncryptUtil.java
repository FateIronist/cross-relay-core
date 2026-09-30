package top.fateironist.cross_relay_core.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

/**
 * 加密工具类：提供 RSA-2048/OAEP 的加解密与公钥 Base64 编解码，以及 AES-256-GCM 的会话密钥生成、SecretKey 与字符串互换和加解密
 * 安全边界：本类只服务于 control 控制通道——control 通道用 AES-256-GCM 加密，而所有代理业务数据流（proxy 的 TCP/UDP 通道）为明文，不经过本类
 * 约定：全部方法为静态、类自身无可变实例状态，异常一律包装为 RuntimeException 抛出，调用方无需处理受检异常；
 * 但未提供私有构造，仍可被实例化（无实际意义）
 */
public class EncryptUtil {

    // 仅用于“对象加解密”方法的内部 JSON 序列化，与 JsonUtil 的单例相互独立
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    // ==================== RSA ====================
    // RSA 只用于握手阶段的密钥包裹：客户端生成密钥对并明文上行公钥，服务端用公钥加密 AES 会话密钥回传，业务数据不再走 RSA

    private static final String RSA_ALGORITHM = "RSA";
    // OAEP + SHA-256 填充（非 PKCS#1 v1.5），安全性更高但单块明文长度上限更小
    private static final String RSA_TRANSFORMATION = "RSA/ECB/OAEPWithSHA-256AndMGF1Padding";
    private static final int RSA_KEY_SIZE = 2048;

    /**
     * 生成 RSA-2048 密钥对
     * 调用时机：ControlClient 在 channelActive 时生成一次，私钥暂存 channel attr，客户端握手结束后即清空
     */
    public static KeyPair generateRSAKeyPair() {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance(RSA_ALGORITHM);
            generator.initialize(RSA_KEY_SIZE, new SecureRandom());
            return generator.generateKeyPair();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("RSA key pair generation failed", e);
        }
    }

    /**
     * RSA 公钥加密（用于加密 AES 密钥）
     * 单块加密，不自动分块：明文长度受密钥长度与填充方式限制（2048 位密钥 + OAEP-SHA256 下上限约 190 字节），超长会直接抛异常，
     * 因此只适合包裹短数据，不要用它加密业务报文
     */
    public static byte[] rsaEncrypt(byte[] data, PublicKey publicKey) {
        try {
            Cipher cipher = Cipher.getInstance(RSA_TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, publicKey);
            return cipher.doFinal(data);
        } catch (Exception e) {
            throw new RuntimeException("RSA encryption failed", e);
        }
    }

    /**
     * RSA 公钥加密 String，返回 Base64 编码字符串
     * 服务端握手时用它包裹 aesKeyToString 的结果，再放进 SESSION_SECRET_KEY 事件体传输
     */
    public static String rsaEncrypt(String data, PublicKey publicKey) {
        byte[] encrypted = rsaEncrypt(data.getBytes(StandardCharsets.UTF_8), publicKey);
        return Base64.getEncoder().encodeToString(encrypted);
    }

    /**
     * RSA 公钥加密 Object（JSON 序列化后加密），返回 Base64 编码字符串
     * 同样受单块长度上限约束，仅适用于小对象；序列化失败与加密失败都统一包装为 RuntimeException
     */
    public static String rsaEncryptObject(Object obj, PublicKey publicKey) {
        try {
            String json = OBJECT_MAPPER.writeValueAsString(obj);
            return rsaEncrypt(json, publicKey);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("RSA encrypt object failed: JSON serialization error", e);
        }
    }

    /**
     * RSA 私钥解密（用于解密 AES 密钥）
     * 由客户端在收到 SESSION_SECRET_KEY 后调用，私钥来自 channel attr，解出会话密钥后应尽快清除 attr 中的私钥
     */
    public static byte[] rsaDecrypt(byte[] encryptedData, PrivateKey privateKey) {
        try {
            Cipher cipher = Cipher.getInstance(RSA_TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, privateKey);
            return cipher.doFinal(encryptedData);
        } catch (Exception e) {
            throw new RuntimeException("RSA decryption failed", e);
        }
    }

    /**
     * RSA 私钥解密 Base64 编码字符串，返回原始字符串
     * Base64 解码或解密失败都会抛 RuntimeException（解密失败通常意味着密钥不匹配或数据被篡改）
     */
    public static String rsaDecrypt(String encryptedBase64, PrivateKey privateKey) {
        byte[] encryptedData = Base64.getDecoder().decode(encryptedBase64);
        byte[] decrypted = rsaDecrypt(encryptedData, privateKey);
        return new String(decrypted, StandardCharsets.UTF_8);
    }

    /**
     * RSA 私钥解密 Base64 编码字符串，反序列化为指定类型
     */
    public static <T> T rsaDecryptToObject(String encryptedBase64, PrivateKey privateKey, Class<T> clazz) {
        String json = rsaDecrypt(encryptedBase64, privateKey);
        try {
            return OBJECT_MAPPER.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("RSA decrypt to object failed: JSON deserialization error", e);
        }
    }

    /**
     * 公钥转 Base64 字符串（用于网络传输）
     * 编码的是 Java 的 X.509 SubjectPublicKeyInfo 格式（PublicKey.getEncoded 的原始字节），必须与 base64ToPublicKey 配对使用
     * 该值在 SESSION_PUBLIC_KEY 事件中以明文发送，公钥本身不敏感，通道的机密性依赖后续的 AES 会话密钥
     */
    public static String publicKeyToBase64(PublicKey publicKey) {
        return Base64.getEncoder().encodeToString(publicKey.getEncoded());
    }

    /**
     * Base64 字符串还原公钥
     * 按 X509EncodedKeySpec 解析，输入必须来自 publicKeyToBase64；格式非法时抛 RuntimeException
     * 本方法不做任何证书链或来源校验，公钥的可信度由业务层的认证钩子（如 beforePermit）负责
     */
    public static PublicKey base64ToPublicKey(String base64) {
        try {
            byte[] keyBytes = Base64.getDecoder().decode(base64);
            X509EncodedKeySpec keySpec = new X509EncodedKeySpec(keyBytes);
            KeyFactory keyFactory = KeyFactory.getInstance(RSA_ALGORITHM);
            return keyFactory.generatePublic(keySpec);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse public key from Base64", e);
        }
    }

    // ==================== AES ====================
    // AES-256-GCM 是 control 通道唯一的对称加密算法：会话密钥由服务端生成、经 RSA 包裹后下发给客户端，
    // 之后 control 通道上往来的一切数据都由 EventEncryptHandler 用该密钥加解密（proxy 业务数据流不在其列）

    private static final String AES_ALGORITHM = "AES";
    private static final String AES_TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int AES_KEY_SIZE = 256;
    // GCM 推荐的 IV 长度，本类固定 12 字节并随机生成、随密文一起传输
    private static final int GCM_IV_LENGTH = 12;
    // 认证标签长度（位），GCM 自带完整性校验，密文被篡改时解密会失败
    private static final int GCM_TAG_LENGTH = 128;

    /**
     * 生成 AES-256 会话密钥
     * 调用时机：ControlServer 收到客户端公钥后生成一次，随后即写入 channel attr 作为该连接后续的会话密钥
     */
    public static SecretKey generateAESKey() {
        try {
            KeyGenerator generator = KeyGenerator.getInstance(AES_ALGORITHM);
            generator.init(AES_KEY_SIZE, new SecureRandom());
            return generator.generateKey();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("AES key generation failed", e);
        }
    }

    /**
     * AES-GCM 加密（返回 iv + ciphertext 拼接结果）
     * 输出格式为 12 字节随机 IV 前置、其后为密文（密文尾部含 128 位认证标签），解密端依赖该格式切分，两端不可各自变更；
     * 每次调用都重新生成 IV，因此相同明文两次加密的结果不同
     */
    public static byte[] aesEncrypt(byte[] data, SecretKey secretKey) {
        try {
            byte[] iv = new byte[GCM_IV_LENGTH];
            new SecureRandom().nextBytes(iv);

            Cipher cipher = Cipher.getInstance(AES_TRANSFORMATION);
            GCMParameterSpec parameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, parameterSpec);
            byte[] ciphertext = cipher.doFinal(data);

            ByteBuffer byteBuffer = ByteBuffer.allocate(iv.length + ciphertext.length);
            byteBuffer.put(iv);
            byteBuffer.put(ciphertext);
            return byteBuffer.array();
        } catch (Exception e) {
            throw new RuntimeException("AES encryption failed", e);
        }
    }

    /**
     * AES-GCM 加密 String，返回 Base64 编码字符串
     * 内部先按 UTF-8 取字节再加密，密文（含 IV）整体 Base64，便于放进 JSON 事件体
     */
    public static String aesEncrypt(String data, SecretKey secretKey) {
        byte[] encrypted = aesEncrypt(data.getBytes(StandardCharsets.UTF_8), secretKey);
        return Base64.getEncoder().encodeToString(encrypted);
    }

    /**
     * AES-GCM 加密 Object（JSON 序列化后加密），返回 Base64 编码字符串
     * 用于把业务对象整体加密后作为事件体传递，序列化失败与加密失败都统一包装为 RuntimeException
     */
    public static String aesEncryptObject(Object obj, SecretKey secretKey) {
        try {
            String json = OBJECT_MAPPER.writeValueAsString(obj);
            return aesEncrypt(json, secretKey);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("AES encrypt object failed: JSON serialization error", e);
        }
    }

    /**
     * AES-GCM 解密（输入为 iv + ciphertext 拼接结果）
     * 输入格式必须与 aesEncrypt(byte[]) 的输出一致：前 12 字节为 IV，其余为密文；
     * 密钥不匹配、数据被篡改或格式不对都会抛出 RuntimeException，调用方应视为不可恢复的协议错误
     */
    public static byte[] aesDecrypt(byte[] encryptedData, SecretKey secretKey) {
        try {
            ByteBuffer byteBuffer = ByteBuffer.wrap(encryptedData);
            byte[] iv = new byte[GCM_IV_LENGTH];
            byteBuffer.get(iv);
            byte[] ciphertext = new byte[byteBuffer.remaining()];
            byteBuffer.get(ciphertext);

            Cipher cipher = Cipher.getInstance(AES_TRANSFORMATION);
            GCMParameterSpec parameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, parameterSpec);
            return cipher.doFinal(ciphertext);
        } catch (Exception e) {
            throw new RuntimeException("AES decryption failed", e);
        }
    }

    /**
     * AES-GCM 解密 Base64 编码字符串，返回原始字符串
     * 与 aesEncrypt(String) 互为逆操作，结果按 UTF-8 还原
     */
    public static String aesDecrypt(String encryptedBase64, SecretKey secretKey) {
        byte[] encryptedData = Base64.getDecoder().decode(encryptedBase64);
        byte[] decrypted = aesDecrypt(encryptedData, secretKey);
        return new String(decrypted, StandardCharsets.UTF_8);
    }

    /**
     * AES-GCM 解密 Base64 编码字符串，反序列化为指定类型
     * 解密与反序列化任一环节失败都会抛出 RuntimeException
     */
    public static <T> T aesDecryptToObject(String encryptedBase64, SecretKey secretKey, Class<T> clazz) {
        String json = aesDecrypt(encryptedBase64, secretKey);
        try {
            return OBJECT_MAPPER.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("AES decrypt to object failed: JSON deserialization error", e);
        }
    }

    /**
     * SecretKey 转 byte[]（用于 RSA 加密传输）
     * 即密钥的原始字节，不经任何编码；直接传输会泄漏会话密钥，必须再经 RSA 公钥加密后使用
     */
    public static byte[] aesKeyToBytes(SecretKey secretKey) {
        return secretKey.getEncoded();
    }

    /**
     * byte[] 还原 SecretKey
     * 不校验字节长度，长度不合法（非 16/24/32 字节）的密钥会在真正加解密时才由 JCE 报错
     */
    public static SecretKey bytesToAESKey(byte[] keyBytes) {
        return new SecretKeySpec(keyBytes, AES_ALGORITHM);
    }

    /**
     * SecretKey 转 Base64 字符串（用于 RSA 加密后网络传输）
     * 明文 Base64 等同于明文密钥，只应作为 rsaEncrypt 的输入使用
     */
    public static String aesKeyToString(SecretKey secretKey) {
        return Base64.getEncoder().encodeToString(secretKey.getEncoded());
    }

    /**
     * Base64 字符串还原 SecretKey
     * 客户端在 RSA 解出密钥字符串后调用，与 aesKeyToString 配对，不做长度校验
     */
    public static SecretKey stringToAESKey(String base64) {
        byte[] keyBytes = Base64.getDecoder().decode(base64);
        return new SecretKeySpec(keyBytes, AES_ALGORITHM);
    }
}
