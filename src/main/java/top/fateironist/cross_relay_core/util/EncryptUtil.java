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

public class EncryptUtil {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    // ==================== RSA ====================

    private static final String RSA_ALGORITHM = "RSA";
    private static final String RSA_TRANSFORMATION = "RSA/ECB/OAEPWithSHA-256AndMGF1Padding";
    private static final int RSA_KEY_SIZE = 2048;

    /**
     * 生成 RSA 密钥对
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
     */
    public static String rsaEncrypt(String data, PublicKey publicKey) {
        byte[] encrypted = rsaEncrypt(data.getBytes(StandardCharsets.UTF_8), publicKey);
        return Base64.getEncoder().encodeToString(encrypted);
    }

    /**
     * RSA 公钥加密 Object（JSON 序列化后加密），返回 Base64 编码字符串
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
     */
    public static String publicKeyToBase64(PublicKey publicKey) {
        return Base64.getEncoder().encodeToString(publicKey.getEncoded());
    }

    /**
     * Base64 字符串还原公钥
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

    private static final String AES_ALGORITHM = "AES";
    private static final String AES_TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int AES_KEY_SIZE = 256;
    private static final int GCM_IV_LENGTH = 12;
    private static final int GCM_TAG_LENGTH = 128;

    /**
     * 生成 AES 密钥
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
     */
    public static String aesEncrypt(String data, SecretKey secretKey) {
        byte[] encrypted = aesEncrypt(data.getBytes(StandardCharsets.UTF_8), secretKey);
        return Base64.getEncoder().encodeToString(encrypted);
    }

    /**
     * AES-GCM 加密 Object（JSON 序列化后加密），返回 Base64 编码字符串
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
     */
    public static String aesDecrypt(String encryptedBase64, SecretKey secretKey) {
        byte[] encryptedData = Base64.getDecoder().decode(encryptedBase64);
        byte[] decrypted = aesDecrypt(encryptedData, secretKey);
        return new String(decrypted, StandardCharsets.UTF_8);
    }

    /**
     * AES-GCM 解密 Base64 编码字符串，反序列化为指定类型
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
     */
    public static byte[] aesKeyToBytes(SecretKey secretKey) {
        return secretKey.getEncoded();
    }

    /**
     * byte[] 还原 SecretKey
     */
    public static SecretKey bytesToAESKey(byte[] keyBytes) {
        return new SecretKeySpec(keyBytes, AES_ALGORITHM);
    }

    /**
     * SecretKey 转 Base64 字符串（用于 RSA 加密后网络传输）
     */
    public static String aesKeyToString(SecretKey secretKey) {
        return Base64.getEncoder().encodeToString(secretKey.getEncoded());
    }

    /**
     * Base64 字符串还原 SecretKey
     */
    public static SecretKey stringToAESKey(String base64) {
        byte[] keyBytes = Base64.getDecoder().decode(base64);
        return new SecretKeySpec(keyBytes, AES_ALGORITHM);
    }
}
