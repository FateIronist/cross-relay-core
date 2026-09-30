package top.fateironist.cross_relay_core.util;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * JSON 工具类：对外只暴露一个全局共享的 Jackson ObjectMapper 单例
 * ObjectMapper 实例构造代价高且线程安全（读侧），故全项目复用同一实例供控制通道事件编解码（JsonEncoder/JsonDecoder）、
 * 代理注册包与服务端信息广播等各处使用（同包的 EncryptUtil 另有一个仅用于其对象加解密内部序列化的私有实例）；不提供任何自定义配置，编解码格式由默认配置决定，
 * 因此两端（客户端与服务端）必须使用同一份配置才能互通
 */
public class JsonUtil {
    // 共享单例，作为静态常量暴露给调用方（如 OBJECT_MAPPER.convertValue/readValue/writeValueAsBytes）
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
}
