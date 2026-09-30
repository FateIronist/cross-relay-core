package top.fateironist.cross_relay_core.model.control;

/**
 * 协议/握手层事件：只服务于 control 通道自身的建立与保活（密钥交换、准入、心跳、关闭），不承载代理业务语义。
 * 收发点集中在 ControlClient / ControlServer 的 pipeline handler，以及 UDP 元数据用的 ServerInfoServer；
 * 代理业务层的事件请见 ProxyControlEventEnum。
 */
public enum ControlProtocolEventEnum implements ControlEventEnum{
    // UDP 元数据查询与应答：客户端向 ServerInfoServer(3461) 查询，服务端以同类型回传 ProxyServerInfo JSON；整条发现流程当前无调用方，属备选寻址手段
    SERVER_INFO,
    // 预留：定义但库内 main 源码无收发点，仅在测试代码中被当作自定义事件使用
    CLIENT_INFO,
    // 客户端→服务端：心跳，客户端在 permit 后按 pingInterval 定时发送，body 携带 pingTime
    PING,
    // 服务端→客户端：PING 的回显，客户端收到后经 ProxyServerInfo.receivePong 计算往返延迟
    PONG,
    // 预留：定义但库内 main 源码无收发点（应答语义当前由事件信封的 ack 字段承担）
    ACK,
    // 服务端→客户端：准入通过；body 为 controlId 与 proxyServerInfo，客户端据此回填会话 id 并合并服务端地址，是握手的完成点
    CONNECTION_PERMIT,
    // 客户端→服务端，明文：body 为 publicKey，客户端在 channelActive 时首个发出自己的 RSA 公钥
    SESSION_PUBLIC_KEY,
    SESSION_SECRET_KEY,   // S→C，明文：body 为 secretKey，即被 RSA 公钥加密后的 AES 会话密钥；服务端收到公钥后下发，客户端解密后写入 channel attr，链路随即转为加密
    // 客户端→服务端，已加密：body 为 ProxyClientInfo 身份信息，是服务端 beforePermit 认证的依据
    SESSION_SECRET_ACK,
    // 双向：由 ControlContext.closeRemote() 发出，告知对端本端会话即将关闭
    CLOSE,
    // 服务端→客户端：body 为 Error；握手期拒绝（未加密就发业务事件、beforePermit 未通过）时先告知原因再关闭连接
    ERROR;

    @Override
    public String getType() {
        return this.toString();
    }
}
