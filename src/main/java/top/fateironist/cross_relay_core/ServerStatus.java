package top.fateironist.cross_relay_core;

/**
 * 服务端组件（ControlServer、ProxyTcpServer、ProxyUdpServer 等）共用的生命周期状态机
 * 带状态机的组件（ControlServer、ServerInfoServer、ProxyTcpServer、ProxyUdpServer）以 volatile 字段持有当前状态：start 前为 INIT，
 * bind 成功后由 Future 回调置为 RUNNING，shutdown/shutdownNow 先置 STOPPING 再置 SHUTDOWN，且生命周期方法都先判断当前状态再决定是否执行；
 * 聚合门面 ProxyServer 不持有该状态、也不做状态判断，仅转发给子实现
 */
public enum ServerStatus {
    // INIT：初始态，组件已构造但尚未 start，是唯一允许 bind 监听的状态
    // RUNNING：监听已就绪（bind 的 Future 成功回调后置入），可接受客户端连接与外部请求
    // STOPPING：shutdown()/shutdownNow() 已触发、正在逐个关闭已接受会话的过渡态，此时不再接受新连接
    // SHUTDOWN：终态，所有会话已关闭；处于终态时 shutdown()/shutdownNow() 均为空操作
    INIT,
    RUNNING,
    STOPPING,
    SHUTDOWN
}
