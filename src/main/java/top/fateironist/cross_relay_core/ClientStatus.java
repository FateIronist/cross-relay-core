package top.fateironist.cross_relay_core;

/**
 * 客户端组件（ControlClient、ProxyTcpClient、ProxyUdpClient 等）共用的生命周期状态机
 * 带状态机的组件（ControlClient、ProxyTcpClient、ProxyUdpClient）以 volatile 字段持有当前状态：connect 前为 INIT，
 * 连接成功后由 Future 回调置为 OPEN，close/closeNow 先置 CLOSING 再置 CLOSED，且生命周期方法都先判断当前状态再决定是否执行；
 * 聚合门面 ProxyClient 不持有该状态、也不做状态判断，仅转发给子实现
 */
public enum ClientStatus {
    // INIT：初始态，组件已构造但尚未 connect，是唯一允许发起连接的状态
    // OPEN：连接已建立（connect 的 Future 成功回调后置入），可正常收发数据与建立隧道
    // CLOSING：close()/closeNow() 已触发、底层连接正在异步关闭的过渡态，此时不再接受新业务
    // CLOSED：终态，连接已完全关闭；处于终态时 close()/closeNow() 均为空操作
    INIT,OPEN,CLOSING,CLOSED
}
