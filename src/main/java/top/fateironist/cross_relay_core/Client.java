package top.fateironist.cross_relay_core;

import top.fateironist.cross_relay_core.model.args.AbstractArgs;

import java.util.concurrent.Future;

/**
 * 客户端侧的顶层生命周期接口：只约定“连接 / 优雅关闭 / 立即关闭”三件事，不涉及具体协议、编解码与业务
 * 由 ControlClient（控制通道）以及 ProxyTcpClient、ProxyUdpClient、ProxyClient（数据面）等实现，
 * 上层业务通过该接口统一持有与停止各组件
 */
public interface Client {

    /**
     * 发起连接，参数为实现相关的连接参数（如 ControlClientConnectAbstractArgs、ProxyClientConnectArgs）
     * 具体语义由实现决定：ControlClient 只在 INIT 状态下建连（其余状态直接返回空 Future），
     * 而 proxy 侧实现每次调用都按参数新建一条隧道或绑定 channel，不做状态判断；
     * 因此调用方应以返回的 Future 是否成功判断结果，而不是依赖异常
     */
    Future<Void> connect(AbstractArgs abstractArgs);

    /**
     * 优雅关闭：异步释放本组件持有的连接与会话，不阻塞调用线程，返回的 Future 用于感知关闭完成
     * 单实现（ControlClient、ProxyTcpClient 等）由状态机决定是否执行并在完成后落终态；
     * 聚合门面（ProxyClient）则直接逐个关闭子实现，异常收敛在结果 Future 中
     */
    Future<?> close();

    /**
     * 立即关闭：语义同 close()，但是同步等待关闭完成，适合进程退出等无法等待异步回调的时机
     * 异常处理随实现而异（ControlClient 包装为 RuntimeException 抛出，聚合门面则直接透传子实现）
     */
    void closeNow();
}
