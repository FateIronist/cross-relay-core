package top.fateironist.cross_relay_core;

import top.fateironist.cross_relay_core.model.args.AbstractArgs;

import java.util.concurrent.Future;

/**
 * 服务端侧的顶层生命周期接口：只约定“启动 / 优雅停机 / 立即停机”三件事，不涉及具体协议与业务
 * 由 ControlServer（控制通道）以及 ProxyTcpServer、ProxyUdpServer、ProxyServer（数据面）等实现
 */
public interface Server {

    /**
     * 启动监听，参数为实现相关的启动参数（如 ControlServerStartArgs、ProxyServerStartArgs）
     * 具体语义由实现决定：带状态机的实现（ControlServer、ProxyTcpServer/ProxyUdpServer）只在 INIT 状态下 bind，
     * 成功后把状态推进到 RUNNING，其余状态不重复启动并返回一个已失败的 Future；
     * 聚合门面（ProxyServer）每次调用都按 Options 启停子服务器，不做状态判断
     */
    Future<Void> start(AbstractArgs abstractArgs);

    /**
     * 优雅停机：先置为 STOPPING，再异步逐个关闭已接受的会话/连接，全部完成后置为 SHUTDOWN
     * 不阻塞调用线程，返回的 Future 用于感知停机完成；带状态机的实现处于非 RUNNING/INIT 状态时为空操作并返回空 Future
     */
    Future<?> shutdown();

    /**
     * 立即停机：语义同 shutdown()，但是同步等待所有会话关闭完成，适合进程退出等无法等待异步回调的时机
     * 异常处理随实现而异（ControlServer 对关闭失败抛出 RuntimeException 并中断整个流程）
     */
    void shutdownNow();
}
