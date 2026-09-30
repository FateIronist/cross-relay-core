package top.fateironist.cross_relay_core;

import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

import java.util.function.Consumer;

/**
 * 全局共享的 Netty EventLoopGroup 与配套的 Future 工具类
 * 各组件（control 与 proxy）实际使用的 EventLoopGroup 由构造方从上层注入，本类的 GROUP 只用于承载这些静态工具创建的 Promise
 * 以及少量内部定时任务（如 ServerUdpProxyContext 的超时扫描），从而不必为“返回一个已完成的 Future”这类需求额外创建线程；
 * 工具方法统一解决“生命周期方法在状态不满足时需要返回一个已完成的 Future”这一需求
 */
public class DefaultEventLoopGroup {
    // 全进程共享的事件循环组：固定 5 个线程、NIO 实现；仅由本类的 Future 工具方法与内部定时任务使用，组件的 channel 注册在上层注入的事件循环组上
    public static final EventLoopGroup GROUP = new MultiThreadIoEventLoopGroup(5, NioIoHandler.newFactory());

    /**
     * 在共享线程组上创建一个未完成的 Promise，供调用方异步 setSuccess/setFailure
     * 主要用于 shutdown() 这类需要“把所有子关闭结果汇总成一个 Future”的场景
     */
    public static <V> Promise<V> newPromise() {
        return GROUP.next().newPromise();
    }

    /**
     * 在共享线程组上创建一个 Promise，并把 consumer 提交到事件循环线程中执行
     * 契约：consumer 需要在事件循环线程内完成 promise，否则该 Future 将永远不完成；
     * 又因为运行在事件循环线程上，consumer 内不应做长时间阻塞操作，否则会拖慢同线程上的其他 channel
     */
    public static <V> Future<V> newPromise(Consumer<Promise<V>> consumer) {
        EventLoop eventLoop = GROUP.next();
        Promise<V> promise = eventLoop.newPromise();
        eventLoop.execute(() -> consumer.accept(promise));
        return promise;
    }

    /**
     * 返回一个“已完成且成功、结果为 null”的 Future
     * 语义是“本次调用无需执行任何操作”，用于生命周期方法在状态不满足（重复 close/start 等）时的统一返回值
     */
    public static Future<?> emptyFuture() {
        EventLoop eventLoop = GROUP.next();
        Promise<Void> promise = eventLoop.newPromise();
        promise.setSuccess(null);
        return promise;
    }

    /**
     * 与 emptyFuture() 语义完全相同，仅通过一个占位参数把返回类型固定为 Future&lt;Void&gt;，
     * 以适配 Client.connect 这类返回 Future&lt;Void&gt; 的方法签名
     */
    public static Future<Void> emptyFuture(Void v) {
        EventLoop eventLoop = GROUP.next();
        Promise<Void> promise = eventLoop.newPromise();
        promise.setSuccess(null);
        return promise;
    }

    /**
     * 返回一个“已完成但失败”的 Future，用于把“当前状态不允许执行该操作”这类拒绝原因告知调用方
     * 注意：异常被封装在 Future 中而非直接抛出，调用方不 get/listener 就会被静默忽略
     */
    public static <V> Future<V> failFuture(Throwable throwable) {
        EventLoop eventLoop = GROUP.next();
        Promise<V> promise = eventLoop.newPromise();
        promise.setFailure(throwable);
        return promise;
    }

    /**
     * 意图是把多个 Future 合并为一个“全部完成才完成”的 Future，目前用于 TunnelContext 级联关闭多个 channel 的场景
     * 已知问题（设计文档 §6 记载，源码现状未修复）：“在事件循环任务中先 setSuccess(null) 后 f.get()”，
     * 即 setSuccess 在提交任务后由调用线程立即执行，而遍历 f.get() 的任务才刚入队，
     * 因此返回的 Future 在任何子 Future 完成前就已成功，等待中的异常也无法再写入（setFailure 落在已完成的 promise 上），
     * 结果就是合并失败信号丢失；调用方不能依赖该返回值判断级联关闭是否全部成功
     */
    public static Future<?> combine(Future<?>... futures) {
        EventLoop eventLoop = GROUP.next();
        Promise<?> promise = eventLoop.newPromise();
        eventLoop.execute(() -> {
            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (Exception e) {
                    promise.setFailure(e);
                }
            }
        });

        promise.setSuccess(null);
        return promise;
    }
}
