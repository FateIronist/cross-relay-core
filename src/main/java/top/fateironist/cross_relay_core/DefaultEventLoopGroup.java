package top.fateironist.cross_relay_core;

import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

import java.util.function.Consumer;

public class DefaultEventLoopGroup {
    public static final EventLoopGroup GROUP = new MultiThreadIoEventLoopGroup(5, NioIoHandler.newFactory());

    public static <V> Promise<V> newPromise() {
        return GROUP.next().newPromise();
    }

    public static <V> Future<V> newPromise(Consumer<Promise<V>> consumer) {
        EventLoop eventLoop = GROUP.next();
        Promise<V> promise = eventLoop.newPromise();
        eventLoop.execute(() -> consumer.accept(promise));
        return promise;
    }

    public static Future<?> emptyFuture() {
        EventLoop eventLoop = GROUP.next();
        Promise<Void> promise = eventLoop.newPromise();
        promise.setSuccess(null);
        return promise;
    }

    public static Future<Void> emptyFuture(Void v) {
        EventLoop eventLoop = GROUP.next();
        Promise<Void> promise = eventLoop.newPromise();
        promise.setSuccess(null);
        return promise;
    }

    public static Future<?> failFuture(Throwable throwable) {
        EventLoop eventLoop = GROUP.next();
        Promise<Void> promise = eventLoop.newPromise();
        promise.setFailure(throwable);
        return promise;
    }

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
