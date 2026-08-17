package top.fateironist.cross_relay_core;


import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.Future;

public class Test {
    public static void main(String[] args) throws Exception {
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);
        EventLoop eventLoop = eventLoopGroup.next();
        Future<Void> future = eventLoop.newPromise();

        future.addListener(f -> {
            Thread.sleep(1000);
            System.out.println("Future completed");
        });

        long startTime = System.currentTimeMillis();
        Promise<Void> promise = (Promise<Void>) future;
        promise.setSuccess(null);
        future.get();
        System.out.println("Time taken: " + (System.currentTimeMillis() - startTime) + "ms");
    }
}
