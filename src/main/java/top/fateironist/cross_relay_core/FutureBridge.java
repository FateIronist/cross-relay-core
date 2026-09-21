package top.fateironist.cross_relay_core;

import top.fateironist.constack.Promise;

import java.util.concurrent.Future;

/**
 * 容器 Future 与对外 JUC Future 的边界桥接：容器 API 内部使用 constack Future，
 * 对外 Server/Client 接口保持 JUC Future 不变。
 */
public final class FutureBridge {

    private FutureBridge() {
    }

    /**
     * 桥接任意 JUC Future 为 constack Promise：原 Future 完成时以固定结果完成。
     * 等待在虚拟线程上执行，不占用调用方线程。
     */
    public static <T> Promise<T> bridge(Future<?> future, T result) {
        Promise<T> promise = new Promise<>();
        Thread.startVirtualThread(() -> {
            try {
                future.get();
                promise.setSuccess(result);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                promise.setFailure(e);
            } catch (Exception e) {
                promise.setFailure(e);
            }
        });
        return promise;
    }
}
