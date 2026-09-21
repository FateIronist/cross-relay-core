package top.fateironist.cross_relay_core;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * synchronized、非公平 ReentrantLock、公平 ReentrantLock 在不同并发等级下的性能对比（微基准）。
 * <p>
 * 公平锁作为对照：它失去非公平模式的 barging（插队）能力，若高并发下明显劣化，
 * 则说明前一轮实验中非公平 lock 的优势主要来自唤醒策略而非锁原语本身。
 * <p>
 * 两种场景：
 * 1. 纯锁竞争：临界区仅一次计数自增；
 * 2. 稍重临界区：临界区内额外做一次简单的哈希混运算，模拟持有锁期间的真实开销。
 * <p>
 * 注意：这是 JUnit 内嵌的简易基准，非 JMH，结果用于量级参考，不作为精确指标。
 */
class LockPerformanceTest {

    /** 每轮总操作数（所有线程分摊） */
    private static final long TOTAL_OPS = 2_000_000L;
    /** 预热轮数，触发 JIT */
    private static final int WARMUP_ROUNDS = 2;
    /** 测量轮数，取最优值以降噪 */
    private static final int MEASURE_ROUNDS = 3;
    /** 待测并发等级 */
    private static final int[] CONCURRENCY_LEVELS = {1, 2, 4, 8, 16, 32};

    @Test
    void synchronizedVsReentrantLock() throws InterruptedException {
        List<LockImpl> impls = List.of(
                new LockImpl("sync        ", () -> new Object(), (lock, body) -> {
                    synchronized (lock) {
                        body.run();
                    }
                }),
                new LockImpl("lock-unfair ", () -> new ReentrantLock(), (lock, body) -> {
                    ((ReentrantLock) lock).lock();
                    try {
                        body.run();
                    } finally {
                        ((ReentrantLock) lock).unlock();
                    }
                }),
                new LockImpl("lock-fair   ", () -> new ReentrantLock(true), (lock, body) -> {
                    ((ReentrantLock) lock).lock();
                    try {
                        body.run();
                    } finally {
                        ((ReentrantLock) lock).unlock();
                    }
                })
        );

        System.out.println("===== 场景1：轻临界区（lock + counter++）=====");
        runScenario(impls);

        System.out.println("\n===== 场景2：稍重临界区（临界区内额外哈希混运算）=====");
        runScenario(impls);
    }

    /** 一种锁实现：名称、锁对象工厂、加锁/解锁包装 */
    private record LockImpl(String name, Supplier<Object> lockSupplier, LockAction action) {
    }

    /**
     * 遍历并发等级，对每种锁实现分别测量并打印对比结果。
     * <p>
     * 公平锁在单线程/低并发下没有队列竞争，语义与公平性无差，但为保持结构统一仍全量测量。
     */
    private void runScenario(List<LockImpl> impls) throws InterruptedException {
        System.out.printf("%-14s %-8s %12s %14s%n", "impl", "threads", "elapsed(ms)", "ops/s");
        for (int threads : CONCURRENCY_LEVELS) {
            // 预热
            for (int i = 0; i < WARMUP_ROUNDS; i++) {
                for (LockImpl impl : impls) {
                    measure(threads, impl.lockSupplier(), impl.action());
                }
            }
            // 测量，取最优
            long[] best = new long[impls.size()];
            java.util.Arrays.fill(best, Long.MAX_VALUE);
            for (int i = 0; i < MEASURE_ROUNDS; i++) {
                for (int k = 0; k < impls.size(); k++) {
                    best[k] = Math.min(best[k], measure(threads, impls.get(k).lockSupplier(), impls.get(k).action()));
                }
            }
            for (int k = 0; k < impls.size(); k++) {
                printRow(impls.get(k).name(), threads, best[k]);
            }
        }
    }

    private void printRow(String impl, int threads, long elapsedNanos) {
        System.out.printf("%-14s %-8d %12.1f %14s%n",
                impl, threads, elapsedNanos / 1_000_000.0, String.format("%,d", opsPerSecond(elapsedNanos)));
    }

    private long opsPerSecond(long elapsedNanos) {
        return (long) (TOTAL_OPS / (elapsedNanos / 1_000_000_000.0));
    }

    /**
     * 用指定锁实现执行 TOTAL_OPS 次临界区操作，返回耗时（纳秒）。
     */
    private long measure(int threads, Supplier<Object> lockSupplier, LockAction action) throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        Object lock = lockSupplier.get();
        AtomicLong counter = new AtomicLong();
        long opsPerThread = TOTAL_OPS / threads;

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (long i = 0; i < opsPerThread; i++) {
                        action.run(lock, counter::incrementAndGet);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        long begin = System.nanoTime();
        start.countDown();
        done.await(2, TimeUnit.MINUTES);
        long elapsed = System.nanoTime() - begin;
        pool.shutdownNow();
        if (counter.get() < TOTAL_OPS) {
            throw new IllegalStateException("counter 未达到预期值，测量无效: " + counter.get());
        }
        return elapsed;
    }

    /** 稍重临界区的工作负载：哈希混运算 + blackhole 防止 JIT 死码消除 */
    private void mix(Runnable body) {
        int h = (int) Thread.currentThread().getId();
        h ^= (h >>> 16);
        h *= 0x5bd1e995;
        h ^= (h >>> 15);
        if (h == 0xdeadbeef) {
            // 几乎不可能触发，仅阻止 JIT 将计算判定为死代码
            System.out.print("");
        }
        body.run();
    }

    @FunctionalInterface
    interface LockAction {
        void run(Object lock, Runnable body);
    }
}
