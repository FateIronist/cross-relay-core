package top.fateironist.cross_relay_core.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ConcurrentBlockingHashMap <K, V> {
    private final Map<K, BlockingValue<V>> map;

    public ConcurrentBlockingHashMap(int initialCapacity) {
        this.map = new ConcurrentHashMap<>(initialCapacity);
    }

    public V get(K key) {
        BlockingValue<V> blockingValue = map.computeIfAbsent(key, k -> new BlockingValue<>());

        blockingValue.lock.lock();
        try {
            while (blockingValue.value == null) {
                blockingValue.condition.await();
            }
            return blockingValue.value;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } finally {
            blockingValue.lock.unlock();
        }
    }

    public void put(K key, V value) {
        BlockingValue<V> blockingValue = map.computeIfAbsent(key, k -> new BlockingValue<>());

        blockingValue.lock.lock();
        try {
            blockingValue.value = value;
            blockingValue.condition.signalAll();
        } finally {
            blockingValue.lock.unlock();
        }
    }

    private static class BlockingValue<V> {
        public V value;
        public ReentrantLock lock = new ReentrantLock();
        public Condition condition = lock.newCondition();
    }
}
