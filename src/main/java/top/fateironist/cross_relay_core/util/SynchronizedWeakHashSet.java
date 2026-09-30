package top.fateironist.cross_relay_core.util;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * 线程安全且对元素持弱引用的 Set：用 WeakHashMap 的 keySet 模拟集合语义，外层套 Collections.synchronizedMap 提供单次操作的同步
 * 解决的问题是“需要按对象去重地登记一批实体，但不想因此阻止这些实体被回收”——元素仅被本集合引用时会被 GC 自动摘除，
 * 因此 size/contains 的结果会随 GC 变化，本类不是强一致容器，不能用于需要精确计数或长期持有的场景；
 * 当前 main 源码中尚无调用点，属于预留工具
 * 关键约束：所有单次方法调用（含 addAll/removeAll/retainAll/removeIf/forEach/toArray，它们被同步包装整体加锁）都是线程安全的，
 * 但 iterator/spliterator/stream/parallelStream 返回的只是底层 keySet 的视图，遍历过程不持锁，并发遍历时必须由调用方自行加锁；
 * 另外本类未覆写 equals/hashCode，仍是 Object 的引用比较语义，不能作为标准 Set 放进其他集合参与相等判断
 */
public class SynchronizedWeakHashSet<E> implements Set<E> {

    // 底层用 Map 承载集合：元素作 key，value 统一为占位常量；WeakHashMap 保证 key 无强引用时可被 GC 回收
    transient Map<E, Object> map;
    // 所有 key 共用的占位 value，配合 map.put 的返回值即可表达“是否新增成功”
    private static final Object PRESENT = new Object();

    /**
     * 构造一个空的同步弱引用集合；三个重载分别用于默认容量、指定初始容量、指定初始容量与负载因子
     * 容量参数最终透传给 WeakHashMap，元素过多时应预估容量以减少扩容
     */
    public SynchronizedWeakHashSet() {
        map = Collections.synchronizedMap(new WeakHashMap<>());
    }

    public SynchronizedWeakHashSet(int initialCapacity, float loadFactor) {
        map = Collections.synchronizedMap(new WeakHashMap<>(initialCapacity, loadFactor));
    }

    public SynchronizedWeakHashSet(int initialCapacity) {
        map = Collections.synchronizedMap(new WeakHashMap<>(initialCapacity));
    }

    // Set methods
    // 以下读/写方法均直接委托给同步包装后的 map 或其 keySet：add 以 put 的返回值判断“原本不存在”来给出 Set 语义，
    // remove 以取回的 value 是否等于 PRESENT 判断“原本存在”，因此两者返回的是本次调用是否改变了集合，而非元素当前状态

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return map.containsKey(o);
    }

    @Override
    public Object[] toArray() {
        return map.keySet().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return map.keySet().toArray(a);
    }

    @Override
    public boolean add(E e) {
        return map.put(e, PRESENT) == null;
    }

    @Override
    public boolean remove(Object o) {
        return map.remove(o) == PRESENT;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return map.keySet().containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        return map.keySet().addAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return map.keySet().retainAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return map.keySet().removeAll(c);
    }

    @Override
    public void clear() {
        map.clear();
    }


    @Override
    public int size() {
        return map.size();
    }

    // iterator/spliterator/stream/parallelStream 返回的只是底层 keySet 的视图，遍历过程不持锁，
    // 并发场景下需调用方自行在 synchronized 块内完成遍历；同时由于 key 是弱引用，遍历结果还可能因 GC 随时少掉元素
    @Override
    public Iterator<E> iterator() {
        return map.keySet().iterator();
    }

    @Override
    public Spliterator<E> spliterator() {
        return map.keySet().spliterator();
    }

    @Override
    public <T> T[] toArray(IntFunction<T[]> generator) {
        return map.keySet().toArray(generator);
    }

    @Override
    public boolean removeIf(Predicate<? super E> filter) {
        return map.keySet().removeIf(filter);
    }

    @Override
    public Stream<E> stream() {
        return map.keySet().stream();
    }

    @Override
    public Stream<E> parallelStream() {
        return map.keySet().parallelStream();
    }

    @Override
    public void forEach(Consumer<? super E> action) {
        map.keySet().forEach(action);
    }
}
