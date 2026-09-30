package top.fateironist.cross_relay_core;

/**
 * 标记接口：仅作为所有 listener（ControlClientListener、ControlServerListener、ProxyClientListener、ProxyServerListener）的公共父类型，
 * 本身不定义任何方法，不承担生命周期语义
 * 各 listener 以普通类形式提供空实现/放行的钩子方法，业务层按需覆写；核心层只负责在对应时机回调，不依赖任何具体实现
 */
public interface Listener {
}
