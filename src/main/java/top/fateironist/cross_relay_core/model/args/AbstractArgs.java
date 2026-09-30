package top.fateironist.cross_relay_core.model.args;

/**
 * 各组件启动/连接参数（Args）的公共抽象基类，本身不定义任何字段，仅作类型标记：
 * 顶层接口 Server.start(AbstractArgs) 与 Client.connect(AbstractArgs) 借此统一方法签名，各实现拿到后再向下转型为具体 Args。
 */
public abstract class AbstractArgs {

}


