package top.fateironist.cross_relay_core.model.control;

public interface ControlEventEnum {
    String getType();
    default boolean matches(String str) {
        return this.getType().equals(str);
    }
}
