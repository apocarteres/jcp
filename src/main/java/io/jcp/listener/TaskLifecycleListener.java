package io.jcp.listener;


public interface TaskLifecycleListener<T> {
    default void onSubmit(T request) {
    }

    default void onExec(T request) {
    }
}
