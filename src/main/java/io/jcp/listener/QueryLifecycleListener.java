package io.jcp.listener;


public interface QueryLifecycleListener<T> {
    default void onSubmit(T request) {
    }

    default void onExec(T request) {
    }
}
