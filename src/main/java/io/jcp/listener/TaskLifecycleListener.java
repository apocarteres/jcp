package io.jcp.listener;

import io.jcp.bean.Query;

public interface TaskLifecycleListener<T extends Query> {
    default void onSubmit(T request) {
    }

    default void onExec(T request) {
    }
}
