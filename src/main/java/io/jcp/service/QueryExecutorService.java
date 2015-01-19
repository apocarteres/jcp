package io.jcp.service;

import io.jcp.bean.Callback;

import java.util.Optional;

public interface QueryExecutorService<T, H> {
    void exec(T task, Optional<Callback<T, H>> callback);

    H exec(T task);

    default void shutdown() {};
}
