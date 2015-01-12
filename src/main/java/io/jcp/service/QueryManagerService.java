package io.jcp.service;

import io.jcp.bean.Callback;

import java.util.Optional;

public interface QueryManagerService<T, H> {
    void submit(T task, Optional<Callback<T, H>> callback);

    H exec(T task) throws InterruptedException;

    long countSubmitted();

    long countInProgress();

    default void shutdown() throws InterruptedException {
    }

}
