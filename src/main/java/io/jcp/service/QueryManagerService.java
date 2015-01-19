package io.jcp.service;

import io.jcp.bean.Callback;

import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.Function;

public interface QueryManagerService<T, H> {
    Future<Optional<H>> submit(T task, Optional<Callback<T, H>> callback);

    Future<Optional<H>> submit(T query);

    H exec(T task);

    long countSubmitted();

    long countInProgress();

    QueryExecutorService<T, H> getExecutorService();

    default void shutdown() {
    }

}
