package io.jcp.service;

import io.jcp.bean.ExecutionCallback;

import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.Function;

public interface QueryExecutorService<T, H> {
    Future<Optional<H>> submit(T query, Optional<ExecutionCallback<T, H>> callback);

    Future<Optional<H>> submit(T query, Function<T, H> f, Optional<ExecutionCallback<T, H>> callback);

    Future<Optional<H>> submit(T query);

    Optional<H> exec(T query);

    long countSubmitted();

    long countInProgress();

    default void shutdown() {
    }

}
