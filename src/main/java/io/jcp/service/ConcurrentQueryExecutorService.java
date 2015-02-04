package io.jcp.service;

import io.jcp.bean.ExecutionCallback;

import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.Function;

public interface ConcurrentQueryExecutorService<T, H> extends QueryExecutorService<T, H> {
    Future<Optional<H>> submit(T query, Optional<ExecutionCallback<T, H>> callback);

    Future<Optional<H>> submit(T query, Function<T, H> f, Optional<ExecutionCallback<T, H>> callback);

    Future<Optional<H>> submit(T query);

    long countSubmitted();

    long countInProgress();

}
