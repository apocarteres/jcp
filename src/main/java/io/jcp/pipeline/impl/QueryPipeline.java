package io.jcp.pipeline.impl;

import io.jcp.pipeline.Pipeline;
import io.jcp.pipeline.callback.QueryCompleteCallback;
import io.jcp.service.QueryExecutorService;

import java.util.Optional;
import java.util.function.Function;

public final class QueryPipeline<T, H> implements Pipeline<T, H> {

    private static final String USING_DIRECTIVE_MUST_BE_FIRST_DEFINED = "'using()' directive must be specified only " +
        "once and must be the first directive in the pipeline";
    private final Optional<Function<T, H>> function;
    private final Optional<T> query;
    private final Optional<QueryExecutorService<T, H>> service;
    private final Optional<QueryCompleteCallback<T, H>> callback;
    private static final String AT_LEAST_ONE_QUERY_MUST_BE_SPECIFIED = "at least one query must be specified";

    public QueryPipeline() {
        this(
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty()
        );
    }

    public QueryPipeline(
        Optional<Function<T, H>> function,
        Optional<T> query,
        Optional<QueryExecutorService<T, H>> service,
        Optional<QueryCompleteCallback<T, H>> callback
    ) {
        this.function = function;
        this.query = query;
        this.service = service;
        this.callback = callback;
    }

    @Override
    public Pipeline<T, H> run(T query) {
        return new QueryPipeline<>(
            Optional.of(q -> {
                if (!service.isPresent()) {
                    throw new IllegalStateException(
                        USING_DIRECTIVE_MUST_BE_FIRST_DEFINED
                    );
                }
                Optional<H> exec = service.get().exec(q);
                H r = null;
                if (exec.isPresent()) {
                    r = exec.get();
                }
                if (callback.isPresent()) {
                    callback.get().onComplete(query, exec);
                }
                return r;
            }),
            Optional.of(query),
            service,
            callback
        );
    }

    @Override
    public <R, K> Pipeline<R, K> run(
        Function<H, R> function, Pipeline<R, K> underlying
    ) {
        return underlying.run(
            this.function.get().andThen(function).apply(
                query.get()
            )
        );
    }

    @Override
    public Pipeline<T, H> run(Function<H, T> function) {
        if (!this.query.isPresent()) {
            throw new IllegalStateException(
                AT_LEAST_ONE_QUERY_MUST_BE_SPECIFIED
            );
        }
        return run(
            this.function.get().andThen(function).apply(
                this.query.get()
            )
        );
    }


    @Override
    public <R, K> Pipeline<R, K> using(QueryExecutorService<R, K> service) {
        if (
            this.query.isPresent() || this.callback.isPresent()
                || this.service.isPresent()
            ) {
            throw new IllegalStateException(USING_DIRECTIVE_MUST_BE_FIRST_DEFINED);
        }
        return new QueryPipeline<>(
            Optional.empty(),
            Optional.empty(),
            Optional.of(service),
            Optional.empty()
        );
    }

    @Override
    public Pipeline<T, H> on(QueryCompleteCallback<T, H> callback) {
        return new QueryPipeline<>(
            this.function,
            this.query,
            this.service,
            Optional.of(callback)
        );
    }

    @Override
    public Optional<H> product() {
        if (!this.query.isPresent()) {
            throw new IllegalStateException(AT_LEAST_ONE_QUERY_MUST_BE_SPECIFIED);
        }
        return Optional.ofNullable(
            this.function.get().apply(this.query.get())
        );
    }
}
