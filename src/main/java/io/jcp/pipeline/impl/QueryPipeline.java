package io.jcp.pipeline.impl;

import io.jcp.pipeline.Pipeline;
import io.jcp.service.QueryManagerService;

import java.util.Optional;
import java.util.function.Function;

public final class QueryPipeline<T, H> implements Pipeline<T, H> {

    private final Optional<Pipeline<T, H>> parent;
    private final Optional<T> query;
    private final Optional<QueryManagerService<T, H>> service;

    public QueryPipeline() {
        this(Optional.empty(), Optional.empty(), Optional.empty());
    }

    public QueryPipeline(
        Optional<Pipeline<T, H>> parent, Optional<T> query, Optional<QueryManagerService<T, H>> service
    ) {
        this.parent = parent;
        this.query = query;
        this.service = service;
    }

    @Override
    public Pipeline<T, H> run(T query) {
        return new QueryPipeline<>(Optional.of(this), Optional.of(query), Optional.empty());
    }

    @Override
    public <F, K> Pipeline<F, K>
    run(Function<? super T, ? extends F> mapper, Class<K> type) {
        return null;
    }

    @Override
    public Pipeline<T, H> with(QueryManagerService<T, H> service) {
        return new QueryPipeline<>(Optional.of(this), Optional.empty(), Optional.of(service));
    }

    @Override
    public H collect() {
        Optional<T> query = this.query;
        Optional<QueryManagerService<T, H>> service = this.service;
        Optional<Pipeline<T, H>> parent = this.parent;
        while (parent.isPresent()) {
            if (!query.isPresent()) {
                query = parent.get().getQuery();
            } else {
                if (parent.get().getQuery().isPresent()) {
                    throw new IllegalStateException("query must be specified once");
                }
            }
            if (!service.isPresent()) {
                service = parent.get().getService();
            } else {
                if (parent.get().getService().isPresent()) {
                    throw new IllegalStateException("service must be specified once");
                }
            }
            parent = parent.get().getParent();
        }
        if (!query.isPresent()) {
            throw new IllegalStateException("query must be specified");
        }
        if (!service.isPresent()) {
            throw new IllegalStateException("service must be specified");
        }
        try {
            return service.get().exec(query.get());
        } catch (InterruptedException e) {
            throw new RuntimeException("exception occurred during collect", e);
        }
    }

    @Override
    public Optional<Pipeline<T, H>> getParent() {
        return this.parent;
    }

    @Override
    public Optional<T> getQuery() {
        return this.query;
    }

    @Override
    public Optional<QueryManagerService<T, H>> getService() {
        return this.service;
    }
}
