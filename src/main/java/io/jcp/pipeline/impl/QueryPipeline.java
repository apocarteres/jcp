package io.jcp.pipeline.impl;

import io.jcp.pipeline.Pipeline;
import io.jcp.service.QueryManagerService;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public final class QueryPipeline<T, H> implements Pipeline<T, H> {

    private final Optional<Pipeline<T, H>> parent;
    private final Optional<Collection<T>> queries;
    private final Optional<QueryManagerService<T, H>> service;

    public QueryPipeline() {
        this(Optional.empty(), Optional.empty(), Optional.empty());
    }

    public QueryPipeline(
        Optional<Pipeline<T, H>> parent, Optional<Collection<T>> queries, Optional<QueryManagerService<T, H>> service
    ) {
        this.parent = parent;
        this.queries = queries;
        this.service = service;
    }

    @Override
    public Pipeline<T, H> run(T query) {
        return new QueryPipeline<>(
            Optional.of(this), Optional.of(new ArrayList<>(Collections.singletonList(query))),
            Optional.empty()
        );
    }

    @Override
    public Pipeline<T, H> run(Collection<T> query) {
        return new QueryPipeline<>(
            Optional.of(this), Optional.of(query),
            Optional.empty()
        );
    }

    @Override
    public <F, K> Pipeline<F, K>
    run(Function<? super T, ? extends F> mapper, Class<K> type) {
        return null;
    }

    @Override
    public Pipeline<T, H> using(QueryManagerService<T, H> service) {
        return new QueryPipeline<>(Optional.of(this), Optional.empty(), Optional.of(service));
    }

    @Override
    public Stream<H> stream() {
        Optional<Collection<T>> queries = this.queries;
        Optional<QueryManagerService<T, H>> service = this.service;
        Optional<Pipeline<T, H>> parent = this.parent;
        while (parent.isPresent()) {
            if (!queries.isPresent()) {
                queries = parent.get().getQueries();
            } else {
                if (service.isPresent()) {
                    Optional<Collection<T>> parentQueries = parent.get().getQueries();
                    if (parentQueries.isPresent()) {
                        queries.get().addAll(parentQueries.get());
                    }
                }
            }
            if (!service.isPresent()) {
                service = parent.get().getService();
            }
            parent = parent.get().getParent();
        }
        if (!queries.isPresent()) {
            throw new IllegalStateException("at least one query must be specified");
        }
        if (!service.isPresent()) {
            throw new IllegalStateException("service must be specified");
        }
        return invokeQueries(queries, service);
    }

    private Stream<H> invokeQueries(
        Optional<Collection<T>> queries, Optional<QueryManagerService<T, H>> service
    ) {
        QueryManagerService<T, H> effectiveService = service.get();
        int length = queries.get().size();
        Collection<H> result = new ConcurrentLinkedQueue<>();
        Semaphore semaphore = new Semaphore(length);
        try {
            semaphore.acquire(length);
        } catch (InterruptedException e) {
            throw new IllegalStateException("exception occurred during pipeline running", e);
        }
        queries.get().forEach(q -> {
            effectiveService.submit(q, Optional.of((query, product) -> {
                if (product.isPresent()) {
                    result.add(product.get());
                }
                semaphore.release();
            }));
        });
        try {
            semaphore.acquire(length);
        } catch (InterruptedException e) {
            throw new IllegalStateException("exception occurred during pipeline running", e);
        }
        semaphore.release();
        return result.stream();
    }

    @Override
    public Optional<H> product() {
        return stream().findFirst();
    }

    @Override
    public List<H> products() {
        return stream().collect(toList());
    }

    @Override
    public Optional<Pipeline<T, H>> getParent() {
        return this.parent;
    }

    @Override
    public Optional<Collection<T>> getQueries() {
        return this.queries;
    }

    @Override
    public Optional<QueryManagerService<T, H>> getService() {
        return this.service;
    }
}
