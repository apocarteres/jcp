package io.jcp.pipeline.impl;

import io.jcp.bean.Callback;
import io.jcp.pipeline.Pipeline;
import io.jcp.pipeline.callback.QueryCompleteCallback;
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
    private final Optional<QueryCompleteCallback<T, H>> queryCompleteCallback;
    private final Optional<QueryManagerService<T, H>> service;
    private final Optional<Function<H, T>> mapper;

    public QueryPipeline(
        Optional<Pipeline<T, H>> parent,
        Optional<Collection<T>> queries,
        Optional<QueryManagerService<T, H>> service,
        Optional<QueryCompleteCallback<T, H>> queryCompleteCallback,
        Optional<Function<H, T>> mapper
    ) {
        this.parent = parent;
        this.queries = queries;
        this.service = service;
        this.queryCompleteCallback = queryCompleteCallback;
        this.mapper = mapper;
    }

    public QueryPipeline() {
        this(
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    }

    @Override
    public Pipeline<T, H> run(T query) {
        return new QueryPipeline<>(
            Optional.of(this),
            Optional.of(new ArrayList<>(Collections.singletonList(query))),
            Optional.empty(),
            Optional.empty(),
            Optional.empty()
        );
    }

    @Override
    public Pipeline<T, H> run(Collection<T> query) {
        return new QueryPipeline<>(
            Optional.of(this),
            Optional.of(query),
            Optional.empty(),
            Optional.empty(),
            Optional.empty()
        );
    }

    @Override
    public Pipeline<T, H>
    run(Function<H, T> f) {
        return new QueryPipeline<>(
            Optional.of(this),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(f)
        );
    }

    @Override
    public Pipeline<T, H> using(QueryManagerService<T, H> service) {
        return new QueryPipeline<>(
            Optional.of(this),
            Optional.empty(),
            Optional.of(service),
            Optional.empty(),
            Optional.empty()
        );
    }

    @Override
    public Pipeline<T, H> on(QueryCompleteCallback<T, H> callback) {
        return new QueryPipeline<>(
            Optional.of(this),
            Optional.empty(),
            Optional.empty(),
            Optional.of(callback),
            Optional.empty()
        );
    }

    @Override
    public Stream<H> stream() {
        Deque<Pipeline<T, H>> pipelines = new LinkedList<>();
        for (Optional<Pipeline<T, H>> pipeline = Optional.of(this);
             pipeline.isPresent();
             pipeline = pipeline.get().getParent()) {
            pipelines.push(pipeline.get());
        }
        List<T> queries = pipelines.stream()
            .filter(p -> p.getQueries().isPresent())
            .map(p -> p.getQueries().get())
            .flatMap(Collection::stream)
            .collect(toList());
        List<QueryCompleteCallback<T, H>> callbacks = pipelines.stream()
            .filter(p -> p.getCompleteCallback().isPresent())
            .map(p -> p.getCompleteCallback().get())
            .collect(toList());
        List<Function<H, T>> mappers = pipelines.stream()
            .filter(p -> p.getProductMapper().isPresent())
            .map(p -> p.getProductMapper().get())
            .collect(toList());
        QueryManagerService<T, H> service = pipelines.stream()
            .filter(p -> p.getService().isPresent())
            .map(Pipeline::getService)
            .findFirst().orElseThrow(() ->
                new IllegalStateException("service must be specified"))
            .get();
        if (pipelines.stream()
            .filter(p -> p.getService().isPresent())
            .count() > 1) {
            throw new IllegalStateException("can't use two or more services");
        }
        if (queries.isEmpty()) {
            throw new IllegalStateException("at least one query must be specified");
        }
        return processQueries(queries, callbacks, mappers, service).stream();
    }

    private Queue<H> processQueries(
        List<T> queries,
        List<QueryCompleteCallback<T, H>> callbacks,
        List<Function<H, T>> mappers,
        QueryManagerService<T, H> service
    ) {
        int length = queries.size();
        Semaphore semaphore = new Semaphore(length);
        acquireLock(semaphore, length);
        Queue<H> products = new ConcurrentLinkedQueue<>();
        Optional<Callback<T, H>> callback = Optional.of((r, p) -> {
            if (p.isPresent()) {
                products.add(p.get());
            }
        });
        Function<T, H> download = q -> {
            Optional<H> exec = service.exec(q);
            callbacks.forEach(c -> c.onComplete(q, exec));
            return exec.isPresent() ? exec.get() : null;
        };
        Function<T, H> next = download;
        for (Function<H, T> mapper : mappers) {
            next = next.andThen(mapper).andThen(download);
        }
        Function<T, H> effectiveNext = next;
        //noinspection CodeBlock2Expr
        queries.forEach(q -> {
            service.submit(q, effectiveNext, Optional.of((query, product) -> {
                callback.get().call(q, product);
                semaphore.release();
            }));
        });
        acquireLock(semaphore, length);
        semaphore.release(length);
        return products;
    }

    private static void acquireLock(Semaphore s, int length) {
        try {
            s.acquire(length);
        } catch (InterruptedException e) {
            throw new IllegalStateException("exception occurred during pipeline running", e);
        }
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

    @Override
    public Optional<QueryCompleteCallback<T, H>> getCompleteCallback() {
        return this.queryCompleteCallback;
    }

    @Override
    public Optional<Function<H, T>> getProductMapper() {
        return this.mapper;
    }

}
