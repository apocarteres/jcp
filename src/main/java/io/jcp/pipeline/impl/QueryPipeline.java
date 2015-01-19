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
    run(Function<H, T> mapper) {
        return new QueryPipeline<>(
            Optional.of(this),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(mapper)
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
    public Pipeline<T, H> on(QueryCompleteCallback<T, H> listener) {
        return new QueryPipeline<>(
            Optional.of(this),
            Optional.empty(),
            Optional.empty(),
            Optional.of(listener),
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
        if (queries.isEmpty()) {
            throw new IllegalStateException("at least one query must be specified");
        }
        return processQueries(queries, callbacks, mappers, service).stream();
//        while (parent.isPresent()) {
//            if (!queries.isPresent()) {
//                queries = parent.get().getQueries();
//            } else {
//                if (service.isPresent()) {
//                    Optional<Collection<T>> parentQueries = parent.get().getQueries();
//                    if (parentQueries.isPresent()) {
//                        queries.get().addAll(parentQueries.get());
//                    }
//                }
//            }
//            if (!service.isPresent()) {
//                service = parent.get().getService();
//            }
//            if (!listener.isPresent()) {
//                listener = parent.get().getCompleteCallback();
//            }
//            if (!mapper.isPresent()) {
//                mapper = parent.get().getProductMapper();
//            }
//            parent = parent.get().getParent();
//        }
//        if (!queries.isPresent()) {
//            throw new IllegalStateException("at least one query must be specified");
//        }
//        if (!service.isPresent()) {
//            throw new IllegalStateException("service must be specified");
//        }
//        return null;//invokeQueries(queries, service, listener, mapper);
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
                H product = p.get();
                if (mappers.isEmpty()) {
                    products.add(product);
                } else {
                    Function<T, H> download = q -> service.getExecutorService().exec(q);
                    Function<T, H> next = download;
                    for (Function<H, T> mapper : mappers) {
                        next = next.andThen(mapper).andThen(download);
                    }
                    product = next.apply(r);
                    products.add(product);
                }
            }
            callbacks.forEach(c -> c.onComplete(r, p));
            semaphore.release();
        });
        queries.forEach(q -> {
            service.submit(q, callback);
        });
        acquireLock(semaphore, length);
        semaphore.release(length);
        return products;
    }

//    private Stream<H> invokeQueries(
//        Optional<Collection<T>> queries, Optional<QueryManagerService<T, H>> service,
//        Optional<QueryCompleteCallback<T, H>> listener, Optional<ProductMapper<H, T>> mapper) {
//        QueryManagerService<T, H> effectiveService = service.get();
//        int factor = mapper.isPresent() ? 2 : 1;
//        int length = queries.get().size() * factor;
//        Collection<H> result = new ConcurrentLinkedQueue<>();
//        Semaphore semaphore = new Semaphore(length);
//        try {
//            semaphore.acquire(length);
//        } catch (InterruptedException e) {
//            throw new IllegalStateException("exception occurred during pipeline running", e);
//        }
//        queries.get().forEach(q -> {
//            effectiveService.submit(q, Optional.of((query, product) -> {
//                if (product.isPresent()) {
//                    if (mapper.isPresent()) {
//                        T mappedQuery = mapper.get().map(product.get());
//                        effectiveService.submit(mappedQuery, Optional.of((x, z) -> {
//                            result.add(z.get());
//                            semaphore.release();
//                        }));
//                    } else {
//                        result.add(product.get());
//                    }
//                }
//                if (listener.isPresent()) {
//                    listener.get().onComplete(query, product);
//                }
//                semaphore.release();
//            }));
//        });
//        try {
//            semaphore.acquire(length);
//        } catch (InterruptedException e) {
//            throw new IllegalStateException("exception occurred during pipeline running", e);
//        }
//        semaphore.release();
//        return result.stream();
//    }

    private static void acquireLock(Semaphore s, int length) {
        try {
            s.acquire(length);
        } catch (InterruptedException e) {
            throw new IllegalStateException("exception occurred during pipeline running", e);
        }
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

    @Override
    public Optional<QueryCompleteCallback<T, H>> getCompleteCallback() {
        return this.queryCompleteCallback;
    }

    @Override
    public Optional<Function<H, T>> getProductMapper() {
        return this.mapper;
    }

}
