package io.jcp.pipeline.impl;

import io.jcp.pipeline.Pipeline;
import io.jcp.pipeline.callback.QueryCompleteCallback;
import io.jcp.provider.Provider;
import io.jcp.service.ConcurrentQueryExecutorService;
import io.jcp.service.QueryExecutorService;
import io.jcp.service.impl.ManagedQueryExecutorService;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;

public final class ManagedPipeline<T, H> implements Pipeline<T, H> {
    private final ThreadPoolExecutor executor;
    private final ConcurrentQueryExecutorService<T, H> service;
    private final Pipeline<T, H> origin;
    private final List<Callable<Optional<H>>> products;

    public ManagedPipeline(int threads, Provider<T, H> provider) {
        this.executor = new ThreadPoolExecutor(
            threads, threads, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>()
        );
        this.service = new ManagedQueryExecutorService<>(
            executor, Collections.emptyList(), provider
        );
        this.origin = new QueryPipeline<T, H>().using(service);
        this.products = Collections.emptyList();
    }

    private ManagedPipeline(
        ThreadPoolExecutor executor,
        ConcurrentQueryExecutorService<T, H> service,
        Pipeline<T, H> origin,
        List<Callable<Optional<H>>> products
    ) {
        this.executor = executor;
        this.service = service;
        this.origin = origin;
        this.products = products;
    }

    @Override
    public <R, K> ManagedPipeline<R, K> using(QueryExecutorService<R, K> service) {
        throw new IllegalStateException(
            "user defined service can't be use with ManagedPipeline. Use QueryPipeline instead."
        );
    }

    @Override
    public ManagedPipeline<T, H> run(T query) {
        return run(Collections.singletonList(query));
    }

    @Override
    public ManagedPipeline<T, H> run(Function<H, T> f) {
        return new ManagedPipeline<>(
            executor,
            service,
            origin,
            products().stream().map(o -> (Callable<Optional<H>>) () ->
                this.origin.run(f).product())
                .collect(toList())
        );

    }

    public ManagedPipeline<T, H> run(Collection<T> query) {
        return wrap(origin, query.stream()
            .map(q -> (Callable<Optional<H>>) () -> this.origin.run(q).product())
            .collect(toList()));
    }

    @Override
    public <R, K> ManagedPipeline<R, K> run(
        Function<H, R> function, Pipeline<R, K> underlying
    ) {
        return new ManagedPipeline<>(
            executor,
            null,
            underlying,
            products().stream().map(o -> (Callable<Optional<K>>) () ->
                underlying.run(function.apply(o.get())).product())
                .collect(toList())
        );
    }

    @Override
    public ManagedPipeline<T, H> on(QueryCompleteCallback<T, H> callback) {
        return wrap(
            this.origin.on(callback)
        );
    }

    @Override
    public Optional<H> product() {
        return this.products().isEmpty() ?
            Optional.empty() : this.products().iterator().next();
    }

    public List<Optional<H>> products() {
        if (this.products.isEmpty()) {
            throw new IllegalStateException("at least one query must be specified");
        }
        List<Optional<H>> result = new ArrayList<>();
        try {
            executor.invokeAll(
                products
            ).forEach(f -> {
                try {
                    result.add(f.get());
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            });
        } catch (InterruptedException e) {
            throw new IllegalStateException("can't get products", e);
        }
        return result;
    }

    private ManagedPipeline<T, H> shutdown() {
        this.service.shutdown();
        this.executor.shutdown();
        return this;
    }

    private ManagedPipeline<T, H> wrap(
        Pipeline<T, H> origin, List<Callable<Optional<H>>> ps
    ) {
        return new ManagedPipeline<>(
            this.executor, this.service, origin, ps
        );
    }

    private ManagedPipeline<T, H> wrap(Pipeline<T, H> origin) {
        return wrap(origin, Collections.emptyList());
    }

}
