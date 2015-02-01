package io.jcp.pipeline.impl;

import io.jcp.pipeline.Pipeline;
import io.jcp.pipeline.callback.QueryCompleteCallback;
import io.jcp.provider.Provider;
import io.jcp.service.QueryExecutorService;
import io.jcp.service.impl.ConcurrentQueryExecutorService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

public final class ManagedPipeline<T, H> implements Pipeline<T, H> {
    private final ThreadPoolExecutor executor;
    private final QueryExecutorService<T, H> service;
    private final Pipeline<T, H> delegate;

    public ManagedPipeline(int threads, Provider<T, H> provider) {
        this.executor = new ThreadPoolExecutor(
            1, threads, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>()
        );
        this.service = new ConcurrentQueryExecutorService<>(
            executor, Collections.emptyList(), provider
        );
        this.delegate = new QueryPipeline<T, H>().using(service);
    }

    private ManagedPipeline(
        ThreadPoolExecutor executor,
        QueryExecutorService<T, H> service,
        Pipeline<T, H> delegate
    ) {
        this.executor = executor;
        this.service = service;
        this.delegate = delegate;
    }

    @Override
    public Pipeline<T, H> run(T query) {
        return wrap(this.delegate.run(query));
    }

    @Override
    public Pipeline<T, H> run(Collection<T> query) {
        return wrap(this.delegate.run(query));
    }

    @Override
    public Pipeline<T, H> run(Function<H, T> f) {
        return wrap(this.delegate.run(f));
    }

    @Override
    public <R, K> Pipeline<R, K> run(Function<H, R> function, Pipeline<R, K> underlying) {
        throw new IllegalStateException("user defined service can't be use by managed pipeline");
    }

    @Override
    public <R, K> Pipeline<R, K> using(QueryExecutorService<R, K> service) {
        throw new IllegalStateException("user defined service can't be use by managed pipeline");
    }

    @Override
    public Pipeline<T, H> on(QueryCompleteCallback<T, H> callback) {
        return wrap(this.delegate.on(callback));
    }

    @Override
    public Stream<H> stream() {
        Stream<H> stream = this.delegate.stream();
        shutdown();
        return stream;
    }

    @Override
    public Optional<H> product() {
        Optional<H> product = this.delegate.product();
        shutdown();
        return product;
    }

    @Override
    public List<H> products() {
        List<H> products = this.delegate.products();
        shutdown();
        return products;
    }

    private void shutdown() {
        this.service.shutdown();
        this.executor.shutdown();
    }

    private Pipeline<T, H> wrap(Pipeline<T, H> origin) {
        return new ManagedPipeline<>(executor, service, origin);
    }

}
