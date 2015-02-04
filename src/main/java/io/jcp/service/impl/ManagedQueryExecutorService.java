package io.jcp.service.impl;

import io.jcp.bean.ExecutionCallback;
import io.jcp.listener.QueryLifecycleListener;
import io.jcp.provider.Provider;
import io.jcp.service.ConcurrentQueryExecutorService;
import io.jcp.service.QueryExecutorService;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public final class ManagedQueryExecutorService<T, H>
    implements ConcurrentQueryExecutorService<T, H> {

    private final ThreadPoolExecutor threadPool;
    private final Collection<QueryLifecycleListener<T>> queryLifecycleListeners;
    private final Provider<T, H> provider;
    private final AtomicLong submittedQueries;
    private final AtomicLong inProgressQueries;
    private final AtomicBoolean shuttingDown;

    public ManagedQueryExecutorService(
        ThreadPoolExecutor threadPool,
        Collection<QueryLifecycleListener<T>> queryLifecycleListeners,
        Provider<T, H> provider
    ) {
        this.threadPool = threadPool;
        this.queryLifecycleListeners = queryLifecycleListeners;
        this.provider = provider;
        this.submittedQueries = new AtomicLong();
        this.inProgressQueries = new AtomicLong();
        this.shuttingDown = new AtomicBoolean(false);
    }

    @Override
    public Future<Optional<H>> submit(
        T query, Optional<ExecutionCallback<T, H>> callback
    ) {
        Function<T, H> f = q -> {
            Optional<H> h = exec(q);
            return h.isPresent() ? h.get() : null;
        };
        return submit(query, f, callback);
    }

    @Override
    public Future<Optional<H>> submit(
        T query, Function<T, H> f, Optional<ExecutionCallback<T, H>> callback
    ) {
        if (this.shuttingDown.get()) {
            throw new IllegalStateException(
                "service is in shutdown state. submissions are blocked"
            );
        }
        Future<Optional<H>> submit = this.threadPool.submit(() -> {
            this.submittedQueries.decrementAndGet();
            this.inProgressQueries.incrementAndGet();
            Optional<H> product = Optional.empty();
            try {
                product = Optional.ofNullable(f.apply(query));
                if (callback.isPresent()) {
                    callback.get().call(query, product);
                }
            } catch (Throwable t) {
                t.printStackTrace();
            } finally {
                this.inProgressQueries.decrementAndGet();
                if (this.inProgressQueries.get() == 0 && this.shuttingDown.get()) {
                    synchronized (this.shuttingDown) {
                        this.shuttingDown.notifyAll();
                    }
                }
            }
            return product;
        });
        queryLifecycleListeners.forEach(l -> l.onSubmit(query));
        this.submittedQueries.incrementAndGet();
        return submit;
    }

    @Override
    public Future<Optional<H>> submit(T query) {
        return submit(query, Optional.empty());
    }

    @Override
    public Optional<H> exec(T query) {
        Optional<H> fetch = this.provider.fetch(query);
        this.queryLifecycleListeners.forEach(l->l.onExec(query));
        return fetch;
    }

    @Override
    public long countSubmitted() {
        return submittedQueries.get();
    }

    @Override
    public long countInProgress() {
        return inProgressQueries.get();
    }

    @Override
    public void shutdown() {
        if (this.shuttingDown.get()) {
            throw new IllegalStateException("already is in shutdown state");
        }
        this.shuttingDown.set(true);
        if (countInProgress() + countSubmitted() == 0) {
            return;
        }
        synchronized (this.shuttingDown) {
            try {
                this.shuttingDown.wait();
            } catch (InterruptedException e) {
                throw new IllegalStateException("failed to wait", e);
            }
        }
    }

}
