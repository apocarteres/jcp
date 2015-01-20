package io.jcp.service.impl;

import io.jcp.bean.Callback;
import io.jcp.listener.QueryLifecycleListener;
import io.jcp.service.QueryExecutorService;
import io.jcp.service.QueryManagerService;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public final class ConcurrentQueryManagerServiceImpl<T, H>
    implements QueryManagerService<T, H> {

    private final ThreadPoolExecutor threadPool;
    private final Collection<QueryLifecycleListener<T>> queryLifecycleListeners;
    private final QueryExecutorService<T, H> executorService;
    private final AtomicLong submittedTask;
    private final AtomicLong inProgressTask;
    private final AtomicBoolean shuttingDown;

    public ConcurrentQueryManagerServiceImpl(
        ThreadPoolExecutor threadPool, Collection<QueryLifecycleListener<T>> queryLifecycleListeners,
        QueryExecutorService<T, H> executorService
    ) {
        this.threadPool = threadPool;
        this.queryLifecycleListeners = queryLifecycleListeners;
        this.executorService = executorService;
        this.submittedTask = new AtomicLong();
        this.inProgressTask = new AtomicLong();
        this.shuttingDown = new AtomicBoolean(false);
    }

    @Override
    public Future<Optional<H>> submit(T query, Optional<Callback<T, H>> callback) {
        Function<T, H> f = q -> {
            Semaphore s = new Semaphore(1);
            AtomicReference<Optional<H>> futureCb = new AtomicReference<>();
            acquire(s);
            executorService.exec(query, Optional.of((r, p) -> {
                futureCb.set(p);
                s.release();
            }));
            acquire(s);
            s.release();
            Optional<H> h = futureCb.get();
            return h.isPresent() ? h.get() : null;
        };
        return submit(query, f, callback);
    }

    @Override
    public Future<Optional<H>> submit(T query, Function<T, H> f, Optional<Callback<T, H>> callback) {
        if (this.shuttingDown.get()) {
            throw new IllegalStateException("service is in shutdown state. submissions are blocked");
        }
        Future<Optional<H>> submit = this.threadPool.submit(() -> {
            this.submittedTask.decrementAndGet();
            this.inProgressTask.incrementAndGet();
            Optional<H> product = Optional.empty();
            try {
                product = Optional.ofNullable(f.apply(query));
                if (callback.isPresent()) {
                    callback.get().call(query, product);
                }
                queryLifecycleListeners.forEach(l -> l.onExec(query));
            } finally {
                this.inProgressTask.decrementAndGet();
                if (this.inProgressTask.get() == 0 && this.shuttingDown.get()) {
                    synchronized (this.shuttingDown) {
                        this.shuttingDown.notifyAll();
                    }
                }
            }
            return product;
        });
        queryLifecycleListeners.forEach(l -> l.onSubmit(query));
        this.submittedTask.incrementAndGet();
        return submit;
    }

    @Override
    public Future<Optional<H>> submit(T query) {
        return submit(query, Optional.empty());
    }

    @Override
    public H exec(T task) {
        Semaphore semaphore = new Semaphore(1);
        AtomicReference<H> product = new AtomicReference<>();
        Callback<T, H> callback = (t, p) -> {
            if (p.isPresent()) {
                product.set(p.get());
            }
            semaphore.release();
        };
        acquire(semaphore);
        submit(task, Optional.of(callback));
        acquire(semaphore);
        semaphore.release();
        return product.get();
    }

    @Override
    public long countSubmitted() {
        return submittedTask.get();
    }

    @Override
    public long countInProgress() {
        return inProgressTask.get();
    }

    @Override
    public QueryExecutorService<T, H> getExecutorService() {
        return this.executorService;
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

    private static void acquire(Semaphore s) {
        try {
            s.acquire();
        } catch (InterruptedException e) {
            throw new IllegalStateException("can't acquire semaphore", e);
        }
    }
}
