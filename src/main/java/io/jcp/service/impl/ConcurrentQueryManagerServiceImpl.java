package io.jcp.service.impl;

import io.jcp.bean.Callback;
import io.jcp.bean.Product;
import io.jcp.bean.Query;
import io.jcp.listener.TaskLifecycleListener;
import io.jcp.service.QueryExecutorService;
import io.jcp.service.QueryManagerService;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class ConcurrentQueryManagerServiceImpl<T extends Query, H extends Product<T>>
    implements QueryManagerService<T, H> {

    private final ThreadPoolExecutor threadPool;
    private final Collection<TaskLifecycleListener<T>> taskLifecycleListeners;
    private final QueryExecutorService<T, H> executorService;
    private final AtomicLong submittedTask;
    private final AtomicLong inProgressTask;
    private final AtomicBoolean shuttingDown;

    public ConcurrentQueryManagerServiceImpl(
        ThreadPoolExecutor threadPool, Collection<TaskLifecycleListener<T>> taskLifecycleListeners,
        QueryExecutorService<T, H> executorService
    ) {
        this.threadPool = threadPool;
        this.taskLifecycleListeners = taskLifecycleListeners;
        this.executorService = executorService;
        this.submittedTask = new AtomicLong();
        this.inProgressTask = new AtomicLong();
        this.shuttingDown = new AtomicBoolean(false);
    }

    @Override
    public void submit(T query, Optional<Callback<H>> callback) {
        if (this.shuttingDown.get()) {
            throw new IllegalStateException("service is in shutdown state. submissions are blocked");
        }
        this.threadPool.submit(() -> {
            this.inProgressTask.incrementAndGet();
            try {
                executorService.exec(query, callback);
                taskLifecycleListeners.forEach(l -> l.onExec(query));
            } finally {
                this.inProgressTask.decrementAndGet();
                if (this.inProgressTask.get() == 0 && this.shuttingDown.get()) {
                    synchronized (this.shuttingDown) {
                        this.shuttingDown.notifyAll();
                    }
                }
            }
        });
        taskLifecycleListeners.forEach(l -> l.onSubmit(query));
        this.submittedTask.incrementAndGet();
    }

    @Override
    public H exec(T task) throws InterruptedException {
        Semaphore semaphore = new Semaphore(1);
        Set<H> result = new HashSet<>();
        Callback<H> callback = t -> {
            result.add(t);
            semaphore.release();
        };
        semaphore.acquire();
        submit(task, Optional.of(callback));
        semaphore.acquire();
        semaphore.release();
        return result.iterator().next();
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
    public void shutdown() throws InterruptedException {
        if (this.shuttingDown.get()) {
            throw new IllegalStateException("already is in shutdown state");
        }
        this.shuttingDown.set(true);
        if (countInProgress() + countSubmitted() == 0) {
            return;
        }
        synchronized (this.shuttingDown) {
            this.shuttingDown.wait();
        }
    }

}
