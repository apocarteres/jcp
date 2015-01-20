package io.jcp.executor;

import io.jcp.bean.Callback;
import io.jcp.service.QueryExecutorService;

import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Stream;

import static java.lang.Thread.sleep;


public abstract class MockQueryExecutorService<T, H> implements QueryExecutorService<T, H> {

    public static final int DEFAULT_TASK_RUNNING = 1000;
    private final Queue<T> tasks;
    private final int taskRunning;

    public MockQueryExecutorService() {
        this(DEFAULT_TASK_RUNNING);
    }

    public MockQueryExecutorService(int taskRunning) {
        this.taskRunning = taskRunning;
        this.tasks = new LinkedList<>();
    }

    @Override
    public void exec(T task, Optional<Callback<T, H>> callback) {
        this.tasks.add(task);
        if (callback.isPresent()) {
            callback.get().call(
                task, exec(task)
            );
        }
    }

    @Override
    public Optional<H> exec(T task) {
        try {
            sleep(taskRunning);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return Optional.of(build(task));
    }

    @Override
    public void shutdown() {

    }

    protected abstract H build(T query);

    public Stream<T> tasks() {
        return this.tasks.stream();
    }
}
