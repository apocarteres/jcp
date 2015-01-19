package io.jcp.executor;

import io.jcp.bean.Callback;
import io.jcp.bean.MockTextProduct;
import io.jcp.bean.MockTextQuery;
import io.jcp.service.QueryExecutorService;

import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Stream;

import static java.lang.Thread.sleep;


public class MockQueryWithEmptyProductExecutorService
    implements QueryExecutorService<MockTextQuery, MockTextProduct> {

    public static final int DEFAULT_TASK_RUNNING = 1000;
    private final Queue<MockTextQuery> tasks;
    private final int taskRunning;

    public MockQueryWithEmptyProductExecutorService() {
        this(DEFAULT_TASK_RUNNING);
    }

    public MockQueryWithEmptyProductExecutorService(int taskRunning) {
        this.taskRunning = taskRunning;
        this.tasks = new LinkedList<>();
    }

    @Override
    public void exec(MockTextQuery task, Optional<Callback<MockTextQuery, MockTextProduct>> callback) {
        try {
            sleep(taskRunning);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.tasks.add(task);
        if (callback.isPresent()) {
            callback.get().call(
                task, Optional.empty()
            );
        }
    }

    @Override
    public Optional<MockTextProduct> exec(MockTextQuery task) {
        return Optional.empty();
    }

    @Override
    public void shutdown() {

    }

    public Stream<MockTextQuery> tasks() {
        return this.tasks.stream();
    }
}
