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


public class MockQueryExecutorService implements QueryExecutorService<MockTextQuery, MockTextProduct> {

    public static final int TASK_RUNNING = 1000;
    private final Queue<MockTextQuery> tasks;

    public MockQueryExecutorService() {
        this.tasks = new LinkedList<>();
    }

    @Override
    public void exec(MockTextQuery task, Optional<Callback<MockTextQuery, MockTextProduct>> callback) {
        try {
            sleep(TASK_RUNNING);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.tasks.add(task);
        if (callback.isPresent()) {
            callback.get().call(task, Optional.of(new MockTextProduct("pong", Optional.of(task))));
        }
    }

    public Stream<MockTextQuery> tasks() {
        return this.tasks.stream();
    }
}
