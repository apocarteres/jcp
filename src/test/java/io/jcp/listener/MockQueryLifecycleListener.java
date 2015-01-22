package io.jcp.listener;

import io.jcp.bean.MockTextQuery;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

public class MockQueryLifecycleListener implements QueryLifecycleListener<MockTextQuery> {

    public enum Event {
        SUBMIT, EXEC
    }

    private final Map<Event, Queue<MockTextQuery>> tasks;

    public MockQueryLifecycleListener() {
        this.tasks = Collections.synchronizedMap(new EnumMap<>(Event.class));
    }

    @Override
    public void onSubmit(MockTextQuery request) {
        put(Event.SUBMIT, request);
    }

    @Override
    public void onExec(MockTextQuery request) {
        put(Event.EXEC, request);
    }

    public Stream<MockTextQuery> requests(Event event) {
        return tasks.get(event).stream();
    }

    private void put(Event event, MockTextQuery request) {
        Queue<MockTextQuery> queue = this.tasks.getOrDefault(event, new ConcurrentLinkedQueue<>());
        queue.add(request);
        this.tasks.put(event, queue);
    }
}
