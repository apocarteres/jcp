package io.jcp.listener;

import io.jcp.bean.MockTextQuery;

import java.util.EnumMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Stream;

public class MockTaskLifecycleListener implements TaskLifecycleListener<MockTextQuery> {

    public enum Event {
        SUBMIT, EXEC
    }

    private final Map<Event, Queue<MockTextQuery>> tasks;

    public MockTaskLifecycleListener() {
        this.tasks = new EnumMap<>(Event.class);
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
        Queue<MockTextQuery> queue = this.tasks.getOrDefault(event, new LinkedList<>());
        queue.add(request);
        this.tasks.put(event, queue);
    }
}
