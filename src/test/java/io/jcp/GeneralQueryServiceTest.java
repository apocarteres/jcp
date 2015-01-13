package io.jcp;

import io.jcp.bean.Callback;
import io.jcp.bean.MockTextProduct;
import io.jcp.bean.MockTextQuery;
import io.jcp.executor.MockQueryExecutorService;
import io.jcp.listener.MockTaskLifecycleListener;
import io.jcp.service.impl.ConcurrentQueryManagerServiceImpl;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GeneralQueryServiceTest {

    @SuppressWarnings("FieldCanBeLocal")
    private ThreadPoolExecutor threadPool;
    private MockTaskLifecycleListener lifecycleListener;
    private ConcurrentQueryManagerServiceImpl<MockTextQuery, MockTextProduct> managerService;
    private MockQueryExecutorService executorService;

    @Before
    public void setUp() throws Exception {
        this.threadPool = new ThreadPoolExecutor(2, 2, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>());
        this.lifecycleListener = new MockTaskLifecycleListener();
        this.executorService = new MockQueryExecutorService();
        this.managerService = new ConcurrentQueryManagerServiceImpl<>(
            this.threadPool, Collections.singleton(this.lifecycleListener),
            this.executorService
        );
    }

    @Test
    public void testThatNoSubmittedRequestsByDefault() throws Exception {
        assertEquals(0, managerService.countSubmitted());
    }

    @Test
    public void testThatNoInProgressRequestsByDefault() throws Exception {
        assertEquals(0, managerService.countInProgress());
    }

    @Test
    public void testThatCountsExactlyOneSubmittedRequest() throws Exception {
        managerService.submit(new MockTextQuery(), Optional.empty());
        managerService.submit(new MockTextQuery(), Optional.empty());
        managerService.submit(new MockTextQuery(), Optional.empty());
        sleep(MockQueryExecutorService.DEFAULT_TASK_RUNNING / 2);
        assertEquals(1, managerService.countSubmitted());
    }

    @Test(timeout = 60000)
    public void testLifecycleListenersWillGetOnSubmitEvent() throws Exception {
        MockTextQuery query = new MockTextQuery();
        managerService.exec(query);
        assertEquals(1, this.lifecycleListener.requests(MockTaskLifecycleListener.Event.SUBMIT).count());
    }

    @Test(timeout = 60000)
    public void testLifecycleListenersWillGetOnConsumeEvent() throws Exception {
        MockTextQuery query = new MockTextQuery();
        managerService.exec(query);
        assertEquals(1, this.lifecycleListener.requests(MockTaskLifecycleListener.Event.EXEC).count());
    }

    @Test(timeout = 60000)
    public void testThatRequestWillBeExecuted() throws Exception {
        MockTextQuery query = new MockTextQuery();
        managerService.exec(query);
        assertEquals(1, this.executorService.tasks().count());
    }

    @Test(timeout = 60000)
    public void testThatNoTasksAfterEverythingWereExecuted() throws Exception {
        MockTextQuery query = new MockTextQuery();
        managerService.exec(query);
        managerService.shutdown();
        assertEquals(0, this.managerService.countInProgress());
    }

    @Test(timeout = 60000)
    public void testThatInProgressTaskCountWell() throws Exception {
        MockTextQuery query1 = new MockTextQuery();
        MockTextQuery query2 = new MockTextQuery();
        managerService.submit(query1, Optional.empty());
        managerService.submit(query2, Optional.empty());
        sleep(MockQueryExecutorService.DEFAULT_TASK_RUNNING / 2);
        assertEquals(2, this.managerService.countInProgress());
    }

    @Test(timeout = 60000)
    public void testThatShutdownMethodWaitsAllCurrentTask() throws Exception {
        MockTextQuery request1 = new MockTextQuery();
        MockTextQuery request2 = new MockTextQuery();
        managerService.submit(request1, Optional.empty());
        managerService.submit(request2, Optional.empty());
        sleep(MockQueryExecutorService.DEFAULT_TASK_RUNNING / 2);
        managerService.shutdown();
        assertEquals(0, this.managerService.countInProgress());
    }

    @Test(timeout = 60000, expected = IllegalStateException.class)
    public void testThatCanNotSubmitWhenInShutdownState() throws Exception {
        new Thread(() -> {
            try {
                sleep(1000);
                managerService.shutdown();
            } catch (InterruptedException ignored) {
            }
        }).start();
        for (int i = 0; i < 10; i++) {
            managerService.submit(new MockTextQuery(), Optional.empty());
            sleep(MockQueryExecutorService.DEFAULT_TASK_RUNNING / 2);
        }
        assertTrue("service is not allowed to accept submissions after shutdown", false);
    }

    @Test(timeout = 60000)
    public void testThatShutdownDoesNotBlockIfNoTasks() throws Exception {
        managerService.shutdown();
        assertTrue(true);
    }

    @Test(timeout = 60000, expected = IllegalStateException.class)
    public void testThatCanNotShutdownMoreThanOneTime() throws Exception {
        managerService.shutdown();
        managerService.shutdown();
        assertTrue(false);
    }

    @Test(timeout = 60000)
    public void testThatProductWillBeReturnedViaCallback() throws Exception {
        MockTextQuery task = new MockTextQuery("ping");
        Set<String> result = new HashSet<>();
        Callback<MockTextQuery, MockTextProduct> callback = (t, p) ->
            result.add(p.get().getResponse());
        managerService.submit(task, Optional.of(callback));
        managerService.shutdown();
        assertEquals("ping_pong", result.iterator().next());
    }

    @Test(timeout = 60000)
    public void testThatCallbackContainsCorrectQuery() throws Exception {
        MockTextQuery task = new MockTextQuery();
        Set<MockTextQuery> result = new HashSet<>();
        Callback<MockTextQuery, MockTextProduct> callback = (t, p) ->
            result.add(t);
        managerService.submit(task, Optional.of(callback));
        managerService.shutdown();
        assertTrue(task == result.iterator().next());
    }

    @Test(timeout = 60000)
    public void testThatExecReturnsProductWell() throws Exception {
        MockTextQuery task = new MockTextQuery("ping");
        MockTextProduct product = managerService.exec(task);
        assertEquals("ping_pong", product.getResponse());
    }

    @Test(timeout = 60000)
    public void testSubmitCounterDecrementsWhenTaskExecuted() throws Exception {
        managerService.exec(new MockTextQuery("ping"));
        assertEquals(0, managerService.countSubmitted());
    }

}
