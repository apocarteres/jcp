package io.jcp.pipeline.impl;

import io.jcp.bean.MockIntProduct;
import io.jcp.bean.MockIntQuery;
import io.jcp.bean.MockTextProduct;
import io.jcp.bean.MockTextQuery;
import io.jcp.executor.MockQueryExecutorService;
import io.jcp.listener.MockTaskLifecycleListener;
import io.jcp.service.impl.ConcurrentQueryManagerServiceImpl;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("FieldCanBeLocal")
public class QueryPipelineTest {

    private QueryPipeline<MockTextQuery, MockTextProduct> pipeline;
    private ConcurrentQueryManagerServiceImpl<MockTextQuery, MockTextProduct> managerService;
    private ThreadPoolExecutor threadPoolExecutor;
    private MockTaskLifecycleListener lifecycleListener;
    private MockQueryExecutorService executorService;

    @Before
    public void setUp() throws Exception {
        this.pipeline = new QueryPipeline<>();
        this.threadPoolExecutor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>());
        this.lifecycleListener = new MockTaskLifecycleListener();
        this.executorService = new MockQueryExecutorService();
        this.managerService = new ConcurrentQueryManagerServiceImpl<>(
            this.threadPoolExecutor, Collections.singleton(this.lifecycleListener),
            this.executorService
        );
    }

    @Test
    public void testThatOneProductWillBeCollectedWell() throws Exception {
        MockTextProduct collect = this.pipeline
            .run(new MockTextQuery("ping"))
            .using(managerService)
            .product().get();
        assertEquals("ping_pong", collect.getResponse());
    }

    @Test
    @Ignore
    public void testMapTextProductToIntQueryAndCollectIntProduct() throws Exception {
        MockIntProduct collect = this.pipeline
            .run(new MockTextQuery("ping"))
            .using(managerService)
            .run(p -> new MockIntQuery(p.hashCode()), MockIntProduct.class)
            .product().get();
        assertEquals("ping_pong".length(), collect.getResponse());
    }

    @Test(expected = IllegalStateException.class)
    public void testThatCanNotRunWithoutQuery() throws Exception {
        this.pipeline
            .using(managerService)
            .product().get();
        assertTrue(false);
    }

    @Test(expected = IllegalStateException.class)
    public void testThatCanNotRunWithoutService() throws Exception {
        this.pipeline
            .run(new MockTextQuery())
            .product().get();
        assertTrue(false);
    }

    @Test(timeout = 300 + MockQueryExecutorService.DEFAULT_TASK_RUNNING)
    public void testQueryCollectionWillProceedWell() throws Exception {
        int numQueries = Runtime.getRuntime().availableProcessors();
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
            numQueries, numQueries, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>()
        );
        ConcurrentQueryManagerServiceImpl<MockTextQuery, MockTextProduct> service =
            new ConcurrentQueryManagerServiceImpl<>(
                pool, Collections.singleton(this.lifecycleListener),
                this.executorService
            );
        List<MockTextQuery> queries = IntStream.range(0, numQueries).mapToObj(
            i -> new MockTextQuery(Integer.toString(i)))
            .sorted((q1, q2) -> q2.getRequest().compareTo(q1.getRequest()))
            .collect(toList());
        List<MockTextProduct> expected = queries.stream()
            .map(q -> new MockTextProduct(q.getRequest() + "_pong", Optional.of(q)))
            .collect(toList());
        List<MockTextProduct> actual = this.pipeline
            .run(queries)
            .using(service)
            .stream()
            .sorted((p1, p2) -> p2.getResponse().compareTo(p1.getResponse()))
            .collect(toList());
        assertEquals(expected, actual);
    }

    @Test
    public void testThatSeveralQueryWillProceedWell() throws Exception {
        int numQueries = 3;
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
            numQueries, numQueries, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>()
        );
        ConcurrentQueryManagerServiceImpl<MockTextQuery, MockTextProduct> service =
            new ConcurrentQueryManagerServiceImpl<>(
                pool, Collections.singleton(this.lifecycleListener),
                this.executorService
            );
        List<MockTextProduct> expected = IntStream.range(0, numQueries).mapToObj(
            i -> new MockTextQuery(Integer.toString(i)))
            .sorted((q1, q2) -> q2.getRequest().compareTo(q1.getRequest()))
            .map(q -> new MockTextProduct(q.getRequest() + "_pong", Optional.of(q)))
            .collect(toList());
        List<MockTextProduct> actual = this.pipeline
            .run(new MockTextQuery("0"))
            .run(new MockTextQuery("1"))
            .run(new MockTextQuery("2"))
            .using(service)
            .stream()
            .sorted((p1, p2) -> p2.getResponse().compareTo(p1.getResponse()))
            .collect(toList());
        assertEquals(expected, actual);
    }


}
