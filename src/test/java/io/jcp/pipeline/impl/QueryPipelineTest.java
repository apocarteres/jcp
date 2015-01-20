package io.jcp.pipeline.impl;

import io.jcp.bean.MockTextProduct;
import io.jcp.bean.MockTextQuery;
import io.jcp.executor.MockQueryExecutorService;
import io.jcp.executor.MockQueryWithEmptyProductExecutorService;
import io.jcp.listener.MockTaskLifecycleListener;
import io.jcp.pipeline.Pipeline;
import io.jcp.service.impl.ConcurrentQueryManagerServiceImpl;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.*;

@SuppressWarnings("FieldCanBeLocal")
public class QueryPipelineTest {

    private Pipeline<MockTextQuery, MockTextProduct> pipeline;
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
    public void testThatCallbackAfterQueryIsDoneWillBeInvokedWithProduct() throws Exception {
        Set<String> result = new HashSet<>();
        this.pipeline
            .run(new MockTextQuery("ping"))
            .on((q, p) -> result.add(q.getRequest() + p.get().getResponse()))
            .using(managerService)
            .product();
        assertEquals("pingping_pong", result.iterator().next());
    }

    @Test
    public void testThatMapWorksCorrect() throws Exception {
        Optional<MockTextProduct> product = this.pipeline
            .run(new MockTextQuery("ping"))
            .run(p -> new MockTextQuery(p.getResponse().concat("_map")))
            .using(managerService)
            .product();
        assertEquals("ping_pong_map_pong", product.get().getResponse());
    }

    @Test(timeout = 5000)
    public void testThatMappedQueryRunsConcurrently() throws Exception {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
            10, 10, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>()
        );
        ConcurrentQueryManagerServiceImpl<MockTextQuery, MockTextProduct> service =
            new ConcurrentQueryManagerServiceImpl<>(
                pool, Collections.singleton(this.lifecycleListener),
                this.executorService
            );
        List<MockTextQuery> queries = IntStream.range(0, 10).mapToObj(
            i -> new MockTextQuery("ping" + Integer.toString(i)))
            .sorted((q1, q2) -> q2.getRequest().compareTo(q1.getRequest()))
            .collect(toList());
        List<MockTextProduct> expected = queries.stream()
            .sorted((q1, q2) -> q2.getRequest().compareTo(q1.getRequest()))
            .map(q -> new MockTextProduct(q.getRequest() + "_pong_map_pong", Optional.of(
                new MockTextQuery(q.getRequest() + "_pong_map")
            )))
            .collect(toList());
        List<MockTextProduct> products = this.pipeline
            .run(queries)
            .run(p -> new MockTextQuery(p.getResponse().concat("_map")))
            .using(service)
            .stream()
            .sorted((p1, p2) -> p2.getResponse().compareTo(p1.getResponse()))
            .collect(toList());
        assertEquals(expected, products);
    }

    @Test(timeout = 30000)
    public void testThatMappedQueryRunsSeqOnSingleThread() throws Exception {
        List<MockTextQuery> queries = IntStream.range(0, 5).mapToObj(
            i -> new MockTextQuery("ping" + Integer.toString(i)))
            .sorted((q1, q2) -> q2.getRequest().compareTo(q1.getRequest()))
            .collect(toList());
        List<MockTextProduct> expected = queries.stream()
            .sorted((q1, q2) -> q2.getRequest().compareTo(q1.getRequest()))
            .map(q -> new MockTextProduct(q.getRequest() + "_pong_map_pong", Optional.of(
                new MockTextQuery(q.getRequest() + "_pong_map")
            )))
            .collect(toList());
        List<MockTextProduct> products = this.pipeline
            .run(queries)
            .run(p -> new MockTextQuery(p.getResponse().concat("_map")))
            .using(managerService)
            .stream()
            .sorted((p1, p2) -> p2.getResponse().compareTo(p1.getResponse()))
            .collect(toList());
        assertEquals(expected, products);
    }

    @Test(timeout = 5000)
    public void testThatMappedQueryCanBeMappedAgain() throws Exception {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
            10, 10, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>()
        );
        ConcurrentQueryManagerServiceImpl<MockTextQuery, MockTextProduct> service =
            new ConcurrentQueryManagerServiceImpl<>(
                pool, Collections.singleton(this.lifecycleListener),
                this.executorService
            );
        List<MockTextQuery> queries = IntStream.range(0, 10).mapToObj(
            i -> new MockTextQuery("ping" + Integer.toString(i)))
            .sorted((q1, q2) -> q2.getRequest().compareTo(q1.getRequest()))
            .collect(toList());
        List<MockTextProduct> expected = queries.stream()
            .sorted((q1, q2) -> q2.getRequest().compareTo(q1.getRequest()))
            .map(q -> new MockTextProduct(q.getRequest() + "_pong_map_pong_remap_pong", Optional.of(
                new MockTextQuery(q.getRequest() + "_pong_map_pong_remap")
            )))
            .collect(toList());
        List<MockTextProduct> products = this.pipeline
            .run(queries)
            .run(p -> new MockTextQuery(p.getResponse().concat("_map")))
            .run(p -> new MockTextQuery(p.getResponse().concat("_remap")))
            .using(service)
            .stream()
            .sorted((p1, p2) -> p2.getResponse().compareTo(p1.getResponse()))
            .collect(toList());
        assertEquals(expected, products);
    }

    @Test(timeout = 30000)
    public void testThatCompleteCallbackWillBeInvokedForQueryAndEachMapping() throws Exception {
        List<MockTextQuery> queries = IntStream.range(0, 1).mapToObj(
            i -> new MockTextQuery("ping" + Integer.toString(i)))
            .sorted((q1, q2) -> q2.getRequest().compareTo(q1.getRequest()))
            .collect(toList());
        List<String> expected = new ArrayList<>();
        queries.stream().forEach(q -> {
            expected.add(q.getRequest() + q.getRequest() + "_pong");
            expected.add(q.getRequest() + "_pong_map" + q.getRequest() + "_pong_map_pong");
        });
        List<String> actual = new ArrayList<>();
        this.pipeline
            .run(queries)
            .run(p -> new MockTextQuery(p.getResponse().concat("_map")))
            .on((q, p) -> actual.add(q.getRequest() + p.get().getResponse()))
            .using(managerService)
            .stream()
            .sorted((p1, p2) -> p2.getResponse().compareTo(p1.getResponse()))
            .collect(toList());
        assertEquals(expected, actual);
    }

    @Test
    public void testThatCallbackAfterQueryIsDoneWillBeInvokedWithEmptyProduct() throws Exception {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
            1, 1, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>()
        );
        MockQueryWithEmptyProductExecutorService executorService =
            new MockQueryWithEmptyProductExecutorService();
        ConcurrentQueryManagerServiceImpl<MockTextQuery, MockTextProduct> service =
            new ConcurrentQueryManagerServiceImpl<>(
                pool, Collections.singleton(this.lifecycleListener),
                executorService
            );
        AtomicBoolean result = new AtomicBoolean(true);
        this.pipeline
            .run(new MockTextQuery("ping"))
            .on((q, p) -> result.set(p.isPresent()))
            .using(service)
            .product();
        assertFalse("product must be empty", result.get());
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

    @Test(timeout = 60000)
    public void testQueryCollectionWillProceedWell() throws Exception {
        int numQueries = 10;
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
        List<MockTextProduct> expected = IntStream.range(0, numQueries)
            .mapToObj(i -> new MockTextQuery(Integer.toString(i)))
            .sorted((q1, q2) -> q2.getRequest().compareTo(q1.getRequest()))
            .map(q -> new MockTextProduct(q.getRequest() + "_pong", Optional.of(q)))
            .collect(toList());
        List<MockTextProduct> actual = this.pipeline
            .run(new MockTextQuery("2"))
            .run(new MockTextQuery("0"))
            .run(new MockTextQuery("1"))
            .using(service)
            .stream()
            .sorted((p1, p2) -> p2.getResponse().compareTo(p1.getResponse()))
            .collect(toList());
        assertEquals(expected, actual);
    }

    @Test
    public void testThatCollectionAndSeveralQueryWillProceedWell() throws Exception {
        int numQueries = 3;
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
        List<MockTextQuery> e1 = new ArrayList<>(queries);
        e1.add(new MockTextQuery(Integer.toString(numQueries)));
        List<MockTextProduct> expected = e1.stream()
            .sorted((q1, q2) -> q2.getRequest().compareTo(q1.getRequest()))
            .map(q -> new MockTextProduct(q.getRequest() + "_pong", Optional.of(q)))
            .collect(toList());
        List<MockTextProduct> actual = this.pipeline
            .run(queries)
            .run(new MockTextQuery("3"))
            .using(service)
            .stream()
            .sorted((p1, p2) -> p2.getResponse().compareTo(p1.getResponse()))
            .collect(toList());
        assertEquals(expected, actual);
    }


}
