package io.jcp.pipeline.impl;

import io.jcp.bean.MockTextProduct;
import io.jcp.bean.MockTextQuery;
import io.jcp.listener.MockQueryLifecycleListener;
import io.jcp.pipeline.Pipeline;
import io.jcp.provider.MockProvider;
import io.jcp.provider.Provider;
import io.jcp.service.QueryManagerService;
import io.jcp.service.impl.ConcurrentQueryManagerServiceImpl;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryPipelineTest {

    @Test(timeout = 30000)
    public void testThatOneProductWillBeCollectedWell() throws Exception {
        MockTextProduct collect = pipeline()
            .run(new MockTextQuery("ping"))
            .product().get();
        assertEquals("ping_pong", collect.getResponse());
    }

    @Test(timeout = 30000)
    public void testThatCallbackAfterQueryIsDoneWillBeInvokedWithProduct() throws Exception {
        Set<String> result = new HashSet<>();
        pipeline()
            .run(new MockTextQuery("ping"))
            .on((q, p) -> result.add(q.getRequest() + p.get().getResponse()))
            .product();
        assertEquals("pingping_pong", result.iterator().next());
    }

    @Test(timeout = 30000)
    public void testThatMapWorksCorrect() throws Exception {
        Optional<MockTextProduct> product = pipeline()
            .run(new MockTextQuery("ping"))
            .run(p -> new MockTextQuery(p.getResponse().concat("_map")))
            .product();
        assertEquals("ping_pong_map_pong", product.get().getResponse());
    }

    @Test(timeout = 5000)
    public void testThatMappedQueryRunsConcurrently() throws Exception {
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
        List<MockTextProduct> products = pipeline(10)
            .run(queries)
            .run(p -> new MockTextQuery(p.getResponse().concat("_map")))
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
        List<MockTextProduct> products = pipeline()
            .run(queries)
            .run(p -> new MockTextQuery(p.getResponse().concat("_map")))
            .stream()
            .sorted((p1, p2) -> p2.getResponse().compareTo(p1.getResponse()))
            .collect(toList());
        assertEquals(expected, products);
    }

    @Test(timeout = 5000)
    public void testThatMappedQueryCanBeMappedAgain() throws Exception {
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
        List<MockTextProduct> products = pipeline(10)
            .run(queries)
            .run(p -> new MockTextQuery(p.getResponse().concat("_map")))
            .run(p -> new MockTextQuery(p.getResponse().concat("_remap")))
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
        pipeline()
            .run(queries)
            .run(p -> new MockTextQuery(p.getResponse().concat("_map")))
            .on((q, p) -> actual.add(q.getRequest() + p.get().getResponse()))
            .stream()
            .sorted((p1, p2) -> p2.getResponse().compareTo(p1.getResponse()))
            .collect(toList());
        assertEquals(expected, actual);
    }

    @Test(timeout = 30000)
    public void testThatCallbackAfterQueryIsDoneWillBeInvokedWithEmptyProduct() throws Exception {
        MockTextQuery query = new MockTextQuery("ping");
        Provider<MockTextQuery, MockTextProduct> provider = mock(Provider.class);
        when(provider.fetch(query)).thenReturn(Optional.empty());
        AtomicBoolean result = new AtomicBoolean(true);
        pipeline(provider)
            .run(query)
            .on((q, p) -> result.set(p.isPresent()))
            .product();
        assertFalse("product must be empty", result.get());
    }

    @Test(timeout = 30000, expected = IllegalStateException.class)
    public void testThatCanNotRunWithoutQuery() throws Exception {
        pipeline()
            .product().get();
        assertTrue(false);
    }

    @Test(expected = IllegalStateException.class)
    public void testThatCanNotRunWithoutService() throws Exception {
        new QueryPipeline<>()
            .run(new MockTextQuery())
            .product().get();
        assertTrue(false);
    }

    @Test(expected = IllegalStateException.class)
    public void testThatCanNotUserTwoOreMoretServices() throws Exception {
        QueryManagerService<MockTextQuery, MockTextProduct> service = service();
        new QueryPipeline<MockTextQuery, MockTextProduct>()
            .using(service)
            .using(service)
            .run(new MockTextQuery())
            .product().get();
        assertTrue(false);
    }

    @Test(timeout = 60000)
    public void testQueryCollectionWillProceedWell() throws Exception {
        List<MockTextQuery> queries = IntStream.range(0, 10).mapToObj(
            i -> new MockTextQuery(Integer.toString(i)))
            .sorted((q1, q2) -> q2.getRequest().compareTo(q1.getRequest()))
            .collect(toList());
        List<MockTextProduct> expected = queries.stream()
            .map(q -> new MockTextProduct(q.getRequest() + "_pong", Optional.of(q)))
            .collect(toList());
        List<MockTextProduct> actual = pipeline(10)
            .run(queries)
            .stream()
            .sorted((p1, p2) -> p2.getResponse().compareTo(p1.getResponse()))
            .collect(toList());
        assertEquals(expected, actual);
    }

    @Test(timeout = 30000)
    public void testThatSeveralQueryWillProceedWell() throws Exception {
        List<MockTextProduct> expected = IntStream.range(0, 3)
            .mapToObj(i -> new MockTextQuery(Integer.toString(i)))
            .sorted((q1, q2) -> q2.getRequest().compareTo(q1.getRequest()))
            .map(q -> new MockTextProduct(q.getRequest() + "_pong", Optional.of(q)))
            .collect(toList());
        List<MockTextProduct> actual = pipeline(3)
            .run(new MockTextQuery("2"))
            .run(new MockTextQuery("0"))
            .run(new MockTextQuery("1"))
            .stream()
            .sorted((p1, p2) -> p2.getResponse().compareTo(p1.getResponse()))
            .collect(toList());
        assertEquals(expected, actual);
    }

    @Test(timeout = 30000)
    public void testThatCollectionAndSeveralQueryWillProceedWell() throws Exception {
        List<MockTextQuery> queries = IntStream.range(0, 3).mapToObj(
            i -> new MockTextQuery(Integer.toString(i)))
            .sorted((q1, q2) -> q2.getRequest().compareTo(q1.getRequest()))
            .collect(toList());
        List<MockTextQuery> e1 = new ArrayList<>(queries);
        e1.add(new MockTextQuery(Integer.toString(3)));
        List<MockTextProduct> expected = e1.stream()
            .sorted((q1, q2) -> q2.getRequest().compareTo(q1.getRequest()))
            .map(q -> new MockTextProduct(q.getRequest() + "_pong", Optional.of(q)))
            .collect(toList());
        List<MockTextProduct> actual = pipeline(3)
            .run(queries)
            .run(new MockTextQuery("3"))
            .stream()
            .sorted((p1, p2) -> p2.getResponse().compareTo(p1.getResponse()))
            .collect(toList());
        assertEquals(expected, actual);
    }

    private static Pipeline<MockTextQuery, MockTextProduct> pipeline() {
        return pipeline(1, new MockProvider());
    }

    private static Pipeline<MockTextQuery, MockTextProduct> pipeline(
        Provider<MockTextQuery, MockTextProduct> provider
    ) {
        return pipeline(1, provider);
    }

    private static Pipeline<MockTextQuery, MockTextProduct> pipeline(
        int threads
    ) {
        return pipeline(threads, new MockProvider());
    }

    private static Pipeline<MockTextQuery, MockTextProduct> pipeline(
        int threads,
        Provider<MockTextQuery, MockTextProduct> provider) {
        return new QueryPipeline<MockTextQuery, MockTextProduct>().using(service(threads, provider));
    }

    private static QueryManagerService<MockTextQuery, MockTextProduct> service(
        int threads, Provider<MockTextQuery, MockTextProduct> provider) {
        ThreadPoolExecutor threadPoolExecutor =
            new ThreadPoolExecutor(threads, threads, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>());
        ;
        MockQueryLifecycleListener lifecycleListener = new MockQueryLifecycleListener();
        return new ConcurrentQueryManagerServiceImpl<>(
            threadPoolExecutor, Collections.singleton(lifecycleListener), provider
        );
    }

    private static QueryManagerService<MockTextQuery, MockTextProduct> service() {
        return service(1, new MockProvider());
    }

}
