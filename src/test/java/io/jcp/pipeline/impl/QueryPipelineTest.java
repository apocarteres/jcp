package io.jcp.pipeline.impl;

import io.jcp.bean.MockIntProduct;
import io.jcp.bean.MockIntQuery;
import io.jcp.bean.MockTextProduct;
import io.jcp.bean.MockTextQuery;
import io.jcp.listener.MockQueryLifecycleListener;
import io.jcp.pipeline.Pipeline;
import io.jcp.provider.MockIntProvider;
import io.jcp.provider.MockTextProvider;
import io.jcp.provider.Provider;
import io.jcp.service.QueryExecutorService;
import io.jcp.service.impl.ConcurrentQueryExecutorService;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class QueryPipelineTest {

    private MockTextQuery textQuery;

    @Before
    public void setUp() throws Exception {
        this.textQuery = new MockTextQuery("ping");
    }

    @Test(timeout = 30000)
    public void testThatOneProductWillBeCollectedWell() throws Exception {
        MockTextProduct collect = new QueryPipeline<>()
            .using(QueryPipelineTest.textService())
            .run(textQuery)
            .product().get();
        assertEquals("ping_pong", collect.getResponse());
    }

    @Test(timeout = 30000)
    public void testThatCallbackAfterQueryIsDoneWillBeInvokedWithProduct() throws Exception {
        Set<String> result = new HashSet<>();
        new QueryPipeline<>()
            .using(QueryPipelineTest.textService())
            .run(new MockTextQuery("ping"))
            .on((q, p) -> result.add(q.getRequest() + p.get().getResponse()))
            .product();
        assertEquals("pingping_pong", result.iterator().next());
    }

    @Test(timeout = 30000)
    public void testThatMapWorksCorrect() throws Exception {
        Optional<MockTextProduct> product = new QueryPipeline<>()
            .using(QueryPipelineTest.textService())
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
        List<MockTextProduct> products = new QueryPipeline<>()
            .using(QueryPipelineTest.textService(10, new MockTextProvider()))
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
        List<MockTextProduct> products = new QueryPipeline<>()
            .using(QueryPipelineTest.textService())
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
        List<MockTextProduct> products = new QueryPipeline<>()
            .using(QueryPipelineTest.textService(10, new MockTextProvider()))
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
        new QueryPipeline<>()
            .using(QueryPipelineTest.textService())
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
        new QueryPipeline<>()
            .using(QueryPipelineTest.textService(1, provider))
            .run(query)
            .on((q, p) -> result.set(p.isPresent()))
            .product();
        assertFalse("product must be empty", result.get());
    }

    @Test(timeout = 30000, expected = IllegalStateException.class)
    public void testThatCanNotRunWithoutQuery() throws Exception {
        new QueryPipeline<>().product();
        throw new RuntimeException("at least one query must be specified");
    }

    @Test(expected = IllegalStateException.class)
    public void testThatCanNotRunWithoutService() throws Exception {
        new QueryPipeline<>()
            .run(new MockTextQuery())
            .product().get();
        throw new RuntimeException("at least one service must be specified");
    }

    @Test(expected = IllegalStateException.class)
    public void testThatCanNotInitWithTwoOreMoreGlobalServices() throws Exception {
        QueryExecutorService<MockTextQuery, MockTextProduct> service = textService();
        new QueryPipeline<>()
            .using(service)
            .using(service)
            .run(new MockTextQuery())
            .product().get();
        throw new RuntimeException("gloal service may be specified only once");
    }

    @Test(expected = IllegalStateException.class)
    public void testThatCanNotInitWithTwoOreMoreServices() throws Exception {
        QueryExecutorService<MockTextQuery, MockTextProduct> service = textService();
        new QueryPipeline<>()
            .run(new MockTextQuery())
            .using(service)
            .using(service)
            .product().get();
        throw new RuntimeException("service may be specified only once");
    }

    @Test(expected = IllegalStateException.class)
    public void testThatCanNotStreamFromEmptyPipeline() throws Exception {
        new QueryPipeline<>().stream();
        throw new RuntimeException("can't run empty pipeline");
    }

    @Test(expected = IllegalStateException.class)
    public void testThatCanNotGetProductFromEmptyPipeline() throws Exception {
        new QueryPipeline<>().product();
        throw new RuntimeException("can't run empty pipeline");
    }

    @Test(expected = IllegalStateException.class)
    public void testThatCanNotGetProductsFromEmptyPipeline() throws Exception {
        new QueryPipeline<>().products();
        throw new RuntimeException("can't run empty pipeline");
    }

    @Test
    public void testThatQueryViaUnderlyingPipelineWorksWell() throws Exception {
        Pipeline<MockIntQuery, MockIntProduct> intPipeline = new QueryPipeline<>().using(intService());
        Pipeline<MockTextQuery, MockTextProduct> textPipeline = new QueryPipeline<>().using(textService());
        MockTextQuery query = new MockTextQuery("ping");
        MockTextProduct product = new QueryPipeline<>()
            .using(QueryPipelineTest.textService())
            .run(query)
            .run(p -> new MockIntQuery(p.getResponse().length()), intPipeline)
            .run(q -> new MockTextQuery("ok_" + q.getResponse()), textPipeline)
            .product()
            .get();
        assertEquals(new MockTextProduct("ok_9000_pong", Optional.of(query)), product);
    }

    @Test
    public void testThatUsingServiceForEachRun() throws Exception {
        MockTextQuery query = new MockTextQuery("ping");
        List<MockTextProduct> actual = new QueryPipeline<>()
            .run(query)
            .using(textService())
            .run(query)
            .using(textService(1, new MockTextProvider("_kong")))
            .products();
        List<MockTextProduct> expected = new ArrayList<>();
        expected.add(new MockTextProduct("ping_pong", Optional.of(query)));
        expected.add(new MockTextProduct("ping_kong", Optional.of(query)));
        assertEquals(expected, actual);
    }

    @Test
    public void testThatEachRunWillBeMapped() throws Exception {
        MockTextQuery q1 = new MockTextQuery("ping");
        MockTextQuery q2 = new MockTextQuery("king");
        List<MockTextProduct> actual = new QueryPipeline<>()
            .run(q1)
            .using(textService())
            .run(i -> new MockTextQuery(i.getResponse() + "_map"))
            .run(q2)
            .run(i -> new MockTextQuery(i.getResponse() + "_ape"))
            .using(textService(1, new MockTextProvider("_kong")))
            .products();
        List<MockTextProduct> expected = new ArrayList<>();
        expected.add(new MockTextProduct("ping_pong_map_pong", Optional.of(new MockTextQuery("ping_pong_map"))));
        expected.add(new MockTextProduct("king_kong_ape_kong", Optional.of(new MockTextQuery("king_kong_ape"))));
        assertEquals(expected, actual);
    }

    @Test
    public void testThatEachRunCollectionWillBeMapped() throws Exception {
        List<MockTextQuery> q1 = new ArrayList<MockTextQuery>() {{
            add(new MockTextQuery("pingA"));
            add(new MockTextQuery("pingB"));
        }};
        List<MockTextQuery> q2 = new ArrayList<MockTextQuery>() {{
            add(new MockTextQuery("kingA"));
            add(new MockTextQuery("kingB"));
        }};
        List<MockTextProduct> actual = new QueryPipeline<>()
            .using(textService())
            .run(q1)
            .run(i -> new MockTextQuery(i.getResponse() + "_map"))
            .run(q2)
            .run(i -> new MockTextQuery(i.getResponse() + "_ape"))
            .using(textService(1, new MockTextProvider("_kong")))
            .products();
        List<MockTextProduct> expected = new ArrayList<>();
        expected.add(new MockTextProduct("pingA_pong_map_pong", Optional.of(new MockTextQuery("pingA_pong_map"))));
        expected.add(new MockTextProduct("pingB_pong_map_pong", Optional.of(new MockTextQuery("pingB_pong_map"))));
        expected.add(new MockTextProduct("kingA_kong_ape_kong", Optional.of(new MockTextQuery("kingA_kong_ape"))));
        expected.add(new MockTextProduct("kingB_kong_ape_kong", Optional.of(new MockTextQuery("kingB_kong_ape"))));
        assertEquals(expected, actual);
    }

    @Test
    public void testThatSeveralRunsWillBeMapped() throws Exception {
        List<MockTextQuery> q1 = new ArrayList<MockTextQuery>() {{
            add(new MockTextQuery("pingA"));
            add(new MockTextQuery("pingB"));
        }};
        List<MockTextQuery> q2 = new ArrayList<MockTextQuery>() {{
            add(new MockTextQuery("kingA"));
            add(new MockTextQuery("kingB"));
        }};
        List<MockTextProduct> actual = new QueryPipeline<>()
            .using(textService())
            .run(q1.get(0))
            .run(q1.get(1))
            .run(i -> new MockTextQuery(i.getResponse() + "_map"))
            .run(q2.get(0))
            .run(q2.get(1))
            .run(i -> new MockTextQuery(i.getResponse() + "_ape"))
            .using(textService(1, new MockTextProvider("_kong")))
            .products();
        List<MockTextProduct> expected = new ArrayList<>();
        expected.add(new MockTextProduct("pingA_pong_map_pong", Optional.of(new MockTextQuery("pingA_pong_map"))));
        expected.add(new MockTextProduct("pingB_pong_map_pong", Optional.of(new MockTextQuery("pingB_pong_map"))));
        expected.add(new MockTextProduct("kingA_kong_ape_kong", Optional.of(new MockTextQuery("kingA_kong_ape"))));
        expected.add(new MockTextProduct("kingB_kong_ape_kong", Optional.of(new MockTextQuery("kingB_kong_ape"))));
        assertEquals(expected, actual);
    }

    @Test
    public void testThatGlobalyDefinedServiceWorksForAllRuns() throws Exception {
        MockTextQuery q1 = new MockTextQuery("ping");
        MockTextQuery q2 = new MockTextQuery("king");
        List<MockTextProduct> actual = new QueryPipeline<>()
            .using(QueryPipelineTest.textService())
            .run(q1)
            .run(q2)
            .products();
        List<MockTextProduct> expected = new ArrayList<>();
        expected.add(new MockTextProduct("ping_pong", Optional.of(q1)));
        expected.add(new MockTextProduct("king_pong", Optional.of(q2)));
        assertEquals(expected, actual);
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
        List<MockTextProduct> actual = new QueryPipeline<>()
            .using(QueryPipelineTest.textService(10, new MockTextProvider()))
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
        List<MockTextProduct> actual = new QueryPipeline<>()
            .using(QueryPipelineTest.textService(3, new MockTextProvider()))
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
        List<MockTextProduct> actual = new QueryPipeline<>()
            .using(QueryPipelineTest.textService())
            .run(queries)
            .run(new MockTextQuery("3"))
            .stream()
            .sorted((p1, p2) -> p2.getResponse().compareTo(p1.getResponse()))
            .collect(toList());
        assertEquals(expected, actual);
    }

    private static QueryExecutorService<MockTextQuery, MockTextProduct> textService() {
        return textService(1, new MockTextProvider());
    }

    private static QueryExecutorService<MockTextQuery, MockTextProduct> textService(
        int threads, Provider<MockTextQuery, MockTextProduct> provider) {
        ThreadPoolExecutor threadPoolExecutor =
            new ThreadPoolExecutor(threads, threads, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>());
        ;
        MockQueryLifecycleListener lifecycleListener = new MockQueryLifecycleListener();
        return new ConcurrentQueryExecutorService<>(
            threadPoolExecutor, Collections.singleton(lifecycleListener), provider
        );
    }

    private static QueryExecutorService<MockIntQuery, MockIntProduct> intService() {
        return intService(1, new MockIntProvider());
    }

    private static QueryExecutorService<MockIntQuery, MockIntProduct> intService(
        int threads, Provider<MockIntQuery, MockIntProduct> provider) {
        ThreadPoolExecutor threadPoolExecutor =
            new ThreadPoolExecutor(threads, threads, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>());
        ;
        return new ConcurrentQueryExecutorService<>(
            threadPoolExecutor, Collections.emptyList(), provider
        );
    }


}
