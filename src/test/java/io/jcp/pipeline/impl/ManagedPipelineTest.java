package io.jcp.pipeline.impl;

import io.jcp.bean.MockIntProduct;
import io.jcp.bean.MockIntQuery;
import io.jcp.bean.MockTextProduct;
import io.jcp.bean.MockTextQuery;
import io.jcp.listener.MockQueryLifecycleListener;
import io.jcp.provider.MockIntProvider;
import io.jcp.provider.MockTextProvider;
import io.jcp.provider.Provider;
import io.jcp.service.QueryExecutorService;
import io.jcp.service.impl.ManagedQueryExecutorService;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public final class ManagedPipelineTest {
    private MockTextQuery textQuery;
    private MockTextProduct textProduct;
    private List<MockTextQuery> threeTextQueries;
    private Set<MockTextProduct> threeTextProducts;

    @Before
    public void setUp() throws Exception {
        this.textQuery = new MockTextQuery("ping");
        this.textProduct = new MockTextProduct(
            "ping_pong", Optional.of(this.textQuery)
        );
        this.threeTextQueries = new ArrayList<MockTextQuery>() {{
            add(new MockTextQuery("ping1"));
            add(new MockTextQuery("ping2"));
            add(new MockTextQuery("ping3"));
        }};
        this.threeTextProducts = this.threeTextQueries.stream()
            .map(
                q -> new MockTextProduct(
                    String.format(
                        "%s%s", q.getRequest(),
                        MockTextProvider.DEFAULT_RESPONSE
                    ), Optional.of(q)
                )
            ).collect(toSet());
    }

    @Test
    public void testThatProductWillBeFetchedWell() throws Exception {
        assertEquals(textProduct, pipeline()
                .run(textQuery)
                .product()
                .get()
        );
    }

    @Test
    public void testThatProductsWillBeFetchedWell() throws Exception {
        assertEquals(threeTextProducts, pipeline(3)
                .run(threeTextQueries)
                .products()
                .stream()
                .map(Optional::get)
                .collect(toSet())
        );
    }

    @Test
    public void testThatQueryOverridesMapping() throws Exception {
        assertEquals(textProduct, pipeline()
                .run(new MockTextQuery("should be overridden"))
                .run(i -> new MockTextQuery(i.getResponse() + "_map"))
                .run(textQuery)
                .product()
                .get()
        );
    }

    @Test
    public void testThatManagedInstanceWillBeReturnedAfterRunQuery() throws Exception {
        assertEquals(ManagedPipeline.class, pipeline()
                .run(textQuery).getClass()
        );
    }

    @Test
    public void testThatManagedInstanceWillBeReturnedAfterRunCollection() throws Exception {
        assertEquals(ManagedPipeline.class, pipeline()
                .run(threeTextQueries).getClass()
        );
    }

    @Test
    public void testThatManagedInstanceWillBeReturnedAfterRunFunction() throws Exception {
        assertEquals(ManagedPipeline.class, pipeline()
                .run(threeTextQueries.get(0))
                .run(f -> textQuery).getClass()
        );
    }

    @Test
    public void testThatManagedInstanceWillBeReturnedAfterInvokeOn() throws Exception {
        assertEquals(ManagedPipeline.class, pipeline()
                .run(textQuery)
                .on((q, p) -> System.out.println("called")).getClass()
        );
    }

    @Test(expected = IllegalStateException.class)
    public void testThatCanNotUseUserSpecifiedService() throws Exception {
        //noinspection unchecked
        pipeline().using(mock(QueryExecutorService.class));
        throw new RuntimeException(
            "exception must be occurred since can't override service in ManagedPipeline"
        );
    }

    @Test(timeout = 30000, expected = IllegalStateException.class)
    public void testThatCanNotRunWithoutQuery() throws Exception {
        pipeline().product();
        throw new RuntimeException(
            "at least one query must be specified"
        );
    }

    @Test
    public void testThatQueryViaUnderlyingPipelineWorksWell() throws Exception {
        MockTextProduct product = pipeline()
            .run(textQuery)
            .run(p -> new MockIntQuery(p.getResponse().length()),
                new QueryPipeline<>().using(intService()))
            .run(q -> new MockTextQuery("ok_" + q.getResponse()),
                new QueryPipeline<>().using(textService()))
            .product()
            .get();
        assertEquals(
            new MockTextProduct(
                "ok_9000_pong", Optional.of(new MockTextQuery("ok_9000"))
            ), product
        );
    }

    @Test
    public void testThatQueriesViaUnderlyingPipelineWorksWell() throws Exception {
        Set<MockTextProduct> actual = pipeline(3)
            .run(threeTextQueries)
            .run(p -> new MockIntQuery(pingIndex(p.getResponse())),
                new QueryPipeline<>().using(intService()))
            .run(q -> new MockTextQuery("ok_" + q.getResponse()),
                new QueryPipeline<>().using(textService()))
            .products()
            .stream()
            .map(Optional::get)
            .collect(toSet());
        Set<MockTextProduct> expected = new HashSet<MockTextProduct>() {{
            add(new MockTextProduct("ok_1000_pong", Optional.of(new MockTextQuery("ok_1000"))));
            add(new MockTextProduct("ok_3000_pong", Optional.of(new MockTextQuery("ok_3000"))));
            add(new MockTextProduct("ok_2000_pong", Optional.of(new MockTextQuery("ok_2000"))));
        }};
        assertEquals(expected, actual);
    }

    @Test(timeout = 30000)
    public void testThatMoreThanOneProductWillBeCollectedWell() throws Exception {
        assertEquals(
            threeTextProducts,
            pipeline()
                .run(threeTextQueries)
                .products()
                .stream()
                .map(Optional::get)
                .collect(toSet())
        );
    }

    //    @Test(timeout = 5000)
//    public void testThatMappedQueryRunsConcurrently() throws Exception {
//        List<MockTextQuery> queries = IntStream.range(0, 10).mapToObj(
//            i -> new MockTextQuery("ping" + Integer.toString(i)))
//            .sorted((q1, q2) -> q2.getRequest().compareTo(q1.getRequest()))
//            .collect(toList());
//        List<MockTextProduct> expected = queries.stream()
//            .sorted((q1, q2) -> q2.getRequest().compareTo(q1.getRequest()))
//            .map(q -> new MockTextProduct(q.getRequest() + "_pong_map_pong", Optional.of(
//                new MockTextQuery(q.getRequest() + "_pong_map")
//            )))
//            .collect(toList());
//        List<MockTextProduct> products = new QueryPipeline<>()
//            .using(QueryPipelineTest.textService(10, new MockTextProvider()))
//            .run(queries)
//            .run(p -> new MockTextQuery(p.getResponse().concat("_map")))
//            .stream()
//            .sorted((p1, p2) -> p2.getResponse().compareTo(p1.getResponse()))
//            .collect(toList());
//        assertEquals(expected, products);
//    }
//
//    @Test(timeout = 30000)
//    public void testThatMappedQueryRunsSeqOnSingleThread() throws Exception {
//        List<MockTextQuery> queries = IntStream.range(0, 5).mapToObj(
//            i -> new MockTextQuery("ping" + Integer.toString(i)))
//            .sorted((q1, q2) -> q2.getRequest().compareTo(q1.getRequest()))
//            .collect(toList());
//        List<MockTextProduct> expected = queries.stream()
//            .sorted((q1, q2) -> q2.getRequest().compareTo(q1.getRequest()))
//            .map(q -> new MockTextProduct(q.getRequest() + "_pong_map_pong", Optional.of(
//                new MockTextQuery(q.getRequest() + "_pong_map")
//            )))
//            .collect(toList());
//        List<MockTextProduct> products = new QueryPipeline<>()
//            .using(QueryPipelineTest.textService())
//            .run(queries)
//            .run(p -> new MockTextQuery(p.getResponse().concat("_map")))
//            .stream()
//            .sorted((p1, p2) -> p2.getResponse().compareTo(p1.getResponse()))
//            .collect(toList());
//        assertEquals(expected, products);
//    }
//
//    @Test(timeout = 5000)
//    public void testThatMappedQueryCanBeMappedAgain() throws Exception {
//        List<MockTextQuery> queries = IntStream.range(0, 10).mapToObj(
//            i -> new MockTextQuery("ping" + Integer.toString(i)))
//            .sorted((q1, q2) -> q2.getRequest().compareTo(q1.getRequest()))
//            .collect(toList());
//        List<MockTextProduct> expected = queries.stream()
//            .sorted((q1, q2) -> q2.getRequest().compareTo(q1.getRequest()))
//            .map(q -> new MockTextProduct(q.getRequest() + "_pong_map_pong_remap_pong", Optional.of(
//                new MockTextQuery(q.getRequest() + "_pong_map_pong_remap")
//            )))
//            .collect(toList());
//        List<MockTextProduct> products = new QueryPipeline<>()
//            .using(QueryPipelineTest.textService(10, new MockTextProvider()))
//            .run(queries)
//            .run(p -> new MockTextQuery(p.getResponse().concat("_map")))
//            .run(p -> new MockTextQuery(p.getResponse().concat("_remap")))
//            .stream()
//            .sorted((p1, p2) -> p2.getResponse().compareTo(p1.getResponse()))
//            .collect(toList());
//        assertEquals(expected, products);
//    }
//
//    @Test(timeout = 30000)
//    public void testThatCompleteCallbackWillBeInvokedForQueryAndEachMapping() throws Exception {
//        List<MockTextQuery> queries = IntStream.range(0, 1).mapToObj(
//            i -> new MockTextQuery("ping" + Integer.toString(i)))
//            .sorted((q1, q2) -> q2.getRequest().compareTo(q1.getRequest()))
//            .collect(toList());
//        List<String> expected = new ArrayList<>();
//        queries.stream().forEach(q -> {
//            expected.add(q.getRequest() + q.getRequest() + "_pong");
//            expected.add(q.getRequest() + "_pong_map" + q.getRequest() + "_pong_map_pong");
//        });
//        List<String> actual = new ArrayList<>();
//        new QueryPipeline<>()
//            .using(QueryPipelineTest.textService())
//            .run(queries)
//            .run(p -> new MockTextQuery(p.getResponse().concat("_map")))
//            .on((q, p) -> actual.add(q.getRequest() + p.get().getResponse()))
//            .stream()
//            .sorted((p1, p2) -> p2.getResponse().compareTo(p1.getResponse()))
//            .collect(toList());
//        assertEquals(expected, actual);
//    }
    //    @Test(expected = IllegalStateException.class)
//    public void testThatCanNotStreamFromEmptyPipeline() throws Exception {
//        new QueryPipeline<>().stream();
//        throw new RuntimeException("can't run empty pipeline");
//    }
//
//    @Test(expected = IllegalStateException.class)
//    public void testThatCanNotGetProductFromEmptyPipeline() throws Exception {
//        new QueryPipeline<>().product();
//        throw new RuntimeException("can't run empty pipeline");
//    }
//
//    @Test(expected = IllegalStateException.class)
//    public void testThatCanNotGetProductsFromEmptyPipeline() throws Exception {
//        new QueryPipeline<>().products();
//        throw new RuntimeException("can't run empty pipeline");
//    }
    //
//    @Test
//    public void testThatUsingServiceForEachRun() throws Exception {
//        MockTextQuery query = new MockTextQuery("ping");
//        List<MockTextProduct> actual = new QueryPipeline<>()
//            .run(query)
//            .using(textService())
//            .run(query)
//            .using(textService(1, new MockTextProvider("_kong")))
//            .products();
//        List<MockTextProduct> expected = new ArrayList<>();
//        expected.add(new MockTextProduct("ping_pong", Optional.of(query)));
//        expected.add(new MockTextProduct("ping_kong", Optional.of(query)));
//        assertEquals(expected, actual);
//    }
//
//    @Test
//    public void testThatEachRunWillBeMapped() throws Exception {
//        MockTextQuery q1 = new MockTextQuery("ping");
//        MockTextQuery q2 = new MockTextQuery("king");
//        List<MockTextProduct> actual = new QueryPipeline<>()
//            .run(q1)
//            .using(textService())
//            .run(i -> new MockTextQuery(i.getResponse() + "_map"))
//            .run(q2)
//            .run(i -> new MockTextQuery(i.getResponse() + "_ape"))
//            .using(textService(1, new MockTextProvider("_kong")))
//            .products();
//        List<MockTextProduct> expected = new ArrayList<>();
//        expected.add(new MockTextProduct("ping_pong_map_pong", Optional.of(new MockTextQuery("ping_pong_map"))));
//        expected.add(new MockTextProduct("king_kong_ape_kong", Optional.of(new MockTextQuery("king_kong_ape"))));
//        assertEquals(expected, actual);
//    }

//    @Test
//    public void testThatEachRunCollectionWillBeMapped() throws Exception {
//        List<MockTextQuery> q1 = new ArrayList<MockTextQuery>() {{
//            add(new MockTextQuery("pingA"));
//            add(new MockTextQuery("pingB"));
//        }};
//        List<MockTextQuery> q2 = new ArrayList<MockTextQuery>() {{
//            add(new MockTextQuery("kingA"));
//            add(new MockTextQuery("kingB"));
//        }};
//        List<MockTextProduct> actual = new QueryPipeline<>()
//            .using(textService())
//            .run(q1)
//            .run(i -> new MockTextQuery(i.getResponse() + "_map"))
//            .run(q2)
//            .run(i -> new MockTextQuery(i.getResponse() + "_ape"))
//            .using(textService(1, new MockTextProvider("_kong")))
//            .products();
//        List<MockTextProduct> expected = new ArrayList<>();
//        expected.add(new MockTextProduct("pingA_pong_map_pong", Optional.of(new MockTextQuery("pingA_pong_map"))));
//        expected.add(new MockTextProduct("pingB_pong_map_pong", Optional.of(new MockTextQuery("pingB_pong_map"))));
//        expected.add(new MockTextProduct("kingA_kong_ape_kong", Optional.of(new MockTextQuery("kingA_kong_ape"))));
//        expected.add(new MockTextProduct("kingB_kong_ape_kong", Optional.of(new MockTextQuery("kingB_kong_ape"))));
//        assertEquals(expected, actual);
//    }
//
//    @Test
//    public void testThatSeveralRunsWillBeMapped() throws Exception {
//        List<MockTextQuery> q1 = new ArrayList<MockTextQuery>() {{
//            add(new MockTextQuery("pingA"));
//            add(new MockTextQuery("pingB"));
//        }};
//        List<MockTextQuery> q2 = new ArrayList<MockTextQuery>() {{
//            add(new MockTextQuery("kingA"));
//            add(new MockTextQuery("kingB"));
//        }};
//        List<MockTextProduct> actual = new QueryPipeline<>()
//            .using(textService())
//            .run(q1.get(0))
//            .run(q1.get(1))
//            .run(i -> new MockTextQuery(i.getResponse() + "_map"))
//            .run(q2.get(0))
//            .run(q2.get(1))
//            .run(i -> new MockTextQuery(i.getResponse() + "_ape"))
//            .using(textService(1, new MockTextProvider("_kong")))
//            .products();
//        List<MockTextProduct> expected = new ArrayList<>();
//        expected.add(new MockTextProduct("pingA_pong_map_pong", Optional.of(new MockTextQuery("pingA_pong_map"))));
//        expected.add(new MockTextProduct("pingB_pong_map_pong", Optional.of(new MockTextQuery("pingB_pong_map"))));
//        expected.add(new MockTextProduct("kingA_kong_ape_kong", Optional.of(new MockTextQuery("kingA_kong_ape"))));
//        expected.add(new MockTextProduct("kingB_kong_ape_kong", Optional.of(new MockTextQuery("kingB_kong_ape"))));
//        assertEquals(expected, actual);
//    }
//
//    @Test
//    public void testThatGlobalyDefinedServiceWorksForAllRuns() throws Exception {
//        MockTextQuery q1 = new MockTextQuery("ping");
//        MockTextQuery q2 = new MockTextQuery("king");
//        List<MockTextProduct> actual = new QueryPipeline<>()
//            .using(QueryPipelineTest.textService())
//            .run(q1)
//            .run(q2)
//            .products();
//        List<MockTextProduct> expected = new ArrayList<>();
//        expected.add(new MockTextProduct("ping_pong", Optional.of(q1)));
//        expected.add(new MockTextProduct("king_pong", Optional.of(q2)));
//        assertEquals(expected, actual);
//    }
//
//    @Test(timeout = 60000)
//    public void testQueryCollectionWillProceedWell() throws Exception {
//        List<MockTextQuery> queries = IntStream.range(0, 10).mapToObj(
//            i -> new MockTextQuery(Integer.toString(i)))
//            .sorted((q1, q2) -> q2.getRequest().compareTo(q1.getRequest()))
//            .collect(toList());
//        List<MockTextProduct> expected = queries.stream()
//            .map(q -> new MockTextProduct(q.getRequest() + "_pong", Optional.of(q)))
//            .collect(toList());
//        List<MockTextProduct> actual = new QueryPipeline<>()
//            .using(QueryPipelineTest.textService(10, new MockTextProvider()))
//            .run(queries)
//            .stream()
//            .sorted((p1, p2) -> p2.getResponse().compareTo(p1.getResponse()))
//            .collect(toList());
//        assertEquals(expected, actual);
//    }
    //
//    @Test(timeout = 30000)
//    public void testThatCollectionAndSeveralQueryWillProceedWell() throws Exception {
//        List<MockTextQuery> queries = IntStream.range(0, 3).mapToObj(
//            i -> new MockTextQuery(Integer.toString(i)))
//            .sorted((q1, q2) -> q2.getRequest().compareTo(q1.getRequest()))
//            .collect(toList());
//        List<MockTextQuery> e1 = new ArrayList<>(queries);
//        e1.add(new MockTextQuery(Integer.toString(3)));
//        List<MockTextProduct> expected = e1.stream()
//            .sorted((q1, q2) -> q2.getRequest().compareTo(q1.getRequest()))
//            .map(q -> new MockTextProduct(q.getRequest() + "_pong", Optional.of(q)))
//            .collect(toList());
//        List<MockTextProduct> actual = new QueryPipeline<>()
//            .using(QueryPipelineTest.textService())
//            .run(queries)
//            .run(new MockTextQuery("3"))
//            .stream()
//            .sorted((p1, p2) -> p2.getResponse().compareTo(p1.getResponse()))
//            .collect(toList());
//        assertEquals(expected, actual);
//    }

    private static QueryExecutorService<MockTextQuery, MockTextProduct> textService() {
        return textService(1, new MockTextProvider());
    }

    private static QueryExecutorService<MockTextQuery, MockTextProduct> textService(
        int threads, Provider<MockTextQuery, MockTextProduct> provider) {
        ThreadPoolExecutor threadPoolExecutor =
            new ThreadPoolExecutor(threads, threads, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>());
        MockQueryLifecycleListener lifecycleListener = new MockQueryLifecycleListener();
        return new ManagedQueryExecutorService<>(
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
        return new ManagedQueryExecutorService<>(
            threadPoolExecutor, Collections.emptyList(), provider
        );
    }

    private static int pingIndex(String response) {
        final int pingIndex = "ping".length();
        return Integer.parseInt(response.substring(pingIndex, pingIndex + 1));
    }


    private static ManagedPipeline<MockTextQuery, MockTextProduct> pipeline() {
        return pipeline(1);
    }

    private static ManagedPipeline<MockTextQuery, MockTextProduct> pipeline(int threads) {
        return new ManagedPipeline<>(threads, new MockTextProvider());
    }

}
