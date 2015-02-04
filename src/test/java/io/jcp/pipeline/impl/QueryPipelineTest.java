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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class QueryPipelineTest {

    private MockTextQuery textQuery;
    private List<MockTextQuery> threeTextQueries;
    private List<MockTextProduct> threeTextProducts;

    @Before
    public void setUp() throws Exception {
        this.textQuery = new MockTextQuery("ping");
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
            ).collect(toList());
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
            .on((q, p) -> result.add(q.getRequest() + p.get().getResponse()))
            .run(new MockTextQuery("ping"))
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

    @Test(timeout = 30000)
    public void testThatCallbackAfterQueryIsDoneWillBeInvokedWithEmptyProduct() throws Exception {
        MockTextQuery query = new MockTextQuery("ping");
        Provider<MockTextQuery, MockTextProduct> provider = mock(Provider.class);
        when(provider.fetch(query)).thenReturn(Optional.empty());
        AtomicBoolean result = new AtomicBoolean(true);
        new QueryPipeline<>()
            .using(QueryPipelineTest.textService(1, provider))
            .on((q, p) -> result.set(p.isPresent()))
            .run(query)
            .product();
        assertFalse("product must be empty", result.get());
    }

    @Test(timeout = 30000, expected = IllegalStateException.class)
    public void testThatCanNotRunWithoutQuery() throws Exception {
        new QueryPipeline<>().product();
        throw new RuntimeException(
            "at least one query must be specified"
        );
    }

    @Test(expected = IllegalStateException.class)
    public void testThatCanNotRunWithoutService() throws Exception {
        new QueryPipeline<>()
            .run(new MockTextQuery())
            .product().get();
        throw new RuntimeException(
            "at least one service must be specified"
        );
    }

    @Test(expected = IllegalStateException.class)
    public void testThatCanNotInitWithTwoOreMoreGlobalServices() throws Exception {
        QueryExecutorService<MockTextQuery, MockTextProduct> service = textService();
        new QueryPipeline<>()
            .using(service)
            .using(service)
            .run(new MockTextQuery())
            .product().get();
        throw new RuntimeException(
            "service may be specified only once and must be first directive in the pipeline"
        );
    }

    @Test(expected = IllegalStateException.class)
    public void testThatUsingDirectiveMustBeAtFirstPlaceOnly() throws Exception {
        QueryExecutorService<MockTextQuery, MockTextProduct> service = textService();
        new QueryPipeline<>()
            .run(new MockTextQuery())
            .using(service)
            .product().get();
        throw new RuntimeException(
            "service may be specified only once and must be first directive in the pipeline"
        );
    }

    @Test
    public void testThatQueryViaUnderlyingPipelineWorksWell() throws Exception {
        MockTextQuery query = new MockTextQuery("ping");
        MockTextProduct product = new QueryPipeline<>()
            .using(QueryPipelineTest.textService())
            .run(query)
            .run(p -> new MockIntQuery(p.getResponse().length()), new QueryPipeline<>().using(intService()))
            .run(q -> new MockTextQuery("ok_" + q.getResponse()), new QueryPipeline<>().using(textService()))
            .product()
            .get();
        assertEquals(
            new MockTextProduct("ok_9000_pong", Optional.of(new MockTextQuery("ok_9000"))), product
        );
    }

    @Test
    public void testThatCallbackOnUnderlyingPipelineWorksWell() throws Exception {
        MockTextQuery query = new MockTextQuery("ping");
        AtomicReference<String> response = new AtomicReference<>();
        new QueryPipeline<>()
            .using(QueryPipelineTest.textService())
            .run(query)
            .run(p -> new MockIntQuery(p.getResponse().length()),
                new QueryPipeline<>().using(intService())
                    .on((y, u) -> response.set("done_" + u.get().getResponse())))
            .run(q -> new MockTextQuery("ok_" + q.getResponse()), new QueryPipeline<>().using(textService()))
            .product()
            .get();
        assertEquals(
            "done_9000", response.get()
        );
    }

    @Test(timeout = 30000)
    public void testThatProductWillBeReturnedForLastExecutedQueryOnly() throws Exception {
        Iterator<MockTextQuery> iterator = threeTextQueries.iterator();
        MockTextProduct actual = new QueryPipeline<>()
            .using(QueryPipelineTest.textService(3, new MockTextProvider()))
            .run(iterator.next())
            .run(iterator.next())
            .run(iterator.next())
            .product().get();
        assertEquals(
            threeTextProducts.get(2), actual
        );
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
        ;
        return new ManagedQueryExecutorService<>(
            threadPoolExecutor, Collections.emptyList(), provider
        );
    }


}
