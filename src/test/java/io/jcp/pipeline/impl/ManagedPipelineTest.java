/**
 * Copyright (c) 2015, Alexander Paderin
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met: 1) Redistributions of source code must retain the above
 * copyright notice, this list of conditions and the following
 * disclaimer. 2) Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following
 * disclaimer in the documentation and/or other materials provided
 * with the distribution. 3) Neither the name of the author nor
 * the names of its contributors may be used to endorse or promote
 * products derived from this software without specific prior written
 * permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

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
                .callback((q, p) -> System.out.println("called")).getClass()
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

    @Test(expected = IllegalStateException.class)
    public void testThatCanNotGetProductsFromEmptyPipeline() throws Exception {
        pipeline().products();
        throw new RuntimeException(
            "can't run empty pipeline"
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

    @Test
    public void testThatCallbacksWorkWellOnUnderlyingPipelines() throws Exception {
        Set<String> actual = Collections.synchronizedSet(new HashSet<>());
        pipeline(3)
            .callback((x, y) -> actual.add(x.getRequest()))
            .run(threeTextQueries)
            .run(p -> new MockIntQuery(pingIndex(p.getResponse())),
                new QueryPipeline<>()
                    .using(intService())
                    .callback((x, y) -> actual.add(String.valueOf(x.getValue())))
            )
            .run(q -> new MockTextQuery("ok_" + q.getResponse()),
                new QueryPipeline<>()
                    .using(textService())
                    .callback((x, y) -> actual.add(x.getRequest()))
            )
            .products()
            .stream()
            .map(Optional::get)
            .collect(toSet());
        Set<String> expected = new HashSet<String>() {{
            add("1");
            add("2");
            add("3");
            add("ok_1000");
            add("ok_2000");
            add("ok_3000");
            add("ping1");
            add("ping2");
            add("ping3");
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
