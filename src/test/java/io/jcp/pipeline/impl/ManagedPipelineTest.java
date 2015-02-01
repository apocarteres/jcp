package io.jcp.pipeline.impl;

import io.jcp.bean.MockTextProduct;
import io.jcp.bean.MockTextQuery;
import io.jcp.pipeline.Pipeline;
import io.jcp.provider.MockTextProvider;
import io.jcp.service.QueryExecutorService;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class ManagedPipelineTest {

    private Pipeline<MockTextQuery, MockTextProduct> pipeline;
    private MockTextQuery ping;
    private MockTextQuery king;

    @Before
    public void setUp() throws Exception {
        this.pipeline = new ManagedPipeline<>(1, new MockTextProvider());
        this.ping = new MockTextQuery("ping");
        this.king = new MockTextQuery("king");
    }

    @Test
    public void testThatProductWillBeFetchedWell() throws Exception {
        MockTextProduct pong = pipeline
            .run(ping)
            .product()
            .get();
        assertEquals(new MockTextProduct("ping_pong", Optional.of(ping)), pong);
    }

    @Test
    public void testThatQueryOverridesMapping() throws Exception {
        MockTextProduct pong = pipeline
            .run(ping)
            .run(i -> new MockTextQuery(i.getResponse() + "_map"))
            .run(king)
            .product()
            .get();
        assertEquals(new MockTextProduct("king_pong", Optional.of(king)), pong);
    }

    @Test
    public void testThatManagedInstanceWillBeReturnedAfterRunQuery() throws Exception {
        Pipeline<MockTextQuery, MockTextProduct> actual = pipeline
            .run(ping);
        assertEquals(pipeline.getClass(), actual.getClass());
    }

    @Test
    public void testThatManagedInstanceWillBeReturnedAfterRunCollection() throws Exception {
        Pipeline<MockTextQuery, MockTextProduct> actual = pipeline
            .run(Collections.singletonList(ping));
        assertEquals(pipeline.getClass(), actual.getClass());
    }

    @Test
    public void testThatManagedInstanceWillBeReturnedAfterRunFunction() throws Exception {
        Pipeline<MockTextQuery, MockTextProduct> actual = pipeline
            .run(f -> ping);
        assertEquals(pipeline.getClass(), actual.getClass());
    }

    @Test
    public void testThatManagedInstanceWillBeReturnedAfterInvokeOn() throws Exception {
        Pipeline<MockTextQuery, MockTextProduct> actual = pipeline
            .on((q, p) -> System.out.println("called"));
        assertEquals(pipeline.getClass(), actual.getClass());
    }

    @Test(expected = IllegalStateException.class)
    public void testThatCanNotUseUserSpecifiedService() throws Exception {
        //noinspection unchecked
        pipeline.using(mock(QueryExecutorService.class));
        assertTrue("exception must be occurred", false);
    }
}
