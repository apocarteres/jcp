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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;


public class QueryPipelineTest {

    private QueryPipeline<MockTextQuery, MockTextProduct> pipeline;
    private ConcurrentQueryManagerServiceImpl<MockTextQuery, MockTextProduct> managerService;

    @Before
    public void setUp() throws Exception {
        this.pipeline = new QueryPipeline<>();
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(1, 1, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>());
        MockTaskLifecycleListener lifecycleListener = new MockTaskLifecycleListener();
        MockQueryExecutorService executorService = new MockQueryExecutorService();
        managerService = new ConcurrentQueryManagerServiceImpl<>(
            threadPool, Collections.singleton(lifecycleListener),
            executorService
        );
    }

    @Test
    public void testThatOneProductWillBeCollectedWell() throws Exception {
        MockTextProduct collect = this.pipeline
            .run(new MockTextQuery())
            .with(managerService)
            .collect();
        assertEquals("pong", collect.getResponse());
    }

    @Test
    @Ignore
    public void testMapTextProductToIntQueryAndCollectIntProduct() throws Exception {
        MockIntProduct collect = this.pipeline
            .run(new MockTextQuery())
            .with(managerService)
            .run(p -> new MockIntQuery(p.hashCode()), MockIntProduct.class)
            .collect();
        assertEquals("pong".length(), collect.getResponse());
    }

    @Test(expected = IllegalStateException.class)
    public void testThatCanNotRunWithoutQuery() throws Exception {
        MockTextProduct collect = this.pipeline
            .with(managerService)
            .collect();
        assertEquals("pong", collect.getResponse());
    }

    @Test(expected = IllegalStateException.class)
    public void testThatCanNotRunWithoutService() throws Exception {
        MockTextProduct collect = this.pipeline
            .run(new MockTextQuery())
            .collect();
        assertEquals("pong", collect.getResponse());
    }

    @Test(expected = IllegalStateException.class)
    public void testThatQueryMustBeSpecifiedJustOnce() throws Exception {
        MockTextProduct collect = this.pipeline
            .run(new MockTextQuery())
            .with(managerService)
            .run(new MockTextQuery())
            .collect();
        assertEquals("pong", collect.getResponse());
    }

    @Test(expected = IllegalStateException.class)
    public void testThatServiceMustBeSpecifiedJustOnce() throws Exception {
        MockTextProduct collect = this.pipeline
            .with(managerService)
            .run(new MockTextQuery())
            .with(managerService)
            .collect();
        assertEquals("pong", collect.getResponse());
    }

}
