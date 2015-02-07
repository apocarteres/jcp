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

package io.jcp;

import io.jcp.bean.ExecutionCallback;
import io.jcp.bean.MockTextProduct;
import io.jcp.bean.MockTextQuery;
import io.jcp.listener.MockQueryLifecycleListener;
import io.jcp.provider.MockTextProvider;
import io.jcp.service.impl.ManagedQueryExecutorService;
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

public class ManagedQueryExecutorServiceTest {

    @SuppressWarnings("FieldCanBeLocal")
    private ThreadPoolExecutor threadPool;
    private MockQueryLifecycleListener lifecycleListener;
    private ManagedQueryExecutorService<MockTextQuery, MockTextProduct> executorService;

    @Before
    public void setUp() throws Exception {
        this.threadPool = new ThreadPoolExecutor(
            2, 2, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>()
        );
        this.lifecycleListener = new MockQueryLifecycleListener();
        this.executorService = new ManagedQueryExecutorService<>(
            this.threadPool, Collections.singleton(this.lifecycleListener),
            new MockTextProvider()
        );
    }

    @Test
    public void testThatNoSubmittedRequestsByDefault() throws Exception {
        assertEquals(0, executorService.countSubmitted());
    }

    @Test
    public void testThatNoInProgressRequestsByDefault() throws Exception {
        assertEquals(0, executorService.countInProgress());
    }

    @Test
    public void testThatCountsExactlyOneSubmittedRequest() throws Exception {
        executorService.submit(new MockTextQuery(), Optional.empty());
        executorService.submit(new MockTextQuery(), Optional.empty());
        executorService.submit(new MockTextQuery(), Optional.empty());
        sleep(MockTextProvider.FETCH_DELAY / 2);
        assertEquals(1, executorService.countSubmitted());
    }

    @Test(timeout = 60000)
    public void testLifecycleListenersWillGetOnConsumeEvent() throws Exception {
        MockTextQuery query = new MockTextQuery();
        executorService.exec(query);
        assertEquals(1, this.lifecycleListener.requests(
            MockQueryLifecycleListener.Event.EXEC).count()
        );
    }

    @Test(timeout = 60000)
    public void testThatRequestWillBeExecuted() throws Exception {
        MockTextQuery query = new MockTextQuery();
        Optional<MockTextProduct> exec = executorService.exec(query);
        assertEquals(new MockTextProduct("_pong", Optional.of(query)), exec.get());
    }

    @Test(timeout = 60000)
    public void testThatNoTasksAfterEverythingWereExecuted() throws Exception {
        MockTextQuery query = new MockTextQuery();
        executorService.exec(query);
        executorService.shutdown();
        assertEquals(0, this.executorService.countInProgress());
    }

    @Test(timeout = 60000)
    public void testThatInProgressTaskCountWell() throws Exception {
        MockTextQuery query1 = new MockTextQuery();
        MockTextQuery query2 = new MockTextQuery();
        executorService.submit(query1, Optional.empty());
        executorService.submit(query2, Optional.empty());
        sleep(MockTextProvider.FETCH_DELAY / 2);
        assertEquals(2, this.executorService.countInProgress());
    }

    @Test(timeout = 60000)
    public void testThatShutdownMethodWaitsAllCurrentTask() throws Exception {
        MockTextQuery request1 = new MockTextQuery();
        MockTextQuery request2 = new MockTextQuery();
        executorService.submit(request1, Optional.empty());
        executorService.submit(request2, Optional.empty());
        sleep(MockTextProvider.FETCH_DELAY / 2);
        executorService.shutdown();
        assertEquals(0, this.executorService.countInProgress());
    }

    @Test(timeout = 60000, expected = IllegalStateException.class)
    public void testThatCanNotSubmitWhenInShutdownState() throws Exception {
        new Thread(() -> {
            try {
                sleep(1000);
                executorService.shutdown();
            } catch (InterruptedException ignored) {
            }
        }).start();
        for (int i = 0; i < 10; i++) {
            executorService.submit(new MockTextQuery(), Optional.empty());
            sleep(MockTextProvider.FETCH_DELAY / 2);
        }
        assertTrue("service is not allowed to accept submissions after shutdown", false);
    }

    @Test(timeout = 60000)
    public void testThatShutdownDoesNotBlockIfNoTasks() throws Exception {
        executorService.shutdown();
        assertTrue(true);
    }

    @Test(timeout = 60000, expected = IllegalStateException.class)
    public void testThatCanNotShutdownMoreThanOneTime() throws Exception {
        executorService.shutdown();
        executorService.shutdown();
        assertTrue(false);
    }

    @Test(timeout = 60000)
    public void testThatProductWillBeReturnedViaCallback() throws Exception {
        MockTextQuery task = new MockTextQuery("ping");
        Set<String> result = new HashSet<>();
        ExecutionCallback<MockTextQuery, MockTextProduct> callback = (t, p) ->
            result.add(p.get().getResponse());
        executorService.submit(task, Optional.of(callback));
        executorService.shutdown();
        assertEquals("ping_pong", result.iterator().next());
    }

    @Test(timeout = 60000)
    public void testThatCallbackContainsCorrectQuery() throws Exception {
        MockTextQuery task = new MockTextQuery();
        Set<MockTextQuery> result = new HashSet<>();
        ExecutionCallback<MockTextQuery, MockTextProduct> callback = (t, p) ->
            result.add(t);
        executorService.submit(task, Optional.of(callback));
        executorService.shutdown();
        assertTrue(task == result.iterator().next());
    }

    @Test(timeout = 60000)
    public void testThatExecReturnsProductWell() throws Exception {
        MockTextQuery task = new MockTextQuery("ping");
        MockTextProduct product = executorService.exec(task).get();
        assertEquals("ping_pong", product.getResponse());
    }

    @Test(timeout = 60000)
    public void testSubmitCounterDecrementsWhenTaskExecuted() throws Exception {
        executorService.exec(new MockTextQuery("ping"));
        assertEquals(0, executorService.countSubmitted());
    }

}
