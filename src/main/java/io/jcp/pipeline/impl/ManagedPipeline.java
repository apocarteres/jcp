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

import io.jcp.pipeline.Pipeline;
import io.jcp.pipeline.callback.QueryCompleteCallback;
import io.jcp.provider.Provider;
import io.jcp.service.ConcurrentQueryExecutorService;
import io.jcp.service.QueryExecutorService;
import io.jcp.service.impl.ManagedQueryExecutorService;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;

public final class ManagedPipeline<T, H> implements Pipeline<T, H> {
    private final ThreadPoolExecutor executor;
    private final ConcurrentQueryExecutorService<T, H> service;
    private final Pipeline<T, H> origin;
    private final List<Callable<Optional<H>>> products;

    public ManagedPipeline(int threads, Provider<T, H> provider) {
        this.executor = new ThreadPoolExecutor(
            threads, threads, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>()
        );
        this.service = new ManagedQueryExecutorService<>(
            executor, Collections.emptyList(), provider
        );
        this.origin = new QueryPipeline<T, H>().using(service);
        this.products = Collections.emptyList();
    }

    private ManagedPipeline(
        ThreadPoolExecutor executor,
        ConcurrentQueryExecutorService<T, H> service,
        Pipeline<T, H> origin,
        List<Callable<Optional<H>>> products
    ) {
        this.executor = executor;
        this.service = service;
        this.origin = origin;
        this.products = products;
    }

    @Override
    public <R, K> ManagedPipeline<R, K> using(QueryExecutorService<R, K> service) {
        throw new IllegalStateException(
            "user defined service can't be use with ManagedPipeline. Use QueryPipeline instead."
        );
    }

    @Override
    public ManagedPipeline<T, H> run(T query) {
        return run(Collections.singletonList(query));
    }

    @Override
    public ManagedPipeline<T, H> run(Function<H, T> f) {
        return new ManagedPipeline<>(
            executor,
            service,
            origin,
            products().stream().map(o -> (Callable<Optional<H>>) () ->
                this.origin.run(f).product())
                .collect(toList())
        );

    }

    public ManagedPipeline<T, H> run(Collection<T> query) {
        return wrap(origin, query.stream()
            .map(q -> (Callable<Optional<H>>) () -> this.origin.run(q).product())
            .collect(toList()));
    }

    @Override
    public <R, K> ManagedPipeline<R, K> run(
        Function<H, R> function, Pipeline<R, K> underlying
    ) {
        return new ManagedPipeline<>(
            executor,
            null,
            underlying,
            products().stream().map(o -> (Callable<Optional<K>>) () ->
                underlying.run(function.apply(o.get())).product())
                .collect(toList())
        );
    }

    @Override
    public ManagedPipeline<T, H> callback(QueryCompleteCallback<T, H> callback) {
        return wrap(
            this.origin.callback(callback)
        );
    }

    @Override
    public Optional<H> product() {
        return this.products().isEmpty() ?
            Optional.empty() : this.products().iterator().next();
    }

    public List<Optional<H>> products() {
        if (this.products.isEmpty()) {
            throw new IllegalStateException("at least one query must be specified");
        }
        List<Optional<H>> result = new ArrayList<>();
        try {
            executor.invokeAll(
                products
            ).forEach(f -> {
                try {
                    result.add(f.get());
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            });
        } catch (InterruptedException e) {
            throw new IllegalStateException("can't get products", e);
        }
        return result;
    }

    private ManagedPipeline<T, H> shutdown() {
        this.service.shutdown();
        this.executor.shutdown();
        return this;
    }

    private ManagedPipeline<T, H> wrap(
        Pipeline<T, H> origin, List<Callable<Optional<H>>> ps
    ) {
        return new ManagedPipeline<>(
            this.executor, this.service, origin, ps
        );
    }

    private ManagedPipeline<T, H> wrap(Pipeline<T, H> origin) {
        return wrap(origin, Collections.emptyList());
    }

}
