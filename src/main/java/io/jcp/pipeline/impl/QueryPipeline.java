package io.jcp.pipeline.impl;

import io.jcp.pipeline.Pipeline;
import io.jcp.pipeline.callback.QueryCompleteCallback;
import io.jcp.service.QueryExecutorService;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.function.Function;
import java.util.stream.Stream;

public final class QueryPipeline<T, H> implements Pipeline<T, H> {
    private final Optional<Pipeline<T, H>> parent;
    private final Optional<Collection<T>> queries;
    private final Optional<QueryCompleteCallback<T, H>> queryCompleteCallback;
    private final Optional<QueryExecutorService<T, H>> service;
    private final Optional<Function<H, T>> mapper;

    public QueryPipeline(
        Optional<Pipeline<T, H>> parent,
        Optional<Collection<T>> queries,
        Optional<QueryExecutorService<T, H>> service,
        Optional<QueryCompleteCallback<T, H>> queryCompleteCallback,
        Optional<Function<H, T>> mapper
    ) {
        this.parent = parent;
        this.queries = queries;
        this.service = service;
        this.queryCompleteCallback = queryCompleteCallback;
        this.mapper = mapper;
    }

    public QueryPipeline() {
        this(
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    }

    @Override
    public Pipeline<T, H> run(T query) {
        return new QueryPipeline<>(
            Optional.of(this),
            Optional.of(new ArrayList<>(
                Collections.singletonList(query))
            ),
            Optional.empty(),
            Optional.empty(),
            Optional.empty()
        );
    }

    @Override
    public Pipeline<T, H> run(Collection<T> query) {
        return new QueryPipeline<>(
            Optional.of(this),
            Optional.of(query),
            Optional.empty(),
            Optional.empty(),
            Optional.empty()
        );
    }

    @Override
    public <R, K> Pipeline<R, K> run(
        Function<H, R> function, Pipeline<R, K> underlying
    ) {
        return new QueryPipeline<R, K>(
            Optional.of(underlying),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of((Function) function)
        );
    }

    @Override
    public Pipeline<T, H>
    run(Function<H, T> f) {
        return new QueryPipeline<>(
            Optional.of(this),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(f)
        );
    }

    @Override
    public <R, K> Pipeline<R, K> using(
        QueryExecutorService<R, K> service
    ) {
        return new QueryPipeline<R, K>(
            Optional.of((Pipeline) this),
            Optional.empty(),
            Optional.of(service),
            Optional.empty(),
            Optional.empty()
        );
    }

    @Override
    public Pipeline<T, H> on(QueryCompleteCallback<T, H> callback) {
        return new QueryPipeline<>(
            Optional.of(this),
            Optional.empty(),
            Optional.empty(),
            Optional.of(callback),
            Optional.empty()
        );
    }

    @Override
    public Stream<H> stream() {

        final Deque<Pipeline<T, H>> pipelines = new LinkedList<>();
        for (Optional<Pipeline<T, H>> pipeline = Optional.of(this);
             pipeline.isPresent();
             pipeline = pipeline.get().getParent()) {
            pipelines.push(pipeline.get());
        }
        return Stream.empty();
//        final Pipeline<T, H> context = new QueryPipeline<>();
//        for (Pipeline<T, H> pipeline : pipelines) {
//            Optional<QueryExecutorService<T, H>> ctxService = pipeline.getService();
//            Optional<QueryCompleteCallback<T, H>> ctxCallback = pipeline.getCompleteCallback();
//            if (ctxService.isPresent()) {
//                context = context.using(ctxService.get());
//            }
//            if (ctxCallback.isPresent()) {
//                context = context.on(ctxCallback.get());
//            }
//            final Optional<Collection<T>> queries = pipeline.getQueries();
//            if (!queries.isPresent()) {
//                continue;
//            }
//            int length = queries.get().size();
//            Semaphore semaphore = new Semaphore(length);
//            acquireLock(semaphore, length);
//            Queue<H> products = new ConcurrentLinkedQueue<>();
//            Optional<ExecutionCallback<T, H>> callback = Optional.of((r, p) -> {
//                if (p.isPresent()) {
//                    products.add(p.get());
//                }
//            });
//            Pipeline<T, H> effectiveContext = context;
//            Function<T, H> download = q -> {
//                Optional<H> exec = effectiveContext.getService().get().exec(q);
//                Optional<QueryCompleteCallback<T, H>> cb = effectiveContext.getCompleteCallback();
//                if (cb.isPresent()) {
//                    cb.get().onComplete(q, exec);
//                }
//                return exec.isPresent() ? exec.get() : null;
//            };
//            Function<T, H> next = download;
//            for (Function<H, T> mapper : mappers) {
//                next = next.andThen(mapper).andThen(download);
//            }
//            Function<T, H> effectiveNext = next;
//            //noinspection CodeBlock2Expr
//            queries.forEach(q -> {
//                service.submit(q, effectiveNext, Optional.of((query, product) -> {
//                    callback.get().call(q, product);
//                    semaphore.release();
//                }));
//            });
//            acquireLock(semaphore, length);
//            semaphore.release(length);
//
//        }
//
//
//        return products;
//
//
//        List<Pipeline<T, H>> toExec = new ArrayList<>();
//        Pipeline<T, H> partial = new QueryPipeline<>();
//        boolean shouldSink = false;
//        for (Pipeline<T, H> pipeline : pipelines1) {
//            if (pipeline.getQueries().isPresent()) {
//                if (shouldSink) {
//                    toExec.add(partial);
//                    partial = new QueryPipeline<>();
//                    shouldSink = false;
//                }
//                partial = partial.run(pipeline.getQueries().get());
//            } else {
//                if (partial.getQueries().isPresent()) {
//                    shouldSink = true;
//                }
//            }
//            if (pipeline.getCompleteCallback().isPresent()) {
//                partial = partial.on(pipeline.getCompleteCallback().get());
//            }
//            if (pipeline.getService().isPresent()) {
//                partial = partial.using(pipeline.getService().get());
//            }
//            if (pipeline.getProductMapper().isPresent()) {
//                partial = partial.run(pipeline.getProductMapper().get());
//            }
//        }
//        toExec.add(partial);
//        Stream<H> result = Stream.empty();
//        for (Pipeline<T, H> pipeline2 : toExec) {
//            Deque<Pipeline<T, H>> pipelines = new LinkedList<>();
//            for (Optional<Pipeline<T, H>> pipeline = Optional.of(pipeline2);
//                 pipeline.isPresent();
//                 pipeline = pipeline.get().getParent()) {
//                pipelines.push(pipeline.get());
//            }
//
//            List<T> queries = pipelines.stream()
//                .filter(p -> p.getQueries().isPresent())
//                .map(p -> p.getQueries().get())
//                .flatMap(Collection::stream)
//                .collect(toList());
//            List<Function<H, T>> mappers = pipelines.stream()
//                .filter(p -> p.getProductMapper().isPresent())
//                .map(p -> p.getProductMapper().get())
//                .collect(toList());
//            List<QueryCompleteCallback<T, H>> callbacks = pipelines.stream()
//                .filter(p -> p.getCompleteCallback().isPresent())
//                .map(p -> p.getCompleteCallback().get())
//                .collect(toList());
//            QueryExecutorService<T, H> service = pipelines.stream()
//                .filter(p -> p.getService().isPresent())
//                .map(Pipeline::getService)
//                .findFirst().orElseThrow(() ->
//                    new IllegalStateException("service must be specified"))
//                .get();
//            if (pipelines.stream()
//                .filter(p -> p.getService().isPresent())
//                .count() > 1) {
//                throw new IllegalStateException("can't use two or more services");
//            }
//            if (queries.isEmpty()) {
//                throw new IllegalStateException("at least one query must be specified");
//            }
//            result = Stream.concat(result, processQueries(queries, callbacks, mappers, service).stream());
//        }
//
//        return result;
    }

    private Queue<H> processQueries(
        List<T> queries,
        List<QueryCompleteCallback<T, H>> callbacks,
        List<Function<H, T>> mappers,
        QueryExecutorService<T, H> service
    ) {
//        int length = queries.size();
//        Semaphore semaphore = new Semaphore(length);
//        acquireLock(semaphore, length);
//        Queue<H> products = new ConcurrentLinkedQueue<>();
//        Optional<ExecutionCallback<T, H>> callback = Optional.of((r, p) -> {
//            if (p.isPresent()) {
//                products.add(p.get());
//            }
//        });
//        Function<T, H> download = q -> {
//            Optional<H> exec = service.exec(q);
//            callbacks.forEach(c -> c.onComplete(q, exec));
//            return exec.isPresent() ? exec.get() : null;
//        };
//        Function<T, H> next = download;
//        for (Function<H, T> mapper : mappers) {
//            next = next.andThen(mapper).andThen(download);
//        }
//        Function<T, H> effectiveNext = next;
//        //noinspection CodeBlock2Expr
//        queries.forEach(q -> {
//            service.submit(q, effectiveNext, Optional.of((query, product) -> {
//                callback.get().call(q, product);
//                semaphore.release();
//            }));
//        });
//        acquireLock(semaphore, length);
//        semaphore.release(length);
//        return products;
        return new LinkedBlockingQueue<>();
    }

    private static void acquireLock(Semaphore s, int length) {
        try {
            s.acquire(length);
        } catch (InterruptedException e) {
            throw new IllegalStateException("exception occurred during pipeline running", e);
        }
    }

    @Override
    public Optional<Pipeline<T, H>> getParent() {
        return this.parent;
    }

    @Override
    public Optional<Collection<T>> getQueries() {
        return this.queries;
    }

    @Override
    public Optional<QueryExecutorService<T, H>> getService() {
        return this.service;
    }

    @Override
    public Optional<QueryCompleteCallback<T, H>> getCompleteCallback() {
        return this.queryCompleteCallback;
    }

    @Override
    public Optional<Function<H, T>> getProductMapper() {
        return this.mapper;
    }

}
