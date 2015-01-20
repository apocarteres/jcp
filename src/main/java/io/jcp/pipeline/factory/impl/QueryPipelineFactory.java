package io.jcp.pipeline.factory.impl;

import io.jcp.pipeline.Pipeline;
import io.jcp.pipeline.callback.QueryCompleteCallback;
import io.jcp.pipeline.factory.PipelineFactory;
import io.jcp.pipeline.impl.QueryPipeline;
import io.jcp.service.QueryManagerService;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;

public class QueryPipelineFactory<T, H> implements PipelineFactory<T, H> {
    @Override
    public Pipeline<T, H> create(
        Optional<Pipeline<T, H>> parent,
        Optional<Collection<T>> queries,
        Optional<QueryManagerService<T, H>> service,
        Optional<QueryCompleteCallback<T, H>> queryCompleteCallback,
        Optional<Function<H, T>> mapper) {
        return new QueryPipeline<>(
            parent, queries, service, queryCompleteCallback, mapper, this
        );
    }
}
