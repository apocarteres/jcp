package io.jcp.pipeline.factory;

import io.jcp.pipeline.Pipeline;
import io.jcp.pipeline.callback.QueryCompleteCallback;
import io.jcp.service.QueryManagerService;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;

public interface PipelineFactory<T, H> {
    Pipeline<T, H> create(
        Optional<Pipeline<T, H>> parent,
        Optional<Collection<T>> queries,
        Optional<QueryManagerService<T, H>> service,
        Optional<QueryCompleteCallback<T, H>> queryCompleteCallback,
        Optional<Function<H, T>> mapper
    );

}
