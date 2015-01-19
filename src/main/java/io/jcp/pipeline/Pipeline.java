package io.jcp.pipeline;

import io.jcp.pipeline.callback.QueryCompleteCallback;
import io.jcp.service.QueryManagerService;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

public interface Pipeline<T, H> {

    Pipeline<T, H> run(T query);

    Pipeline<T, H> run(Collection<T> query);

    Pipeline<T, H>
    run(Function<H, T> mapper);

    Pipeline<T, H> using(QueryManagerService<T, H> service);

    Pipeline<T, H> on(QueryCompleteCallback<T, H> listener);

    Stream<H> stream();

    Optional<H> product();

    List<H> products();

    Optional<Pipeline<T, H>> getParent();

    Optional<Collection<T>> getQueries();

    Optional<QueryManagerService<T, H>> getService();

    Optional<QueryCompleteCallback<T, H>> getCompleteCallback();

    Optional<Function<H, T>> getProductMapper();
}
