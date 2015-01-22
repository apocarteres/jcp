package io.jcp.pipeline;

import io.jcp.pipeline.callback.QueryCompleteCallback;
import io.jcp.service.QueryManagerService;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public interface Pipeline<T, H> {

    Pipeline<T, H> run(T query);

    Pipeline<T, H> run(Collection<T> query);

    Pipeline<T, H>
    run(Function<H, T> f);

    Pipeline<T, H> using(QueryManagerService<T, H> service);

    Pipeline<T, H> on(QueryCompleteCallback<T, H> callback);

    default Stream<H> stream() {
        return Stream.empty();
    }

    default Optional<H> product() {
        return stream().findFirst();
    }

    default List<H> products() {
        return stream().collect(toList());
    }

    default Optional<Pipeline<T, H>> getParent() {
        return Optional.empty();
    }

    default Optional<Collection<T>> getQueries() {
        return Optional.empty();
    }

    default Optional<QueryManagerService<T, H>> getService() {
        return Optional.empty();
    }

    default Optional<QueryCompleteCallback<T, H>> getCompleteCallback() {
        return Optional.empty();
    }

    default Optional<Function<H, T>> getProductMapper() {
        return Optional.empty();
    }
}
