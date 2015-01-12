package io.jcp.pipeline;

import io.jcp.service.QueryManagerService;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

public interface Pipeline<T, H> extends AutoCloseable {

    Pipeline<T, H> run(T query);

    Pipeline<T, H> run(Collection<T> query);

    <F, K> Pipeline<F, K>
    run(Function<? super T, ? extends F> mapper, Class<K> type);

    Pipeline<T, H> using(QueryManagerService<T, H> service);

    Stream<H> stream();

    Optional<H> product();

    List<H> products();

    Optional<Pipeline<T, H>> getParent();

    Optional<Collection<T>> getQueries();

    Optional<QueryManagerService<T, H>> getService();

    default void close() throws Exception {
    }
}
