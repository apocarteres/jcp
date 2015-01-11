package io.jcp.pipeline;

import io.jcp.bean.Product;
import io.jcp.bean.Query;
import io.jcp.service.QueryManagerService;

import java.util.Optional;
import java.util.function.Function;

public interface Pipeline<T extends Query, H extends Product<T>> {

    Pipeline<T, H> run(T query);

    <F extends Query, K extends Product<F>> Pipeline<F, K>
    run(Function<? super T, ? extends F> mapper, Class<K> type);

    Pipeline<T, H> with(QueryManagerService<T, H> service);

    H collect();

    Optional<Pipeline<T, H>> getParent();

    Optional<T> getQuery();

    Optional<QueryManagerService<T, H>> getService();

}
